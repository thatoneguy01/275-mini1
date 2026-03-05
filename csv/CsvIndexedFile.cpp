#include "CsvIndexedFile.hpp"

#ifdef _WIN32
#include <windows.h>
#include <sys/stat.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <fstream>
#include <stdexcept>
#include <vector>
#include <mutex>
#include <algorithm>
#include <sstream>

#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"
#include "../parallel/ThreadPool.hpp"
#include "../parallel/ChunkWorker.hpp"

#undef ENABLE_LOGGING
#include "../logging.hpp"

// ---------- helpers ----------

uint64_t CsvIndexedFile::file_size(const std::string& path)
{
#ifdef _WIN32
    struct _stat64 st{};
    if (_stat64(path.c_str(), &st) != 0)
        throw std::runtime_error("stat failed");
    return static_cast<uint64_t>(st.st_size);
#else
    struct stat st{};
    if (stat(path.c_str(), &st) != 0)
        throw std::runtime_error("stat failed");
    return static_cast<uint64_t>(st.st_size);
#endif
}

// ---------- ctor / dtor ----------

CsvIndexedFile::CsvIndexedFile(const std::string& csvPath, std::size_t chunk_size, std::size_t thread_pool_size)
    : csv_path_(csvPath),
      idx_path_(csvPath + ".idx"),
      chunk_size_(chunk_size),
      thread_pool_size_(thread_pool_size),
      file_(csvPath, std::ios::binary)
{
    if (!file_)
        throw std::runtime_error("Failed to open CSV");

    ensure_index();
}

CsvIndexedFile::~CsvIndexedFile()
{
#ifdef _WIN32
    if (mmap_mem_) {
        UnmapViewOfFile(mmap_mem_);
        mmap_mem_ = nullptr;
    }
    if (mmap_handle_) {
        CloseHandle(static_cast<HANDLE>(mmap_handle_));
        mmap_handle_ = nullptr;
    }
#else
    if (mmap_mem_) {
        munmap(mmap_mem_, mmap_size_);
        mmap_mem_ = nullptr;
    }
    if (mmap_fd_ >= 0) {
        close(mmap_fd_);
        mmap_fd_ = -1;
    }
#endif
}

// ---------- public ----------

std::size_t CsvIndexedFile::row_count() const
{
    return header_->row_count;
}

void CsvIndexedFile::seek_row(std::size_t row_index)
{
    if (row_index >= header_->row_count)
        throw std::out_of_range("row out of range");

    file_.clear();
    file_.seekg(offsets_[row_index]);
}

std::string CsvIndexedFile::read_row(std::size_t row_index)
{
    if (row_index >= header_->row_count)
        throw std::out_of_range("row out of range");

    uint64_t row_start = offsets_[row_index];
    uint64_t row_end;

    if (row_index + 1 < header_->row_count) {
        row_end = offsets_[row_index + 1];
    } else {
        // Last row: read to end of file
        row_end = file_size(csv_path_);
    }

    // Calculate row size (excluding the newline)
    uint64_t row_size = row_end - row_start;
    if (row_size > 0 && row_size <= 1) {
        // Skip the trailing newline if present
        row_size--;
    }

    file_.clear();
    file_.seekg(row_start);

    std::string row(row_size, '\0');
    file_.read(&row[0], row_size);

    // Remove trailing newline if it exists
    if (!row.empty() && row.back() == '\n') {
        row.pop_back();
    }

    return row;
}

std::string CsvIndexedFile::read_rows(int n)
{
    if (n <= 0)
        return "";

    // Get current file position
    uint64_t current_pos = file_.tellg();

    // Find which row index corresponds to current position
    std::size_t start_row = 0;
    bool found = false;
    for (std::size_t i = 0; i < header_->row_count; ++i) {
        if (offsets_[i] == current_pos) {
            start_row = i;
            found = true;
            break;
        }
    }

    if (!found) {
        throw std::runtime_error("Current file position does not align with any indexed row");
    }

    // Cap n to the actual number of rows available from current position
    std::size_t rows_to_read = std::min(static_cast<std::size_t>(n),
                                         header_->row_count - start_row);

    if (rows_to_read == 0)
        return "";

    // Determine the end position
    uint64_t end_pos;
    if (start_row + rows_to_read >= header_->row_count) {
        // Reading all remaining rows to end of file
        end_pos = file_size(csv_path_);
    } else {
        // Reading n rows, so end at the start of the next row
        end_pos = offsets_[start_row + rows_to_read];
    }

    // Calculate chunk size and read
    std::size_t chunk_size = end_pos - current_pos;

    std::string chunk(chunk_size, '\0');
    file_.read(&chunk[0], chunk_size);

    return chunk;
}

// ---------- index lifecycle ----------

void CsvIndexedFile::ensure_index()
{
    if (try_load_index())
        return;

    build_index();
    map_index();
}

bool CsvIndexedFile::try_load_index()
{
#ifdef _WIN32
    struct _stat64 st{};
    if (_stat64(idx_path_.c_str(), &st) != 0)
        return false;
#else
    struct stat st{};
    if (stat(idx_path_.c_str(), &st) != 0)
        return false;
#endif

    CsvIndexHeader h{};
    std::ifstream in(idx_path_, std::ios::binary);
    if (!in)
        return false;

    in.read(reinterpret_cast<char*>(&h), sizeof(h));
    if (!in)
        return false;

    if (h.magic != 0x4353564944583031ULL) return false;
    if (h.version != 1) return false;
    if (h.file_size != file_size(csv_path_)) return false;

    map_index();
    return true;
}

void CsvIndexedFile::build_index()
{
    uint64_t total_size = file_size(csv_path_);
    const std::size_t chunk_read_size = 65536; // 64 KB chunks

    file_.clear();
    file_.seekg(0);

    // Shared state for collecting offsets
    std::mutex offsets_mutex;
    std::vector<uint64_t> offsets;
    offsets.push_back(0);

    // Create ThreadPool
    parallel::ThreadPool pool(thread_pool_size_, chunk_size_);

    // Main thread reads chunks and enqueues processing tasks
    uint64_t global_offset = 0;
    std::vector<char> buffer(chunk_read_size);

    while (file_.read(buffer.data(), chunk_read_size) || file_.gcount() > 0)
    {
        std::size_t bytes_read = file_.gcount();

        if (bytes_read == 0) break;

        // Store chunk data and offset
        auto chunk_data = std::make_shared<std::string>(buffer.data(), bytes_read);
        uint64_t chunk_offset = global_offset;
        global_offset += bytes_read;

        // Enqueue task to process this chunk
        pool.enqueue(chunk_data, [chunk_offset, &offsets, &offsets_mutex](
            const std::shared_ptr<std::string>& chunk,
            const std::shared_ptr<parallel::ChunkWorker>&)
        {
            const char* data = chunk->data();
            const char* ptr  = data;
            const char* end  = data + chunk->size();

            std::vector<uint64_t> local_offsets;
            local_offsets.reserve(chunk->size() / 32); // heuristic

            bool in_quotes = false;

            while (ptr < end)
            {
                const char* next_quote = (const char*)memchr(ptr, '"', end - ptr);
                const char* next_nl    = (const char*)memchr(ptr, '\n', end - ptr);

                const char* next = nullptr;

                if (!next_quote) next = next_nl;
                else if (!next_nl) next = next_quote;
                else next = (next_quote < next_nl) ? next_quote : next_nl;

                if (!next)
                    break;

                if (*next == '"')
                {
                    if (in_quotes)
                    {
                        if (next + 1 < end && next[1] == '"')
                        {
                            ptr = next + 2; // skip escaped quote
                            continue;
                        }
                        else
                        {
                            in_quotes = false;
                        }
                    }
                    else
                    {
                        in_quotes = true;
                    }

                    ptr = next + 1;
                }
                else
                {
                    if (!in_quotes)
                        local_offsets.push_back(chunk_offset + (next - data) + 1);

                    ptr = next + 1;
                }
            }

            {
                std::lock_guard<std::mutex> lock(offsets_mutex);
                offsets.insert(offsets.end(), local_offsets.begin(), local_offsets.end());
            }
        });
    }

    LOG("CsvIndexedFile::build_index: Waiting for all tasks to complete...");
    // Wait for all tasks to complete
    pool.wait_all();
    LOG("CsvIndexedFile::build_index: All tasks completed.");
    // Sort offsets to ensure correct order
    std::sort(offsets.begin(), offsets.end());

    // Remove duplicates
    offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());

    if (!offsets.empty() && offsets.back() == total_size)
        offsets.pop_back();

    save_index(offsets, total_size);
}

void CsvIndexedFile::save_index(const std::vector<uint64_t>& offsets,
                                uint64_t fileSize)
{
    CsvIndexHeader h;
    h.file_size = fileSize;
    h.row_count = offsets.size();

    std::ofstream out(idx_path_, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to write index");

    out.write(reinterpret_cast<char*>(&h), sizeof(h));
    out.write(reinterpret_cast<const char*>(offsets.data()),
              offsets.size() * sizeof(uint64_t));
}

void CsvIndexedFile::map_index()
{
#ifdef _WIN32
    HANDLE file = CreateFileA(
        idx_path_.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);

    if (file == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("open idx failed");
    }

    LARGE_INTEGER size{};
    if (!GetFileSizeEx(file, &size)) {
        CloseHandle(file);
        throw std::runtime_error("GetFileSizeEx failed");
    }

    mmap_size_ = static_cast<size_t>(size.QuadPart);

    HANDLE mapping = CreateFileMappingA(file, nullptr, PAGE_READONLY, 0, 0, nullptr);
    CloseHandle(file);

    if (!mapping) {
        throw std::runtime_error("CreateFileMapping failed");
    }

    void* view = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);
    if (!view) {
        CloseHandle(mapping);
        throw std::runtime_error("MapViewOfFile failed");
    }

    mmap_handle_ = mapping;
    mmap_mem_ = view;
#else
    mmap_fd_ = open(idx_path_.c_str(), O_RDONLY);
    if (mmap_fd_ < 0)
        throw std::runtime_error("open idx failed");

    struct stat st{};
    fstat(mmap_fd_, &st);

    mmap_size_ = st.st_size;

    mmap_mem_ = mmap(nullptr, mmap_size_, PROT_READ, MAP_SHARED, mmap_fd_, 0);

    if (mmap_mem_ == MAP_FAILED)
        throw std::runtime_error("mmap failed");
#endif

    header_ = reinterpret_cast<CsvIndexHeader*>(mmap_mem_);
    offsets_ = reinterpret_cast<uint64_t*>(
        reinterpret_cast<char*>(mmap_mem_) + sizeof(CsvIndexHeader));

    // Cache offsets in memory
    if (header_->row_count > 0 && offsets_) {
        cached_offsets_.assign(offsets_, offsets_ + header_->row_count);
    }

    LOG("CsvIndexedFile::map_index: Cached %llu row offsets in memory", header_->row_count);
}

const uint64_t* CsvIndexedFile::get_offsets() const
{
    if (cached_offsets_.empty()) {
        throw std::runtime_error("Index not available");
    }
    return cached_offsets_.data();
}

uint64_t CsvIndexedFile::get_row_offset(std::size_t row_index) const
{
    if (row_index >= cached_offsets_.size()) {
        throw std::out_of_range("row out of range");
    }
    return cached_offsets_[row_index];
}

std::vector<dob::DobJobApplication> CsvIndexedFile::query(query::Query &q) {
    parallel::ParallelQueryProcessor processor(*this, thread_pool_size_, chunk_size_);
    std::vector<dob::DobJobApplication> out_results;
    processor.execute(q, out_results);
    return out_results;
}

