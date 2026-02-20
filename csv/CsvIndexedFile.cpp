#include "CsvIndexedFile.hpp"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

// ---------- helpers ----------

uint64_t CsvIndexedFile::file_size(const std::string& path)
{
    struct stat st{};
    if (stat(path.c_str(), &st) != 0)
        throw std::runtime_error("stat failed");
    return st.st_size;
}

// ---------- ctor / dtor ----------

CsvIndexedFile::CsvIndexedFile(const std::string& csvPath)
    : csv_path_(csvPath),
      idx_path_(csvPath + ".idx"),
      file_(csvPath, std::ios::binary)
{
    if (!file_)
        throw std::runtime_error("Failed to open CSV");

    ensure_index();
}

CsvIndexedFile::~CsvIndexedFile()
{
    if (mmap_mem_)
        munmap(mmap_mem_, mmap_size_);
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
    seek_row(row_index);

    std::string row;
    bool in_quotes = false;
    char c;

    while (file_.get(c))
    {
        if (c == '"')
        {
            row += c;

            if (in_quotes)
            {
                if (file_.peek() == '"')
                    row += file_.get();
                else
                    in_quotes = false;
            }
            else
                in_quotes = true;
        }
        else if (c == '\n' && !in_quotes)
        {
            break;
        }
        else
            row += c;
    }

    return row;
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
    struct stat st{};
    if (stat(idx_path_.c_str(), &st) != 0)
        return false;

    // quick header validation
    int fd = open(idx_path_.c_str(), O_RDONLY);
    if (fd < 0) return false;

    CsvIndexHeader h{};
    if (read(fd, &h, sizeof(h)) != sizeof(h))
    {
        close(fd);
        return false;
    }
    close(fd);

    if (h.magic != 0x4353564944583031ULL) return false;
    if (h.version != 1) return false;
    if (h.file_size != file_size(csv_path_)) return false;

    map_index();
    return true;
}

void CsvIndexedFile::build_index()
{
    std::vector<uint64_t> offsets;

    file_.clear();
    file_.seekg(0);

    bool in_quotes = false;
    char c;

    offsets.push_back(0);

    while (file_.get(c))
    {
        if (c == '"')
        {
            if (in_quotes)
            {
                if (file_.peek() == '"')
                    file_.get();
                else
                    in_quotes = false;
            }
            else
                in_quotes = true;
        }
        else if (c == '\n' && !in_quotes)
        {
            offsets.push_back((uint64_t)file_.tellg());
        }
    }

    uint64_t size = file_size(csv_path_);
    if (!offsets.empty() && offsets.back() == size)
        offsets.pop_back();

    save_index(offsets, size);
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
    mmap_fd_ = open(idx_path_.c_str(), O_RDONLY);
    if (mmap_fd_ < 0)
        throw std::runtime_error("open idx failed");

    struct stat st{};
    fstat(mmap_fd_, &st);

    mmap_size_ = st.st_size;

    mmap_mem_ = mmap(nullptr, mmap_size_, PROT_READ, MAP_SHARED, mmap_fd_, 0);
    close(mmap_fd_);

    if (mmap_mem_ == MAP_FAILED)
        throw std::runtime_error("mmap failed");

    header_ = reinterpret_cast<CsvIndexHeader*>(mmap_mem_);
    offsets_ = reinterpret_cast<uint64_t*>(
        reinterpret_cast<char*>(mmap_mem_) + sizeof(CsvIndexHeader));
}
