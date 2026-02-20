#pragma once
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>

struct CsvIndexHeader {
    uint64_t magic = 0x4353564944583031ULL; // CSVIDX01
    uint64_t version = 1;
    uint64_t file_size = 0;
    uint64_t row_count = 0;
};

class CsvIndexedFile {
public:
    explicit CsvIndexedFile(const std::string& csvPath);
    ~CsvIndexedFile();

    std::size_t row_count() const;

    void seek_row(std::size_t row_index);
    std::string read_row(std::size_t row_index);

private:
    std::string csv_path_;
    std::string idx_path_;

    std::ifstream file_;

    // mmap index
    int mmap_fd_ = -1;
    void* mmap_mem_ = nullptr;
    size_t mmap_size_ = 0;

    CsvIndexHeader* header_ = nullptr;
    uint64_t* offsets_ = nullptr;

private:
    void ensure_index();
    bool try_load_index();
    void build_index();
    void save_index(const std::vector<uint64_t>& offsets, uint64_t fileSize);
    void map_index();

    static uint64_t file_size(const std::string& path);
};
