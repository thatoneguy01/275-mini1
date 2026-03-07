#pragma once
#include <vector>
#include <string>

#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"

class CsvIndexedFile;

namespace parallel {

class ParallelQueryProcessor {
public:
    ParallelQueryProcessor(const CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size);

    void execute(const query::Query& q, std::vector<dob::DobJobApplication>& out_results);

private:
    const CsvIndexedFile& csv_file_;
    std::size_t thread_pool_size_;
    std::size_t chunk_size_;
};

} // namespace parallel

