#pragma once
#include <vector>
#include <string>

#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"

class CsvIndexedFile;

namespace parallel {

class ParallelQueryProcessor {
public:
    ParallelQueryProcessor(CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size);

    std::vector<dob::DobJobApplication>& execute(query::Query& q);

private:
    CsvIndexedFile& csv_file_;
    std::size_t thread_pool_size_;
    std::size_t chunk_size_;
};

} // namespace parallel

