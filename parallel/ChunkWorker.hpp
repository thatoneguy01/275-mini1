#pragma once
#include <vector>
#include <string>
#include <cstddef>

#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"

namespace parallel {

class ChunkWorker {
public:
    ChunkWorker(query::Query& query, std::size_t chunk_size);

    void process(std::size_t tid, const std::string& chunk,
                 std::vector<std::vector<dob::DobJobApplication>>& thread_results);

private:
    query::Query& query_;
    std::size_t chunk_size_;
};

} // namespace parallel

