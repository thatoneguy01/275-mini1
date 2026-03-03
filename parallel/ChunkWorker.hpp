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

    void process(const std::string& chunk);

    std::vector<dob::DobJobApplication>& get_results();
    void clear_results();

private:
    query::Query& query_;
    std::size_t chunk_size_;
    std::vector<dob::DobJobApplication> results_;
};

} // namespace parallel

