#include "ChunkWorker.hpp"
#include <sstream>

namespace parallel {

ChunkWorker::ChunkWorker(query::Query& query, std::size_t chunk_size)
    : query_(query), chunk_size_(chunk_size) {}

void ChunkWorker::process(const std::string& chunk) {
    // Reserve space for expected number of results
    std::size_t initial_size = results_.size();
    results_.reserve(initial_size + chunk_size_);

    // Parse lines from the chunk string
    std::istringstream stream(chunk);
    std::string line;

    while (std::getline(stream, line)) {
        if (line.empty())
            continue;

        if (query_.eval(line)) {
            try {
                results_.push_back(dob::parse_row(line));
            } catch (const std::exception& e) {
                // Handle parse error (e.g., log it)
            }
        }
    }
}

std::vector<dob::DobJobApplication>& ChunkWorker::get_results() {
    return results_;
}

void ChunkWorker::clear_results() {
    results_.clear();
}

} // namespace parallel
