#include "ChunkWorker.hpp"
#include <sstream>

#define ENABLE_LOGGING 1
#include "../logging.hpp"

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
    int total_lines = 0;
    int lines_matched = 0;
    while (std::getline(stream, line)) {
        ++total_lines;
        if (line.empty())
            continue;

        if (query_.eval(line)) {
            try {
                results_.push_back(dob::parse_row(line));
                ++lines_matched;
            } catch (const std::exception& e) {
                // Handle parse error (e.g., log it)
            }
        }
    }
    LOG("ChunkWorker::process: Processed chunk with %d lines, matched %d lines.",
        total_lines, lines_matched);
}

std::vector<dob::DobJobApplication>& ChunkWorker::get_results() {
    return results_;
}

void ChunkWorker::clear_results() {
    results_.clear();
}

} // namespace parallel
