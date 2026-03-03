#include "ChunkWorker.hpp"
#include <sstream>

namespace parallel {

ChunkWorker::ChunkWorker(query::Query& query, std::size_t chunk_size)
    : query_(query), chunk_size_(chunk_size) {}

void ChunkWorker::process(std::size_t tid, const std::string& chunk,
                          std::vector<std::vector<dob::DobJobApplication>>& thread_results) {
    auto& local = thread_results[tid];
    local.reserve(chunk_size_);

    // Parse lines from the chunk string
    std::istringstream stream(chunk);
    std::string line;

    while (std::getline(stream, line)) {
        if (line.empty())
            continue;

        if (query_.eval(line)) {
            try {
                local.push_back(dob::parse_row(line));
            } catch (const std::exception& e) {
                // Handle parse error (e.g., log it)
            }
        }
    }
}

} // namespace parallel
