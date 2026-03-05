#include "ParallelQueryProcessor.hpp"
#include "ChunkWorker.hpp"
#include "ThreadPool.hpp"
#include "../csv/CsvIndexedFile.hpp"
#include <vector>
#include <memory>
#include <sstream>

#undef ENABLE_LOGGING
#include "../logging.hpp"

namespace parallel {

ParallelQueryProcessor::ParallelQueryProcessor(CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size)
    : csv_file_(csv_file), thread_pool_size_(thread_pool_size), chunk_size_(chunk_size) {}

void ParallelQueryProcessor::execute(query::Query& q, std::vector<dob::DobJobApplication>& out_results) {
    std::size_t total_rows = csv_file_.row_count();

    // Create a persistent thread pool with integrated workers
    ThreadPool pool(thread_pool_size_, chunk_size_);

    // Process chunks with fixed-size thread pool
    std::size_t chunk_id = 0;
    std::size_t rows_processed = 0;

    // Seek to the beginning of the file
    csv_file_.seek_row(0);

    while (rows_processed < total_rows) {
        LOG("ParallelQueryProcessor::execute: Enqueuing chunk %zu (rows %zu to %zu)",
               chunk_id, rows_processed, std::min(rows_processed + chunk_size_, total_rows) - 1);
        std::size_t rows_in_chunk = std::min(chunk_size_, total_rows - rows_processed);

        // Read the chunk
        std::string chunk = csv_file_.read_rows(static_cast<int>(rows_in_chunk));

        // Store chunk in shared_ptr to manage lifetime safely
        auto chunk_ptr = std::make_shared<std::string>(std::move(chunk));

        // Enqueue task to process this chunk
        // The task receives the chunk pointer and the ChunkWorker pointer
        pool.enqueue(chunk_ptr, [&q](const std::shared_ptr<std::string>& thread_chunk, const std::shared_ptr<ChunkWorker>& worker) {
            // Process chunk - parse lines and filter by query
            std::istringstream stream(*thread_chunk);
            std::string line;
            int total_lines = 0;
            int lines_matched = 0;

            //TODO: get lines unsing indexes
            while (std::getline(stream, line)) {
                ++total_lines;
                if (line.empty())
                    continue;

                if (q.eval(line)) {
                    try {
                        worker->add_result(dob::parse_row(line));
                        ++lines_matched;
                    } catch (const std::exception& e) {
                        // Handle parse error (e.g., log it)
                    }
                }
            }

            LOG("ChunkProcessor: Processed chunk with %d lines, matched %d lines.",
                total_lines, lines_matched);
        });

        rows_processed += rows_in_chunk;
        chunk_id++;
    }

    LOG("ParallelQueryProcessor::execute: Finished Enqueuing all chunks. Total chunks: %zu", chunk_id);
    // Wait for all tasks to complete
    pool.wait_all();

    LOG("ParallelQueryProcessor::execute: All tasks completed. Collecting results...");
    // Collect results from all workers
    std::vector<std::vector<dob::DobJobApplication>> all_worker_results;
    pool.get_all_results(all_worker_results);

    // Calculate total size
    std::size_t total_size = 0;
    for (auto& results : all_worker_results) {
        total_size += results.size();
    }

    out_results.reserve(total_size);

    // Merge results from all workers using move semantics
    for (auto& results : all_worker_results) {
        out_results.insert(out_results.end(),
                             std::make_move_iterator(results.begin()),
                             std::make_move_iterator(results.end()));
    }
}

} // namespace parallel
