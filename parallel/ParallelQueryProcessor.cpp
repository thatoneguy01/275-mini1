#include "ParallelQueryProcessor.hpp"
#include "ChunkWorker.hpp"
#include "ThreadPool.hpp"
#include "../csv/CsvIndexedFile.hpp"
#include <vector>
#include <memory>

#define ENABLE_LOGGING 1
#include "../logging.hpp"

namespace parallel {

ParallelQueryProcessor::ParallelQueryProcessor(CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size)
    : csv_file_(csv_file), thread_pool_size_(thread_pool_size), chunk_size_(chunk_size) {}

std::vector<dob::DobJobApplication>& ParallelQueryProcessor::execute(query::Query& q) {
    std::size_t total_rows = csv_file_.row_count();

    // Create a persistent thread pool with integrated workers
    ThreadPool pool(thread_pool_size_, q, chunk_size_);

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

        // Assign this chunk to a worker based on which thread will process it
        std::size_t worker_id = chunk_id % thread_pool_size_;

        // Store chunk in shared_ptr to manage lifetime safely
        auto chunk_ptr = std::make_shared<std::string>(std::move(chunk));

        // Enqueue task to process this chunk (worker accumulates in its own vector)
        pool.enqueue([worker_id, chunk_ptr, &pool]() {
            auto worker = pool.worker_at(worker_id);
            worker->process(*chunk_ptr);
        });

        rows_processed += rows_in_chunk;
        chunk_id++;
    }

    LOG("ParallelQueryProcessor::execute: Finished Enqueuing all chunks. Total chunks: %zu", chunk_id);
    // Wait for all tasks to complete
    pool.wait_all();

    LOG("ParallelQueryProcessor::execute: All tasks completed. Collecting results...");
    // Collect results from all workers
    auto& all_worker_results = pool.get_all_results();

    // Calculate total size
    std::size_t total_size = 0;
    for (auto& results : all_worker_results) {
        total_size += results.size();
    }

    std::vector<dob::DobJobApplication> final_results;
    final_results.reserve(total_size);

    // Merge results from all workers using move semantics
    for (auto& results : all_worker_results) {
        final_results.insert(final_results.end(),
                             std::make_move_iterator(results.begin()),
                             std::make_move_iterator(results.end()));
    }

    return final_results;
}

} // namespace parallel
