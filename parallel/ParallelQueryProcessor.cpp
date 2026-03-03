#include "ParallelQueryProcessor.hpp"
#include "ChunkWorker.hpp"
#include "../csv/CsvIndexedFile.hpp"
#include <thread>
#include <vector>

namespace parallel {

ParallelQueryProcessor::ParallelQueryProcessor(CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size)
    : csv_file_(csv_file), thread_pool_size_(thread_pool_size), chunk_size_(chunk_size) {}

std::vector<dob::DobJobApplication> ParallelQueryProcessor::execute(query::Query& q) {
    std::size_t total_rows = csv_file_.row_count();

    // Use fixed-size thread pool
    std::vector<std::thread> thread_pool;
    std::vector<std::vector<dob::DobJobApplication>> thread_results;

    // Create worker (with chunk_size for reserving space)
    ChunkWorker worker(q, chunk_size_);

    // Process chunks with fixed-size thread pool
    std::size_t tid = 0;
    std::size_t rows_processed = 0;

    // Seek to the beginning of the file
    csv_file_.seek_row(0);

    while (rows_processed < total_rows) {
        // If thread pool is full, wait for one to finish
        if (thread_pool.size() >= thread_pool_size_) {
            thread_pool[tid % thread_pool_size_].join();
            thread_pool.erase(thread_pool.begin() + (tid % thread_pool_size_));
        }

        // Allocate result vector for this thread if needed
        if (thread_results.size() <= tid) {
            thread_results.resize(tid + 1);
        }

        std::size_t rows_in_chunk = std::min(chunk_size_, total_rows - rows_processed);

        // Read the chunk
        std::string chunk = csv_file_.read_rows(static_cast<int>(rows_in_chunk));

        // Create a thread to process this chunk
        thread_pool.emplace_back([&worker, tid, chunk, &thread_results]() {
            worker.process(tid, chunk, thread_results);
        });

        rows_processed += rows_in_chunk;
        tid++;
    }

    // Wait for all remaining threads to complete
    for (auto& thread : thread_pool) {
        thread.join();
    }

    // Calculate total size and reserve space
    std::size_t total_size = 0;
    for (auto& v : thread_results)
        total_size += v.size();

    std::vector<dob::DobJobApplication> final_results;
    final_results.reserve(total_size);

    // Merge results using move semantics
    for (auto& v : thread_results) {
        final_results.insert(final_results.end(),
                             std::make_move_iterator(v.begin()),
                             std::make_move_iterator(v.end()));
    }

    return final_results;
}

} // namespace parallel
