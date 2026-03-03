#include "ParallelQueryProcessor.hpp"
#include "ChunkWorker.hpp"
#include "ThreadPool.hpp"
#include "../csv/CsvIndexedFile.hpp"
#include <vector>

namespace parallel {

ParallelQueryProcessor::ParallelQueryProcessor(CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size)
    : csv_file_(csv_file), thread_pool_size_(thread_pool_size), chunk_size_(chunk_size) {}

std::vector<dob::DobJobApplication> ParallelQueryProcessor::execute(query::Query& q) {
    std::size_t total_rows = csv_file_.row_count();

    // Create a persistent thread pool
    ThreadPool pool(thread_pool_size_);
    std::vector<std::vector<dob::DobJobApplication>> thread_results;

    // Create worker (with chunk_size for reserving space)
    ChunkWorker worker(q, chunk_size_);

    // Process chunks with fixed-size thread pool
    std::size_t chunk_id = 0;
    std::size_t rows_processed = 0;

    // Seek to the beginning of the file
    csv_file_.seek_row(0);

    while (rows_processed < total_rows) {
        std::size_t rows_in_chunk = std::min(chunk_size_, total_rows - rows_processed);

        // Read the chunk
        std::string chunk = csv_file_.read_rows(static_cast<int>(rows_in_chunk));

        // Allocate a unique result vector for this chunk
        thread_results.emplace_back();
        std::size_t current_chunk_id = chunk_id;

        // Enqueue task to process this chunk
        pool.enqueue([&worker, current_chunk_id, chunk, &thread_results]() {
            worker.process(current_chunk_id, chunk, thread_results);
        });

        rows_processed += rows_in_chunk;
        chunk_id++;
    }

    // Wait for all tasks to complete
    pool.wait_all();

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
