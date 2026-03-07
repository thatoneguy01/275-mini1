#include "ParallelQueryProcessor.hpp"
#include "ChunkWorker.hpp"
#include "ThreadPool.hpp"
#include "../csv/CsvIndexedFile.hpp"
#include <vector>
#include <memory>

#undef ENABLE_LOGGING
#include "../logging.hpp"

namespace parallel {

ParallelQueryProcessor::ParallelQueryProcessor(const CsvIndexedFile& csv_file, std::size_t thread_pool_size, std::size_t chunk_size)
    : csv_file_(csv_file), thread_pool_size_(thread_pool_size), chunk_size_(chunk_size) {}

void ParallelQueryProcessor::execute(query::Query& q, std::vector<dob::DobJobApplication>& out_results) {
    std::size_t total_rows = csv_file_.row_count();

    // Create a persistent thread pool with integrated workers
    ThreadPool pool(thread_pool_size_, chunk_size_);

    // Get index access for efficient row reading
    const uint64_t* offsets = csv_file_.get_offsets();
    std::string csv_path = csv_file_.get_csv_path();

    // Process chunks with fixed-size thread pool
    std::size_t chunk_id = 0;
    std::size_t rows_processed = 0;

    // Seek to the beginning of the file
    csv_file_.seek_row(0);

    while (rows_processed < total_rows) {
        LOG("ParallelQueryProcessor::execute: Enqueuing chunk %zu (rows %zu to %zu)",
               chunk_id, rows_processed, std::min(rows_processed + chunk_size_, total_rows) - 1);
        std::size_t rows_in_chunk = std::min(chunk_size_, total_rows - rows_processed);
        std::size_t start_row = rows_processed;
        std::size_t end_row = rows_processed + rows_in_chunk;

        // Read the chunk
        std::string chunk = csv_file_.read_rows(static_cast<int>(rows_in_chunk));

        // Store chunk in shared_ptr to manage lifetime safely
        auto chunk_ptr = std::make_shared<std::string>(std::move(chunk));

        // Calculate the chunk's starting byte offset
        uint64_t chunk_start_offset = offsets[start_row];

        // Enqueue task to process this chunk
        pool.enqueue(chunk_ptr, [&q, offsets, start_row, end_row, total_rows, chunk_start_offset](
                const std::shared_ptr<std::string>& thread_chunk,
                const std::shared_ptr<ChunkWorker>& worker) {

            // Process each row in the chunk using offsets
            for (std::size_t row_idx = start_row; row_idx < end_row; ++row_idx) {
                // Calculate the position of this row within the chunk
                uint64_t row_start = offsets[row_idx];
                uint64_t row_end;

                if (row_idx + 1 < total_rows) {
                    row_end = offsets[row_idx + 1];
                } else {
                    // Last row - use chunk end
                    row_end = chunk_start_offset + thread_chunk->size();
                }

                // Extract the line from the chunk data
                uint64_t row_offset_in_chunk = row_start - chunk_start_offset;
                uint64_t row_length = row_end - row_start;

                // Remove trailing newline if present
                if (row_length > 0 && (*thread_chunk)[row_offset_in_chunk + row_length - 1] == '\n') {
                    row_length--;
                }
                if (row_length > 0 && (*thread_chunk)[row_offset_in_chunk + row_length - 1] == '\r') {
                    row_length--;
                }

                if (row_length == 0) {
                    continue;
                }

                std::string_view line(thread_chunk->data() + row_offset_in_chunk, row_length);

                if (q.eval(line)) {
                    try {
                        worker->add_result(dob::parse_row(line));
                    } catch (const std::exception& e) {
                        // Handle parse error (e.g., log it)
                    }
                }
            }

            LOG("ChunkProcessor: Processed chunk with %zu rows.", end_row - start_row);
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

    // Clear intermediate storage to free memory
    all_worker_results.clear();
}

} // namespace parallel
