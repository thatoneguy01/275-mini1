#include "ThreadPool.hpp"
#include "ChunkWorker.hpp"

#define ENABLE_LOGGING 1
#include "../logging.hpp"

namespace parallel {

ThreadPool::ThreadPool(std::size_t num_threads, std::size_t chunk_size) {
    // Create num_threads ChunkWorkers, each with its own thread
    for (std::size_t i = 0; i < num_threads; ++i) {
        // Note: ChunkWorker still needs a query reference, which must be provided by the task
        // We'll pass a placeholder for now since the actual query comes from the caller
        static thread_local query::MatchQuery dummy_query("", 0);
        workers_.push_back(std::make_shared<ChunkWorker>(dummy_query, chunk_size));
    }
}

ThreadPool::~ThreadPool() {
    wait_all();
    // Workers will be destroyed and their threads will be joined in ChunkWorker destructor
}

void ThreadPool::wait_all() {
    // Wait for all workers to complete their tasks
    for (auto& worker : workers_) {
        worker->wait_for_completion();
    }
}


void ThreadPool::get_all_results(std::vector<std::vector<dob::DobJobApplication>>& all_results) {
    for (auto& worker : workers_) {
        all_results.push_back(std::move(worker->get_results()));
    }
}

} // namespace parallel
