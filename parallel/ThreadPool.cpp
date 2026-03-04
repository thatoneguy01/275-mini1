#include "ThreadPool.hpp"
#include "ChunkWorker.hpp"

#define ENABLE_LOGGING 1
#include "../logging.hpp"

namespace parallel {

ThreadPool::ThreadPool(std::size_t num_threads, std::size_t chunk_size) {
    // Create num_threads ChunkWorkers, each with its own thread
    for (std::size_t i = 0; i < num_threads; ++i) {
        workers_.push_back(std::make_shared<ChunkWorker>(chunk_size));
    }
}

ThreadPool::~ThreadPool() {
    wait_all();
    // Workers will be destroyed and their threads will be joined in ChunkWorker destructor
}

void ThreadPool::wait_all() {
    // Wait for all workers to complete their tasks
    LOG("ThreadPool::wait_all: Starting, waiting for %zu workers", workers_.size());

    for (std::size_t i = 0; i < workers_.size(); ++i) {
        LOG("ThreadPool::wait_all: Waiting for worker %zu to complete", i);
        workers_[i]->wait_for_completion();
        LOG("ThreadPool::wait_all: Worker %zu completed", i);
    }

    LOG("ThreadPool::wait_all: All workers completed");
}


void ThreadPool::get_all_results(std::vector<std::vector<dob::DobJobApplication>>& all_results) {
    for (auto& worker : workers_) {
        all_results.push_back(std::move(worker->get_results()));
    }
}

} // namespace parallel
