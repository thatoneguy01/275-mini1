#pragma once
#include <vector>
#include <memory>
#include <atomic>
#include <functional>

#include "ChunkWorker.hpp"
#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"

namespace parallel {

class ThreadPool {
public:
    ThreadPool(std::size_t num_threads, std::size_t chunk_size);
    ~ThreadPool();

    // Enqueue a task for a chunk to be processed by a worker
    // The task function receives the chunk pointer and worker pointer
    template<typename F>
    void enqueue(std::shared_ptr<std::string> chunk, F&& task) {
        // Round-robin task distribution to workers
        std::size_t worker_idx = next_worker_idx_.fetch_add(1) % workers_.size();
        auto& worker = workers_[worker_idx];

        // Create wrapped task and enqueue it to the worker
        worker->enqueue_task([chunk, t = std::forward<F>(task), worker]() mutable {
            t(chunk, worker);
        });
    }

    void wait_all();

    void get_all_results(std::vector<std::vector<dob::DobJobApplication>>& all_results);

private:

    std::vector<std::shared_ptr<ChunkWorker>> workers_;
    std::atomic<std::size_t> next_worker_idx_{0};
};

} // namespace parallel

