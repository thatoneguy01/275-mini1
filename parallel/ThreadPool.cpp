#include "ThreadPool.hpp"
#include "ChunkWorker.hpp"

#define ENABLE_LOGGING 1
#include "../logging.hpp"

namespace parallel {

ThreadPool::ThreadPool(std::size_t num_threads, query::Query& query, std::size_t chunk_size) {
    // Create one ChunkWorker per thread
    for (std::size_t i = 0; i < num_threads; ++i) {
        workers_.push_back(std::make_shared<ChunkWorker>(query, chunk_size));
    }

    // Create worker threads
    for (std::size_t i = 0; i < num_threads; ++i) {
        threads_.emplace_back([this]() { worker_thread(); });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        shutdown_ = true;
    }
    condition_.notify_all();
    for (auto& thread : threads_) {
        thread.join();
    }
}

void ThreadPool::worker_thread() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            condition_.wait(lock, [this]() { return !tasks_.empty() || shutdown_; });

            if (shutdown_ && tasks_.empty()) {
                break;
            }

            if (tasks_.empty()) {
                continue;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
        }
        LOG("ThreadPool::worker_thread: Starting task on thread %zu", std::hash<std::thread::id>{}(std::this_thread::get_id()));
        task();
        LOG("ThreadPool::worker_thread: Finished task on thread %zu.",
                    std::hash<std::thread::id>{}(std::this_thread::get_id()));
        {
            std::lock_guard<std::mutex> lock(active_mutex_);
            if (pending_tasks_ > 0) {
                --pending_tasks_;
            }
            if (pending_tasks_ == 0) {
                active_condition_.notify_all();
            }
        }
    }
}

void ThreadPool::wait_all() {
    std::unique_lock<std::mutex> lock(active_mutex_);
    active_condition_.wait(lock, [this]() { return pending_tasks_ == 0; });
}

std::shared_ptr<ChunkWorker> ThreadPool::worker_at(std::size_t idx) {
    return workers_.at(idx);
}

std::vector<std::vector<dob::DobJobApplication>>& ThreadPool::get_all_results() {
    std::vector<std::vector<dob::DobJobApplication>> all_results;
    for (auto& worker : workers_) {
        all_results.push_back(std::move(worker->get_results()));
    }
    return all_results;
}

} // namespace parallel
