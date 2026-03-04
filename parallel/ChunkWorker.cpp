#include "ChunkWorker.hpp"
#include <sstream>

#undef ENABLE_LOGGING
#include "../logging.hpp"

namespace parallel {

ChunkWorker::ChunkWorker(std::size_t chunk_size)
    : chunk_size_(chunk_size) {
    // Create and start the worker thread
    thread_ = std::make_unique<std::thread>([this]() { worker_thread_main(); });
}

ChunkWorker::~ChunkWorker() {
    shutdown();
}

void ChunkWorker::worker_thread_main() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            condition_.wait(lock, [this]() { return !task_queue_.empty() || shutdown_; });

            if (shutdown_ && task_queue_.empty()) {
                break;
            }

            if (task_queue_.empty()) {
                continue;
            }

            task = std::move(task_queue_.front());
            task_queue_.pop();
        }

        LOG("ChunkWorker::worker_thread_main: Starting task on thread %zu", 
            std::hash<std::thread::id>{}(std::this_thread::get_id()));
        task();
        LOG("ChunkWorker::worker_thread_main: Finished task on thread %zu.", 
            std::hash<std::thread::id>{}(std::this_thread::get_id()));

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (pending_tasks_ > 0) {
                --pending_tasks_;
            }
        }
    }
}

void ChunkWorker::enqueue_task(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        ++pending_tasks_;
        task_queue_.push(std::move(task));
    }
    condition_.notify_one();
}

void ChunkWorker::shutdown() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_ = true;
    }
    condition_.notify_one();

    if (thread_ && thread_->joinable()) {
        thread_->join();
    }
}

void ChunkWorker::wait_for_completion() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    condition_.wait(lock, [this]() { return pending_tasks_ == 0 && task_queue_.empty(); });
}

std::vector<dob::DobJobApplication>& ChunkWorker::get_results() {
    return results_;
}

void ChunkWorker::clear_results() {
    results_.clear();
}

void ChunkWorker::add_result(dob::DobJobApplication&& result) {
    results_.push_back(std::move(result));
}

} // namespace parallel
