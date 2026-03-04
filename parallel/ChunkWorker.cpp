#include "ChunkWorker.hpp"
#include <sstream>

#define ENABLE_LOGGING 1
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
    LOG("ChunkWorker::worker_thread_main: Worker thread started");

    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            LOG("ChunkWorker::worker_thread_main: Waiting for task - pending_tasks=%zu, queue_size=%zu, shutdown=%d",
                pending_tasks_, task_queue_.size(), shutdown_);
            condition_.wait(lock, [this]() { return !task_queue_.empty() || shutdown_; });

            if (shutdown_ && task_queue_.empty()) {
                LOG("ChunkWorker::worker_thread_main: Shutting down");
                break;
            }

            if (task_queue_.empty()) {
                LOG("ChunkWorker::worker_thread_main: Woke up but queue is empty, continuing");
                continue;
            }

            task = std::move(task_queue_.front());
            task_queue_.pop();
            LOG("ChunkWorker::worker_thread_main: Got task, queue_size now=%zu", task_queue_.size());
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
                LOG("ChunkWorker::worker_thread_main: Decremented pending_tasks to %zu", pending_tasks_);
            }
            condition_.notify_one();
        }
    }

    LOG("ChunkWorker::worker_thread_main: Worker thread exiting");
}

void ChunkWorker::enqueue_task(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        ++pending_tasks_;
        task_queue_.push(std::move(task));
        LOG("ChunkWorker::enqueue_task: Enqueued task, pending_tasks=%zu, queue_size=%zu",
            pending_tasks_, task_queue_.size());
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
    LOG("ChunkWorker::wait_for_completion: Starting wait");
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        LOG("ChunkWorker::wait_for_completion: Initial state - pending_tasks=%zu, queue_size=%zu",
            pending_tasks_, task_queue_.size());

        while (pending_tasks_ > 0 || !task_queue_.empty()) {
            LOG("ChunkWorker::wait_for_completion: Waiting - pending_tasks=%zu, queue_size=%zu",
                pending_tasks_, task_queue_.size());
            condition_.wait(lock);
            LOG("ChunkWorker::wait_for_completion: Woke up - pending_tasks=%zu, queue_size=%zu",
                pending_tasks_, task_queue_.size());
        }
    }
    LOG("ChunkWorker::wait_for_completion: Completed");
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
