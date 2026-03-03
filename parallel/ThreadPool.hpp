#pragma once
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <vector>
#include <memory>

#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"

namespace parallel {

class ChunkWorker;

class ThreadPool {
public:
    ThreadPool(std::size_t num_threads, query::Query& query, std::size_t chunk_size);
    ~ThreadPool();

    template<typename F>
    void enqueue(F&& task) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            tasks_.push(std::forward<F>(task));
        }
        condition_.notify_one();
    }

    void wait_all();

    std::vector<std::vector<dob::DobJobApplication>> get_all_results();

    std::vector<std::shared_ptr<ChunkWorker>> workers_;

private:
    void worker_thread();

    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool shutdown_ = false;
    std::size_t active_tasks_ = 0;
    std::mutex active_mutex_;
    std::condition_variable active_condition_;
};

} // namespace parallel

