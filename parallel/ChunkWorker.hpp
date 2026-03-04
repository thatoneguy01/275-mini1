#pragma once
#include <vector>
#include <string>
#include <cstddef>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <memory>

#include "../dob/DobJobApplication.hpp"
#include "../query/Querys.hpp"

namespace parallel {

class ChunkWorker {
public:
    ChunkWorker(query::Query& query, std::size_t chunk_size);
    ~ChunkWorker();

    void enqueue_task(std::function<void()> task);
    void shutdown();
    void wait_for_completion();

    std::vector<dob::DobJobApplication>& get_results();
    void clear_results();

    // Add a result to the worker's result collection (thread-safe within worker's own thread)
    void add_result(dob::DobJobApplication&& result);

private:
    void worker_thread_main();

    query::Query& query_;
    std::size_t chunk_size_;
    std::vector<dob::DobJobApplication> results_;

    std::unique_ptr<std::thread> thread_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool shutdown_ = false;
    std::size_t pending_tasks_ = 0;
};

} // namespace parallel

