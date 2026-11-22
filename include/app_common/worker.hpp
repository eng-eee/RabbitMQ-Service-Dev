#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace app {

/// Single background worker. RabbitMQ handlers run on the library's IO thread
/// and must not block, so services hand deliveries to this worker.
class Worker {
public:
    explicit Worker(std::string name);
    ~Worker();

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    void enqueue(std::function<void()> task);
    void stop();

private:
    void run();

    std::string name_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    bool stopping_ = false;
};

}  // namespace app
