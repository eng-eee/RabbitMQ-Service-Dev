#include "app_common/event.hpp"
#include "app_common/logging.hpp"
#include "app_common/service.hpp"
#include "log.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

struct Trace {
    std::string order_id;
    std::string last_status;
    std::string last_event;
    bool finalized = false;
    bool failed = false;
    bool warned = false;
    std::chrono::system_clock::time_point first_seen = std::chrono::system_clock::now();
    std::chrono::system_clock::time_point last_seen = std::chrono::system_clock::now();
    std::vector<std::string> history;
};

class OrderMonitorService : public app::Service {
public:
    OrderMonitorService() : Service("order-monitor-service") {}

    ~OrderMonitorService() override { stop(); }

    bool init() override {
        if (!Service::init()) return false;
        timeoutSeconds_ = config().getInt("MONITOR_INCOMPLETE_TIMEOUT_SECONDS", 60);
        intervalSeconds_ = config().getInt("MONITOR_INTERVAL_SECONDS", 5);
        if (intervalSeconds_ <= 0) intervalSeconds_ = 5;
        LOG(INFO) << "monitor settings: incomplete_timeout=" << timeoutSeconds_
                  << "s interval=" << intervalSeconds_ << "s\n";
        return true;
    }

    bool start() override {
        subscribe("monitor.events",
                  {{exchange(), "order.#"},
                   {exchange(), "inventory.#"},
                   {exchange(), "payment.#"}},
                  [this](const rmq::Delivery& delivery) {
                      enqueue([this, delivery] { process(delivery); });
                      return rmq::AckAction::Ack;
                  });

        timer_ = std::thread(&OrderMonitorService::timerLoop, this);
        return true;
    }

    void stop() override {
        {
            std::lock_guard<std::mutex> lock(timerMutex_);
            timerStopping_ = true;
        }
        timerCv_.notify_all();
        if (timer_.joinable()) {
            timer_.join();
        }
        Service::stop();
    }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, Trace> traces_;
    long long timeoutSeconds_ = 60;
    long long intervalSeconds_ = 5;

    std::thread timer_;
    std::mutex timerMutex_;
    std::condition_variable timerCv_;
    bool timerStopping_ = false;

    void process(const rmq::Delivery& delivery) {
        app::Event event;
        try {
            event = app::Event::fromJson(delivery.message.body);
        } catch (const std::exception& e) {
            LOG(ERROR) << "[monitor] failed to parse lifecycle event: " << e.what() << "\n";
            return;
        }

        if (event.order_id.empty()) {
            if (event.type == app::event_type::OrderRejected) {
                LOG(WARN) << "[monitor] order request rejected: "
                          << event.data["reason"].asString() << "\n";
            }
            return;
        }

        const std::string status = event.data["status"].asString(event.type);
        const bool failedEvent = event.type == app::event_type::InventoryRejected ||
                                 event.type == app::event_type::PaymentFailed ||
                                 event.type == app::event_type::OrderRejected ||
                                 event.type == app::event_type::OrderCancelled;
        const bool finalEvent = event.type == app::event_type::OrderCompleted ||
                                event.type == app::event_type::OrderCancelled;

        std::lock_guard<std::mutex> lock(mutex_);
        Trace& trace = traces_[event.order_id];
        if (trace.order_id.empty()) {
            trace.order_id = event.order_id;
            trace.first_seen = std::chrono::system_clock::now();
        }
        trace.last_event = event.type;
        trace.last_status = status;
        trace.last_seen = std::chrono::system_clock::now();
        trace.warned = false;
        if (failedEvent) trace.failed = true;
        if (finalEvent) trace.finalized = true;
        trace.history.push_back(app::nowIso8601() + " " + event.type + " " + status);

        LOG(INFO) << "[monitor] order " << event.order_id << " event=" << event.type
                  << " status=" << status << "\n";

        if (failedEvent) {
            LOG(WARN) << "[monitor] failure observed for order " << event.order_id
                      << " via " << event.type << "\n";
        }

        if (finalEvent) {
            LOG(INFO) << "[monitor] order " << event.order_id << " history:\n";
            for (const auto& entry : trace.history) {
                LOG(INFO) << "[monitor]   " << entry << "\n";
            }
        }
    }

    void timerLoop() {
        std::unique_lock<std::mutex> lock(timerMutex_);
        while (!timerStopping_) {
            if (timerCv_.wait_for(lock, std::chrono::seconds(intervalSeconds_),
                                  [this] { return timerStopping_; })) {
                break;
            }
            lock.unlock();
            checkIncomplete();
            lock.lock();
        }
    }

    void checkIncomplete() {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = std::chrono::system_clock::now();
        for (auto& entry : traces_) {
            Trace& trace = entry.second;
            if (trace.finalized || trace.warned) continue;
            const auto age = std::chrono::duration_cast<std::chrono::seconds>(
                                 now - trace.last_seen)
                                 .count();
            if (age >= timeoutSeconds_) {
                trace.warned = true;
                LOG(WARN) << "[monitor] order " << trace.order_id
                          << " is incomplete after " << age << "s (last_event="
                          << trace.last_event << ", status=" << trace.last_status << ")\n";
            }
        }
    }
};

}  // namespace

int main() {
    OrderMonitorService service;
    return app::runService(service);
}
