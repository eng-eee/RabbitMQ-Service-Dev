#pragma once

#include "app_common/config.hpp"
#include "app_common/event.hpp"
#include "app_common/worker.hpp"

#include <rmq/rmq.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace app {

/// Common plumbing shared by every executable: configuration, one RabbitMQ
/// connection, a publisher for the event exchange, a worker thread and a set of
/// subscribers. Derived services implement `start()` and enqueue deliveries to
/// the worker.
class Service {
public:
    explicit Service(std::string name);
    virtual ~Service();

    Service(const Service&) = delete;
    Service& operator=(const Service&) = delete;

    const std::string& name() const noexcept { return name_; }

    virtual bool init();
    virtual bool start() = 0;
    virtual void stop();

protected:
    void subscribe(const std::string& queue,
                   std::vector<rmq::Binding> bindings,
                   rmq::Subscriber::Handler handler,
                   std::uint16_t prefetch = 20);

    bool publishEvent(const Event& event, const std::string& routing_key = "");
    void enqueue(std::function<void()> task);

    Worker& worker() { return *worker_; }
    const Config& config() const noexcept { return config_; }
    const std::string& exchange() const noexcept { return exchange_; }

private:
    void ensureExchange();

    std::string name_;
    Config config_;
    std::string exchange_ = "order.events";
    std::unique_ptr<rmq::Connection> connection_;
    std::unique_ptr<rmq::Publisher> publisher_;
    std::unique_ptr<Worker> worker_;
    std::vector<std::unique_ptr<rmq::Subscriber>> subscribers_;
    std::atomic<bool> stopped_{false};
};

}  // namespace app
