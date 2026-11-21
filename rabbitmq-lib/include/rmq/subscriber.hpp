#pragma once

#include <rmq/connection.hpp>
#include <rmq/message.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace rmq {

enum class AckAction {
    Ack,
    NackRequeue,
    NackDiscard
};

struct Binding {
    std::string exchange;
    std::string routing_key;
};

struct SubscriberOptions {
    std::string queue;                  // empty = server-named queue
    bool durable = true;
    bool exclusive = false;
    bool auto_delete = false;
    std::vector<Binding> bindings;      // queue -> exchange bindings to (re)create
    std::uint16_t prefetch = 50;
    bool auto_ack = false;              // if true the handler's return value is ignored
};

/// Consume side of the pub/sub model. Survives reconnects (queue, bindings and
/// consumer are re-established automatically). Use durable queues if messages must
/// survive a broker restart; deliveries not acked before a connection loss are
/// redelivered by the broker (Delivery::redelivered). A handler that throws causes
/// NackDiscard and an error report through Connection's ErrorCallback.
class Subscriber {
public:
    /// Runs on the IO thread: keep it short and never block in it. Hand the
    /// work to your own thread pool if processing is slow.
    using Handler = std::function<AckAction(const Delivery&)>;

    Subscriber(Connection& connection, SubscriberOptions options);
    ~Subscriber();

    Subscriber(const Subscriber&) = delete;
    Subscriber& operator=(const Subscriber&) = delete;

    void start(Handler handler);
    void stop();

    /// Name of the queue (server-generated names are known after the first connect).
    std::string queueName() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace rmq
