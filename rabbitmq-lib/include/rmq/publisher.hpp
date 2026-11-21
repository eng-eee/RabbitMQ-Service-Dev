#pragma once

#include <rmq/connection.hpp>
#include <rmq/message.hpp>

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>

namespace rmq {

struct PublisherOptions {
    std::string exchange;                  // "" = default exchange
    std::string exchange_type = "direct";  // used only when declare_exchange is set
    bool declare_exchange = false;
    bool durable = true;
    bool confirms = true;                  // use publisher confirms
    std::size_t max_pending = 10000;       // back-pressure limit for async publishing
};

/// Publish side of the pub/sub model. Survives reconnects (the channel is
/// re-created and the exchange re-declared automatically).
///
/// Delivery semantics:
///  - Messages accepted while the connection is down are queued (up to
///    max_pending) and sent after the reconnect.
///  - Messages that were already sent but not yet confirmed when the connection
///    drops are NOT resent (that could duplicate); their completion reports an error.
///  - The Completion may run inline on the calling thread when the publisher is
///    full (Overflow) or the connection was stopped (Closed); otherwise on the IO thread.
class Publisher {
public:
    /// Called from the IO thread once the broker confirmed (or the publish failed).
    using Completion = std::function<void(const Status&)>;

    Publisher(Connection& connection, PublisherOptions options = {});
    ~Publisher();

    Publisher(const Publisher&) = delete;
    Publisher& operator=(const Publisher&) = delete;

    /// Non-blocking. Enqueues the message; `done` (optional) reports the outcome.
    void publishAsync(const std::string& routing_key, Message message, Completion done = {});

    /// Blocking: returns after the broker confirmed or after `timeout`.
    /// Must not be called from the IO thread (i.e. from a rmq callback).
    Status publish(const std::string& routing_key, Message message,
                   std::chrono::milliseconds timeout = std::chrono::seconds(5));

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace rmq
