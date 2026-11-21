#pragma once

// Private: keeps one AMQP channel alive across connection losses and
// channel-level errors, and gives the owning component hooks to (re)build its
// topology. Everything except open()/close() runs on the connection's IO thread.

#include "connection_core.hpp"

#include <rmq/error.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <string>

namespace rmq::detail {

class ManagedChannel : public std::enable_shared_from_this<ManagedChannel> {
public:
    /// IO thread. The channel was created: declare queues, start consumers, flush work.
    using SetupFn = std::function<void(AMQP::Channel&)>;
    /// IO thread. The channel is gone (connection lost, channel error, or closed).
    /// Called *before* the channel object is destroyed. status.code == Closed means
    /// close() was called and the channel will not come back.
    using LostFn = std::function<void(const Status&)>;

    ManagedChannel(std::shared_ptr<ConnectionCore> core, std::string name, SetupFn setup, LostFn lost);

    /// Any thread. Starts following the connection; the channel appears once it is up.
    void open();

    /// Any thread (blocks until done unless called on the IO thread). Idempotent.
    void close();

    /// IO thread only.
    AMQP::Channel* channel() const noexcept { return channel_.get(); }

private:
    void create(AMQP::Connection& connection);
    void drop(const Status& status);
    void scheduleReopen();

    const std::shared_ptr<ConnectionCore> core_;
    const std::string name_;
    SetupFn setup_;
    LostFn lost_;

    std::atomic<bool> opened_{false};
    std::atomic<bool> closeStarted_{false};
    std::uint64_t observerId_ = 0;

    // IO-thread only
    std::unique_ptr<AMQP::Channel> channel_;
    std::uint64_t generation_ = 0;
    bool closed_ = false;
    bool reopenPending_ = false;
};

}  // namespace rmq::detail
