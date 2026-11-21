#pragma once

// Private: queue consumer shared by Subscriber and RpcServer.
// Declares the queue, bindings and QoS, consumes, acks according to the
// delivery function's result, and rebuilds everything after a reconnect.

#include "managed_channel.hpp"

#include <rmq/subscriber.hpp>

#include <mutex>

namespace rmq::detail {

struct ConsumerConfig {
    std::string name = "consumer";  // used in error messages
    std::string queue;
    bool durable = true;
    bool exclusive = false;
    bool auto_delete = false;
    std::vector<Binding> bindings;
    std::uint16_t prefetch = 50;
    bool auto_ack = false;
};

class Consumer : public std::enable_shared_from_this<Consumer> {
public:
    /// Runs on the IO thread. May use the channel (e.g. to publish a reply).
    using DeliveryFn = std::function<AckAction(AMQP::Channel&, const Delivery&)>;

    Consumer(std::shared_ptr<ConnectionCore> core, ConsumerConfig config, DeliveryFn fn);

    void start();
    void stop();  // blocking; no delivery callback runs after it returned
    std::string queueName() const;

private:
    void setup(AMQP::Channel& channel);
    void onMessage(const AMQP::Message& message, std::uint64_t tag, bool redelivered);

    const std::shared_ptr<ConnectionCore> core_;
    const ConsumerConfig cfg_;
    const DeliveryFn fn_;
    std::shared_ptr<ManagedChannel> mc_;

    mutable std::mutex nameMutex_;
    std::string queueName_;
};

}  // namespace rmq::detail
