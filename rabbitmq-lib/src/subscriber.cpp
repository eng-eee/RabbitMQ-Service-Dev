#include <rmq/subscriber.hpp>

#include "detail/consumer.hpp"

#include <mutex>

namespace rmq {

struct Subscriber::Impl {
    std::shared_ptr<detail::ConnectionCore> core;
    SubscriberOptions opts;

    std::mutex mutex;
    std::shared_ptr<detail::Consumer> consumer;
};

Subscriber::Subscriber(Connection& connection, SubscriberOptions options) {
    for (const Binding& b : options.bindings) {
        if (b.exchange.empty()) {
            throw Error(ErrorCode::InvalidArgument, "SubscriberOptions: cannot bind to the default exchange");
        }
    }
    impl_ = std::make_shared<Impl>();
    impl_->core = connection.core();
    impl_->opts = std::move(options);
}

Subscriber::~Subscriber() { stop(); }

void Subscriber::start(Handler handler) {
    if (!handler) throw Error(ErrorCode::InvalidArgument, "Subscriber::start: empty handler");

    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->consumer) throw Error(ErrorCode::InvalidArgument, "Subscriber already started");

    detail::ConsumerConfig cfg;
    cfg.name = "subscriber";
    cfg.queue = impl_->opts.queue;
    cfg.durable = impl_->opts.durable;
    cfg.exclusive = impl_->opts.exclusive;
    cfg.auto_delete = impl_->opts.auto_delete;
    cfg.bindings = impl_->opts.bindings;
    cfg.prefetch = impl_->opts.prefetch;
    cfg.auto_ack = impl_->opts.auto_ack;

    auto consumer = std::make_shared<detail::Consumer>(
        impl_->core, std::move(cfg),
        [handler = std::move(handler)](AMQP::Channel&, const Delivery& d) { return handler(d); });
    consumer->start();
    impl_->consumer = std::move(consumer);
}

void Subscriber::stop() {
    std::shared_ptr<detail::Consumer> consumer;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        consumer = std::move(impl_->consumer);
    }
    if (consumer) consumer->stop();
}

std::string Subscriber::queueName() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->consumer ? impl_->consumer->queueName() : impl_->opts.queue;
}

}  // namespace rmq
