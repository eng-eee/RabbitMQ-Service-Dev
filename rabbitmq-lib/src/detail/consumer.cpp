#include "consumer.hpp"

#include "amqp_util.hpp"
#include "util.hpp"

namespace rmq::detail {

Consumer::Consumer(std::shared_ptr<ConnectionCore> core, ConsumerConfig config, DeliveryFn fn)
    : core_(std::move(core)), cfg_(std::move(config)), fn_(std::move(fn)) {
    queueName_ = cfg_.queue;
}

void Consumer::start() {
    std::weak_ptr<Consumer> weak = shared_from_this();
    mc_ = std::make_shared<ManagedChannel>(
        core_, cfg_.name,
        [weak](AMQP::Channel& ch) {
            if (auto self = weak.lock()) self->setup(ch);
        },
        [](const Status&) {});
    mc_->open();
}

void Consumer::stop() {
    if (mc_) mc_->close();
}

std::string Consumer::queueName() const {
    std::lock_guard<std::mutex> lock(nameMutex_);
    return queueName_;
}

void Consumer::setup(AMQP::Channel& channel) {
    std::weak_ptr<Consumer> weak = shared_from_this();

    if (cfg_.prefetch > 0 && !cfg_.auto_ack) channel.setQos(cfg_.prefetch);

    int flags = 0;
    if (cfg_.durable) flags |= AMQP::durable;
    if (cfg_.exclusive) flags |= AMQP::exclusive;
    if (cfg_.auto_delete) flags |= AMQP::autodelete;

    channel.declareQueue(cfg_.queue, flags)
        .onSuccess([weak](const std::string& name, std::uint32_t, std::uint32_t) {
            auto self = weak.lock();
            if (!self) return;
            AMQP::Channel* ch = self->mc_->channel();
            if (!ch) return;

            {
                std::lock_guard<std::mutex> lock(self->nameMutex_);
                self->queueName_ = name;
            }
            for (const Binding& b : self->cfg_.bindings) ch->bindQueue(b.exchange, name, b.routing_key);

            ch->consume(name, self->cfg_.auto_ack ? AMQP::noack : 0)
                .onReceived([weak](const AMQP::Message& m, std::uint64_t tag, bool redelivered) {
                    if (auto s = weak.lock()) s->onMessage(m, tag, redelivered);
                });
        });
}

void Consumer::onMessage(const AMQP::Message& message, std::uint64_t tag, bool redelivered) {
    AMQP::Channel* ch = mc_->channel();
    if (!ch) return;

    const Delivery delivery = makeDelivery(message, tag, redelivered);

    AckAction action = AckAction::Ack;
    try {
        action = fn_(*ch, delivery);
    } catch (const std::exception& e) {
        core_->reportError(Status{ErrorCode::Internal, cfg_.name + ": handler threw: " + e.what()});
        action = AckAction::NackDiscard;
    } catch (...) {
        core_->reportError(Status{ErrorCode::Internal, cfg_.name + ": handler threw"});
        action = AckAction::NackDiscard;
    }

    if (cfg_.auto_ack) return;
    // The handler may have caused the channel to go away.
    ch = mc_->channel();
    if (!ch) return;

    switch (action) {
        case AckAction::Ack:         ch->ack(tag); break;
        case AckAction::NackRequeue: ch->reject(tag, AMQP::requeue); break;
        case AckAction::NackDiscard: ch->reject(tag); break;
    }
}

}  // namespace rmq::detail
