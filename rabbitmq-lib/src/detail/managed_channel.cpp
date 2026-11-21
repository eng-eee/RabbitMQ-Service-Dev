#include "managed_channel.hpp"

#include <future>

namespace rmq::detail {

namespace {
constexpr std::chrono::milliseconds kReopenDelay{1000};
}

ManagedChannel::ManagedChannel(std::shared_ptr<ConnectionCore> core, std::string name, SetupFn setup,
                               LostFn lost)
    : core_(std::move(core)), name_(std::move(name)), setup_(std::move(setup)), lost_(std::move(lost)) {}

void ManagedChannel::open() {
    if (opened_.exchange(true)) return;
    std::weak_ptr<ManagedChannel> weak = shared_from_this();
    observerId_ = core_->addObserver(
        [weak](AMQP::Connection& conn) {
            if (auto self = weak.lock()) {
                if (!self->closed_ && !self->channel_) self->create(conn);
            }
        },
        [weak]() {
            if (auto self = weak.lock()) {
                self->drop(Status{ErrorCode::Io, "connection lost"});
            }
        });
}

void ManagedChannel::close() {
    if (closeStarted_.exchange(true)) return;

    auto self = shared_from_this();
    auto work = [self]() {
        if (self->closed_) return;
        self->closed_ = true;
        self->core_->removeObserver(self->observerId_);
        self->drop(Status{ErrorCode::Closed, "closed"});
    };

    if (core_->onIoThread()) {
        work();
        return;
    }

    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    const bool posted = core_->post([work, done](AMQP::Connection*) {
        work();
        done->set_value();
    });
    if (posted) {
        // Resolves either because the task ran or because the IO thread exited
        // and destroyed it (broken promise) - both mean "nothing left to run".
        future.wait_for(std::chrono::seconds(5));
    } else if (!core_->onIoThread()) {
        // IO thread already finished: nothing can touch the channel any more.
        self->closed_ = true;
        if (self->lost_) self->lost_(Status{ErrorCode::Closed, "closed"});
        self->channel_.reset();
    }
}

void ManagedChannel::create(AMQP::Connection& connection) {
    ++generation_;
    const std::uint64_t gen = generation_;
    std::weak_ptr<ManagedChannel> weak = shared_from_this();

    channel_ = std::make_unique<AMQP::Channel>(&connection);

    channel_->onError([weak, gen](const char* message) {
        auto self = weak.lock();
        if (!self) return;
        const std::string text = message ? message : "channel error";
        // Never destroy the channel from inside its own callback: defer.
        self->core_->post([weak, gen, text](AMQP::Connection*) {
            auto s = weak.lock();
            if (!s || s->generation_ != gen || !s->channel_) return;
            s->core_->reportError(Status{ErrorCode::Channel, s->name_ + ": " + text});
            s->drop(Status{ErrorCode::Channel, text});
            s->scheduleReopen();
        });
    });

    channel_->onReady([weak, gen]() {
        auto self = weak.lock();
        if (!self || self->generation_ != gen || !self->channel_) return;
        if (self->setup_) self->setup_(*self->channel_);
    });
}

void ManagedChannel::drop(const Status& status) {
    // A Closed notification is always delivered (even without a live channel) so the
    // owner can fail work that was only queued while disconnected.
    if (!channel_ && status.code != ErrorCode::Closed) return;
    ++generation_;  // invalidates callbacks of the dying channel
    if (lost_) lost_(status);
    channel_.reset();
}

void ManagedChannel::scheduleReopen() {
    if (closed_ || reopenPending_) return;
    reopenPending_ = true;
    std::weak_ptr<ManagedChannel> weak = shared_from_this();
    core_->postDelayed(kReopenDelay, [weak](AMQP::Connection* conn) {
        auto self = weak.lock();
        if (!self) return;
        self->reopenPending_ = false;
        // conn == nullptr: not connected; the observer re-creates it on the next connect.
        if (conn && !self->closed_ && !self->channel_) self->create(*conn);
    });
}

}  // namespace rmq::detail
