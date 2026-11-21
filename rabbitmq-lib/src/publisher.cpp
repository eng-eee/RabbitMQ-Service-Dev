#include <rmq/publisher.hpp>

#include "detail/amqp_util.hpp"
#include "detail/managed_channel.hpp"
#include "detail/util.hpp"

#include <atomic>
#include <deque>
#include <future>
#include <unordered_map>

namespace rmq {

namespace {

AMQP::ExchangeType exchangeType(const std::string& name) {
    if (name == "direct") return AMQP::direct;
    if (name == "fanout") return AMQP::fanout;
    if (name == "topic") return AMQP::topic;
    if (name == "headers") return AMQP::headers;
    throw Error(ErrorCode::InvalidArgument, "PublisherOptions: unknown exchange_type '" + name + "'");
}

}  // namespace

struct Publisher::Impl : std::enable_shared_from_this<Impl> {
    struct Item {
        std::string routingKey;
        Message message;
        Completion done;
    };

    std::shared_ptr<detail::ConnectionCore> core;
    PublisherOptions opts;
    AMQP::ExchangeType type = AMQP::direct;
    std::shared_ptr<detail::ManagedChannel> mc;

    std::atomic<std::size_t> outstanding{0};  // queued + in flight, any thread

    // ---- IO thread only ----
    std::deque<Item> pending;
    AMQP::Channel* chan = nullptr;
    std::unique_ptr<AMQP::Reliable<>> reliable;
    std::unordered_map<std::uint64_t, Completion> inflight;
    std::uint64_t nextSeq = 1;
    bool closed = false;

    void init() {
        std::weak_ptr<Impl> weak = weak_from_this();
        mc = std::make_shared<detail::ManagedChannel>(
            core, "publisher",
            [weak](AMQP::Channel& ch) {
                if (auto self = weak.lock()) self->onChannelReady(ch);
            },
            [weak](const Status& st) {
                if (auto self = weak.lock()) self->onChannelLost(st);
            });
        mc->open();
    }

    void complete(Completion& done, const Status& status) {
        outstanding.fetch_sub(1);
        if (done) detail::guarded([&] { done(status); });
    }

    // ---- any thread ----
    void enqueue(const std::string& routingKey, Message message, Completion done) {
        if (outstanding.fetch_add(1) >= opts.max_pending) {
            complete(done, Status{ErrorCode::Overflow, "publisher queue is full"});
            return;
        }
        auto self = shared_from_this();
        const bool posted = core->post(
            [self, item = Item{routingKey, std::move(message), done}](AMQP::Connection*) mutable {
                if (self->closed) {
                    self->complete(item.done, Status{ErrorCode::Closed, "publisher closed"});
                    return;
                }
                self->pending.push_back(std::move(item));
                self->pump();
            });
        if (!posted) complete(done, Status{ErrorCode::Closed, "connection stopped"});
    }

    // ---- IO thread ----
    void onChannelReady(AMQP::Channel& ch) {
        chan = &ch;
        if (opts.confirms) reliable = std::make_unique<AMQP::Reliable<>>(ch);
        if (opts.declare_exchange) {
            ch.declareExchange(opts.exchange, type, opts.durable ? AMQP::durable : 0);
        }
        pump();
    }

    void onChannelLost(const Status& st) {
        auto flying = std::move(inflight);
        inflight.clear();
        reliable.reset();
        chan = nullptr;

        // Closed / Io (connection lost) / Channel (broker error text, e.g. NOT_FOUND ...).
        const Status failure{st.code, st.message};
        for (auto& kv : flying) complete(kv.second, failure);

        if (st.code == ErrorCode::Closed) {
            closed = true;
            auto queued = std::move(pending);
            pending.clear();
            for (auto& item : queued) complete(item.done, Status{ErrorCode::Closed, "publisher closed"});
        }
    }

    void pump() {
        while (chan && chan->usable() && !pending.empty()) {
            Item item = std::move(pending.front());
            pending.pop_front();
            send(item);
        }
    }

    void send(Item& item) {
        AMQP::Envelope envelope(item.message.body.data(), item.message.body.size());
        detail::fillMetadata(item.message, envelope);

        if (!reliable) {
            const bool ok = chan->publish(opts.exchange, item.routingKey, envelope);
            complete(item.done, ok ? Status::success() : Status{ErrorCode::Channel, "publish failed"});
            return;
        }

        const std::uint64_t seq = nextSeq++;
        inflight.emplace(seq, std::move(item.done));
        std::weak_ptr<Impl> weak = weak_from_this();
        auto finish = [weak, seq](ErrorCode code, std::string text) {
            if (auto self = weak.lock()) self->finish(seq, Status{code, std::move(text)});
        };

        reliable->publish(opts.exchange, item.routingKey, envelope)
            .onAck([finish] { finish(ErrorCode::Ok, ""); })
            .onNack([finish] { finish(ErrorCode::Channel, "message was nacked by the broker"); })
            .onError([finish](const char* message) { finish(ErrorCode::Channel, message ? message : "publish error"); });
    }

    void finish(std::uint64_t seq, const Status& status) {
        auto it = inflight.find(seq);
        if (it == inflight.end()) return;
        Completion done = std::move(it->second);
        inflight.erase(it);
        complete(done, status);
    }
};

Publisher::Publisher(Connection& connection, PublisherOptions options) {
    if (options.declare_exchange && options.exchange.empty()) {
        throw Error(ErrorCode::InvalidArgument, "PublisherOptions: cannot declare the default exchange");
    }
    if (options.max_pending == 0) {
        throw Error(ErrorCode::InvalidArgument, "PublisherOptions: max_pending must be > 0");
    }
    impl_ = std::make_shared<Impl>();
    impl_->type = exchangeType(options.exchange_type);
    impl_->core = connection.core();
    impl_->opts = std::move(options);
    impl_->init();
}

Publisher::~Publisher() {
    if (impl_ && impl_->mc) impl_->mc->close();
}

void Publisher::publishAsync(const std::string& routing_key, Message message, Completion done) {
    impl_->enqueue(routing_key, std::move(message), std::move(done));
}

Status Publisher::publish(const std::string& routing_key, Message message,
                          std::chrono::milliseconds timeout) {
    if (impl_->core->onIoThread()) {
        return Status{ErrorCode::Internal, "blocking publish() called from the IO thread"};
    }
    auto promise = std::make_shared<std::promise<Status>>();
    auto future = promise->get_future();
    publishAsync(routing_key, std::move(message), [promise](const Status& s) { promise->set_value(s); });

    if (future.wait_for(timeout) != std::future_status::ready) {
        return Status{ErrorCode::Timeout, "no confirmation within the timeout"};
    }
    try {
        return future.get();
    } catch (const std::future_error&) {  // IO thread went away with the task still queued
        return Status{ErrorCode::Closed, "connection stopped"};
    }
}

}  // namespace rmq
