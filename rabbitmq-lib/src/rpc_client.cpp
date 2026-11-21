#include <rmq/rpc_client.hpp>

#include "detail/amqp_util.hpp"
#include "detail/managed_channel.hpp"
#include "detail/util.hpp"

#include <algorithm>
#include <deque>
#include <random>
#include <sstream>
#include <unordered_map>

namespace rmq {

struct RpcClient::Impl : std::enable_shared_from_this<Impl> {
    struct Call {
        Callback callback;
        std::uint64_t timerId = 0;
        bool sent = false;
    };
    struct Waiting {
        std::string correlationId;
        Message request;
    };

    std::shared_ptr<detail::ConnectionCore> core;
    RpcClientOptions opts;
    std::shared_ptr<detail::ManagedChannel> mc;
    std::string idPrefix;

    // ---- IO thread only ----
    std::unordered_map<std::string, Call> calls;
    std::deque<Waiting> waiting;  // accepted, but no reply queue yet
    std::string replyQueue;
    std::uint64_t nextId = 1;
    bool closed = false;

    void init() {
        std::random_device rd;
        std::ostringstream os;
        os << std::hex << rd() << rd();
        idPrefix = os.str();

        std::weak_ptr<Impl> weak = weak_from_this();
        mc = std::make_shared<detail::ManagedChannel>(
            core, "rpc-client",
            [weak](AMQP::Channel& ch) {
                if (auto self = weak.lock()) self->onChannelReady(ch);
            },
            [weak](const Status& st) {
                if (auto self = weak.lock()) self->onChannelLost(st);
            });
        mc->open();
    }

    // ---- any thread ----
    void callAsync(Message request, Callback callback, std::chrono::milliseconds timeout) {
        if (timeout.count() <= 0) timeout = opts.default_timeout;
        auto self = shared_from_this();
        const bool posted = core->post(
            [self, request = std::move(request), callback, timeout](AMQP::Connection*) mutable {
                self->submit(std::move(request), std::move(callback), timeout);
            });
        if (!posted) {
            detail::guarded([&] { callback(Status{ErrorCode::Closed, "connection stopped"}, Message{}); });
        }
    }

    // ---- IO thread ----
    void submit(Message request, Callback callback, std::chrono::milliseconds timeout) {
        if (closed) {
            detail::guarded([&] { callback(Status{ErrorCode::Closed, "rpc client closed"}, Message{}); });
            return;
        }

        const std::string id = idPrefix + "-" + std::to_string(nextId++);
        std::weak_ptr<Impl> weak = weak_from_this();

        Call call;
        call.callback = std::move(callback);
        call.timerId = core->postDelayed(timeout, [weak, id](AMQP::Connection*) {
            if (auto self = weak.lock()) {
                self->finish(id, Status{ErrorCode::Timeout, "no reply within the timeout"}, Message{},
                             /*cancelTimer=*/false);
            }
        });
        calls.emplace(id, std::move(call));

        request.correlation_id = id;
        AMQP::Channel* ch = mc->channel();
        if (ch && ch->usable() && !replyQueue.empty()) {
            send(id, request);
        } else {
            waiting.push_back(Waiting{id, std::move(request)});
        }
    }

    void send(const std::string& id, Message& request) {
        AMQP::Channel* ch = mc->channel();
        request.reply_to = replyQueue;

        AMQP::Envelope envelope(request.body.data(), request.body.size());
        detail::fillMetadata(request, envelope);

        auto it = calls.find(id);
        if (it != calls.end()) it->second.sent = true;
        if (!ch->publish(opts.exchange, opts.routing_key, envelope)) {
            finish(id, Status{ErrorCode::Channel, "publish failed"}, Message{}, true);
        }
    }

    void onChannelReady(AMQP::Channel& ch) {
        std::weak_ptr<Impl> weak = weak_from_this();
        ch.declareQueue(AMQP::exclusive | AMQP::autodelete)
            .onSuccess([weak](const std::string& name, std::uint32_t, std::uint32_t) {
                auto self = weak.lock();
                if (!self) return;
                AMQP::Channel* c = self->mc->channel();
                if (!c) return;

                self->replyQueue = name;
                c->consume(name, AMQP::noack).onReceived([weak](const AMQP::Message& m, std::uint64_t, bool) {
                    if (auto s = weak.lock()) s->onReply(m);
                });
                self->flushWaiting();
            });
    }

    void flushWaiting() {
        AMQP::Channel* ch = mc->channel();
        while (ch && ch->usable() && !replyQueue.empty() && !waiting.empty()) {
            Waiting w = std::move(waiting.front());
            waiting.pop_front();
            if (calls.count(w.correlationId)) send(w.correlationId, w.request);  // may have timed out meanwhile
        }
    }

    void onReply(const AMQP::Message& message) {
        if (!message.hasCorrelationID()) return;
        Delivery d = detail::makeDelivery(message, 0, false);
        const std::string id = d.message.correlation_id;  // copy: d.message is moved from below
        auto err = d.message.headers.find("rmq-error");
        if (err != d.message.headers.end()) {
            finish(id, Status{ErrorCode::Remote, err->second}, Message{}, true);
        } else {
            finish(id, Status::success(), std::move(d.message), true);
        }
    }

    void onChannelLost(const Status& st) {
        replyQueue.clear();

        const bool closing = st.code == ErrorCode::Closed;
        std::vector<std::string> doomed;
        for (const auto& kv : calls) {
            if (kv.second.sent || closing) doomed.push_back(kv.first);
        }
        if (closing) {
            closed = true;
            waiting.clear();
        }
        const Status failure{closing ? ErrorCode::Closed : ErrorCode::Io,
                             "channel lost before the reply arrived: " + st.message};
        for (const auto& id : doomed) finish(id, failure, Message{}, true);
    }

    void finish(const std::string& id, const Status& status, Message reply, bool cancelTimer) {
        auto it = calls.find(id);
        if (it == calls.end()) return;
        Call call = std::move(it->second);
        calls.erase(it);

        if (cancelTimer) core->cancelTimer(call.timerId);
        waiting.erase(std::remove_if(waiting.begin(), waiting.end(),
                                     [&](const Waiting& w) { return w.correlationId == id; }),
                      waiting.end());

        detail::guarded([&] { call.callback(status, std::move(reply)); });
    }
};

RpcClient::RpcClient(Connection& connection, RpcClientOptions options) {
    if (options.exchange.empty() && options.routing_key.empty()) {
        throw Error(ErrorCode::InvalidArgument,
                    "RpcClientOptions: set routing_key (the server's queue) and/or exchange");
    }
    impl_ = std::make_shared<Impl>();
    impl_->core = connection.core();
    impl_->opts = std::move(options);
    impl_->init();
}

RpcClient::~RpcClient() {
    if (impl_ && impl_->mc) impl_->mc->close();
}

void RpcClient::callAsync(Message request, Callback callback, std::chrono::milliseconds timeout) {
    if (!callback) throw Error(ErrorCode::InvalidArgument, "RpcClient::callAsync: empty callback");
    impl_->callAsync(std::move(request), std::move(callback), timeout);
}

std::future<Message> RpcClient::call(Message request, std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<Message>>();
    auto future = promise->get_future();
    impl_->callAsync(
        std::move(request),
        [promise](const Status& status, Message reply) {
            if (status.ok()) {
                promise->set_value(std::move(reply));
            } else {
                promise->set_exception(std::make_exception_ptr(Error(status.code, status.message)));
            }
        },
        timeout);
    return future;
}

}  // namespace rmq
