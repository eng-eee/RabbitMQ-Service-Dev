#include <rmq/rpc_server.hpp>

#include "detail/amqp_util.hpp"
#include "detail/consumer.hpp"

#include <mutex>

namespace rmq {

struct RpcServer::Impl {
    std::shared_ptr<detail::ConnectionCore> core;
    RpcServerOptions opts;

    std::mutex mutex;
    std::shared_ptr<detail::Consumer> consumer;
};

RpcServer::RpcServer(Connection& connection, RpcServerOptions options) {
    if (options.queue.empty()) {
        throw Error(ErrorCode::InvalidArgument, "RpcServerOptions: queue must not be empty");
    }
    impl_ = std::make_shared<Impl>();
    impl_->core = connection.core();
    impl_->opts = std::move(options);
}

RpcServer::~RpcServer() { stop(); }

void RpcServer::start(Handler handler) {
    if (!handler) throw Error(ErrorCode::InvalidArgument, "RpcServer::start: empty handler");

    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->consumer) throw Error(ErrorCode::InvalidArgument, "RpcServer already started");

    detail::ConsumerConfig cfg;
    cfg.name = "rpc-server";
    cfg.queue = impl_->opts.queue;
    cfg.durable = impl_->opts.durable;
    cfg.prefetch = impl_->opts.prefetch;
    cfg.auto_ack = false;

    auto fn = [handler = std::move(handler)](AMQP::Channel& channel, const Delivery& request) {
        Message reply;
        try {
            reply = handler(request);
        } catch (const std::exception& e) {
            reply = Message{};
            reply.headers["rmq-error"] = e.what();
        } catch (...) {
            reply = Message{};
            reply.headers["rmq-error"] = "unknown error";
        }

        if (!request.message.reply_to.empty()) {
            reply.correlation_id = request.message.correlation_id;
            reply.reply_to.clear();
            AMQP::Envelope envelope(reply.body.data(), reply.body.size());
            detail::fillMetadata(reply, envelope);
            channel.publish("", request.message.reply_to, envelope);
        }
        return AckAction::Ack;  // the request is consumed either way
    };

    auto consumer = std::make_shared<detail::Consumer>(impl_->core, std::move(cfg), std::move(fn));
    consumer->start();
    impl_->consumer = std::move(consumer);
}

void RpcServer::stop() {
    std::shared_ptr<detail::Consumer> consumer;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        consumer = std::move(impl_->consumer);
    }
    if (consumer) consumer->stop();
}

}  // namespace rmq
