#pragma once

#include <rmq/connection.hpp>
#include <rmq/message.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace rmq {

struct RpcServerOptions {
    std::string queue;              // request queue (required)
    bool durable = true;
    std::uint16_t prefetch = 1;
};

/// Request/reply server. For every request the handler's return value is sent
/// to `reply_to` with the request's correlation id. If the handler throws, an
/// error reply with header "rmq-error" is sent and the request is acked.
class RpcServer {
public:
    /// Runs on the IO thread: keep it short and never block in it.
    using Handler = std::function<Message(const Delivery& request)>;

    RpcServer(Connection& connection, RpcServerOptions options);
    ~RpcServer();

    RpcServer(const RpcServer&) = delete;
    RpcServer& operator=(const RpcServer&) = delete;

    void start(Handler handler);
    void stop();

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace rmq
