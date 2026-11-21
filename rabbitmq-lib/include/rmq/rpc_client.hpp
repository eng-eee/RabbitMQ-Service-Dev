#pragma once

#include <rmq/connection.hpp>
#include <rmq/message.hpp>

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>

namespace rmq {

struct RpcClientOptions {
    std::string exchange;     // "" = default exchange
    std::string routing_key;  // target queue name when using the default exchange
    std::chrono::milliseconds default_timeout{5000};
};

/// Request/reply client. Uses an exclusive auto-delete reply queue and
/// correlation ids; pending calls fail with ErrorCode::Closed/Timeout.
class RpcClient {
public:
    using Callback = std::function<void(const Status&, Message reply)>;

    RpcClient(Connection& connection, RpcClientOptions options);
    ~RpcClient();

    RpcClient(const RpcClient&) = delete;
    RpcClient& operator=(const RpcClient&) = delete;

    /// Non-blocking. The future throws rmq::Error on timeout / failure.
    /// timeout == 0 selects options.default_timeout.
    std::future<Message> call(Message request, std::chrono::milliseconds timeout = {});

    /// Non-blocking, callback runs on the IO thread.
    void callAsync(Message request, Callback callback, std::chrono::milliseconds timeout = {});

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace rmq
