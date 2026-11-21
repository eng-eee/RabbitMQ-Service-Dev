#pragma once

#include <rmq/error.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace rmq {

namespace detail {
class ConnectionCore;  // implementation detail, wraps AMQP-CPP + the IO thread
}

enum class ConnectionState {
    Idle,          // constructed, start() not called yet
    Connecting,    // first connection attempt(s) in progress
    Connected,     // AMQP handshake finished, channels may be opened
    Reconnecting,  // connection was lost, retrying with back-off
    Closing,       // graceful shutdown in progress
    Closed,        // stopped; the object cannot be restarted
    Failed         // connection lost and auto_reconnect is disabled
};

const char* to_string(ConnectionState state) noexcept;

struct ConnectionOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 5672;
    std::string username = "guest";
    std::string password = "guest";
    std::string vhost = "/";

    /// Heartbeat interval in seconds. 0 = accept whatever the broker proposes.
    std::uint16_t heartbeat_seconds = 60;

    /// Upper bound for TCP connect and for the AMQP handshake (each).
    std::chrono::milliseconds connect_timeout{5000};

    /// Reconnect automatically with exponential back-off after a lost connection.
    bool auto_reconnect = true;
    std::chrono::milliseconds reconnect_min_delay{500};
    std::chrono::milliseconds reconnect_max_delay{30000};

    /// How long stop() waits for the broker to acknowledge the close handshake.
    std::chrono::milliseconds shutdown_timeout{2000};

    /// Throws rmq::Error(InvalidArgument) when a value is unusable.
    void validate() const;
};

/// Called from the IO thread on every state change. Must not block and must
/// not call Connection::stop()/waitConnected().
using StateCallback = std::function<void(ConnectionState state, const std::string& detail)>;

/// Called from the IO thread for every connection-level error.
using ErrorCallback = std::function<void(const Status& status)>;

/// A single broker connection. Owns one internal IO thread that runs the
/// socket event loop, the AMQP protocol state machine, heartbeats and
/// reconnection. All public methods are thread-safe.
///
/// Higher-level objects (Publisher, Subscriber, RpcClient, RpcServer) borrow
/// the connection and run their channel operations on its IO thread.
class Connection {
public:
    explicit Connection(ConnectionOptions options = {});
    ~Connection();  // stops the IO thread

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    /// Spawns the IO thread and begins connecting. Returns immediately.
    /// Throws rmq::Error if the connection was already stopped.
    void start();

    /// Graceful shutdown: closes the AMQP connection, joins the IO thread.
    /// Idempotent. Calling it from the IO thread (i.e. from a callback) only
    /// requests the shutdown and does not join.
    void stop();

    /// Blocks until the connection is Connected (true), or it reached a state
    /// from which it will not become Connected by itself, or the timeout expired (false).
    bool waitConnected(std::chrono::milliseconds timeout) const;

    ConnectionState state() const;
    bool isConnected() const { return state() == ConnectionState::Connected; }
    const ConnectionOptions& options() const noexcept;

    void setStateCallback(StateCallback callback);
    void setErrorCallback(ErrorCallback callback);

    /// Library-internal: used by Publisher/Subscriber/Rpc* implementations.
    std::shared_ptr<detail::ConnectionCore> core() const noexcept { return core_; }

private:
    std::shared_ptr<detail::ConnectionCore> core_;
};

}  // namespace rmq
