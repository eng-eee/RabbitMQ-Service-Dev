#pragma once

#include <stdexcept>
#include <string>

namespace rmq {

enum class ErrorCode {
    Ok = 0,
    InvalidArgument,
    NotConnected,
    ConnectFailed,  // TCP connect / name resolution failed
    Io,             // socket read/write failure, peer closed the connection
    Protocol,       // AMQP-level error reported by the broker or the codec
    Timeout,
    Closed,         // object was stopped / shut down
    Channel,        // channel-level error (queue/exchange declare, publish...)
    Overflow,       // internal queue is full (back-pressure)
    Remote,         // the peer (RPC server) reported an error
    Internal
};

const char* to_string(ErrorCode code) noexcept;

/// Lightweight result type used by callbacks and non-throwing APIs.
struct Status {
    ErrorCode code = ErrorCode::Ok;
    std::string message;

    bool ok() const noexcept { return code == ErrorCode::Ok; }
    explicit operator bool() const noexcept { return ok(); }
    static Status success() { return {}; }
};

/// Thrown by APIs that report failures through exceptions.
class Error : public std::runtime_error {
public:
    Error(ErrorCode code, const std::string& what) : std::runtime_error(what), code_(code) {}
    ErrorCode code() const noexcept { return code_; }

private:
    ErrorCode code_;
};

}  // namespace rmq
