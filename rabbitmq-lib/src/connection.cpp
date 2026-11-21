#include <rmq/connection.hpp>

#include "detail/connection_core.hpp"

#include <utility>

namespace rmq {

const char* to_string(ConnectionState state) noexcept {
    switch (state) {
        case ConnectionState::Idle:         return "Idle";
        case ConnectionState::Connecting:   return "Connecting";
        case ConnectionState::Connected:    return "Connected";
        case ConnectionState::Reconnecting: return "Reconnecting";
        case ConnectionState::Closing:      return "Closing";
        case ConnectionState::Closed:       return "Closed";
        case ConnectionState::Failed:       return "Failed";
    }
    return "Unknown";
}

void ConnectionOptions::validate() const {
    using std::chrono::milliseconds;
    auto bad = [](const char* what) { throw Error(ErrorCode::InvalidArgument, what); };

    if (host.empty())                          bad("ConnectionOptions: host must not be empty");
    if (port == 0)                             bad("ConnectionOptions: port must not be 0");
    if (vhost.empty())                         bad("ConnectionOptions: vhost must not be empty");
    if (connect_timeout <= milliseconds(0))    bad("ConnectionOptions: connect_timeout must be > 0");
    if (reconnect_min_delay <= milliseconds(0))
        bad("ConnectionOptions: reconnect_min_delay must be > 0");
    if (reconnect_max_delay < reconnect_min_delay)
        bad("ConnectionOptions: reconnect_max_delay must be >= reconnect_min_delay");
    if (shutdown_timeout < milliseconds(0))    bad("ConnectionOptions: shutdown_timeout must be >= 0");
}

Connection::Connection(ConnectionOptions options) {
    options.validate();
    core_ = std::make_shared<detail::ConnectionCore>(std::move(options));
}

Connection::~Connection() {
    if (core_) core_->stop();
}

void Connection::start() { core_->start(); }
void Connection::stop() { core_->stop(); }

bool Connection::waitConnected(std::chrono::milliseconds timeout) const {
    return core_->waitConnected(timeout);
}

ConnectionState Connection::state() const { return core_->state(); }
const ConnectionOptions& Connection::options() const noexcept { return core_->options(); }

void Connection::setStateCallback(StateCallback callback) {
    core_->setStateCallback(std::move(callback));
}

void Connection::setErrorCallback(ErrorCallback callback) {
    core_->setErrorCallback(std::move(callback));
}

}  // namespace rmq
