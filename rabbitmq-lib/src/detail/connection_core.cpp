#include "connection_core.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <system_error>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

namespace rmq::detail {

namespace {

std::string errnoMessage(int err) { return std::system_category().message(err); }

void setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/// User callbacks must never take the IO thread down.
template <typename F>
void guarded(F&& f) noexcept {
    try {
        f();
    } catch (...) {
    }
}

}  // namespace

// ===========================================================================
// Construction / lifecycle (caller threads)
// ===========================================================================

ConnectionCore::ConnectionCore(ConnectionOptions options)
    : opts_(std::move(options)), backoff_(opts_.reconnect_min_delay) {
    if (::pipe2(wake_, O_NONBLOCK | O_CLOEXEC) != 0) {
        throw Error(ErrorCode::Internal, "pipe2 failed: " + errnoMessage(errno));
    }
}

ConnectionCore::~ConnectionCore() {
    stop();
    for (int& fd : wake_) {
        if (fd >= 0) ::close(fd);
        fd = -1;
    }
}

void ConnectionCore::start() {
    std::lock_guard<std::mutex> lock(threadMutex_);
    if (stopRequested_.load()) throw Error(ErrorCode::Closed, "connection was already stopped");
    if (started_) return;
    started_ = true;
    try {
        thread_ = std::thread([this] { run(); });
    } catch (...) {
        started_ = false;
        throw;
    }
}

void ConnectionCore::stop() {
    std::thread toJoin;
    bool neverStarted = false;
    {
        std::lock_guard<std::mutex> lock(threadMutex_);
        stopRequested_.store(true);
        if (!started_) {
            neverStarted = true;
        } else if (thread_.get_id() == std::this_thread::get_id()) {
            // Called from a callback on the IO thread: request only, never self-join.
        } else {
            toJoin = std::move(thread_);
        }
    }
    wake();
    if (neverStarted) {
        finished_.store(true);
        setState(ConnectionState::Closed, "stopped before start");
        return;
    }
    if (toJoin.joinable()) toJoin.join();
}

ConnectionState ConnectionCore::state() const {
    std::lock_guard<std::mutex> lock(stateMutex_);
    return state_;
}

bool ConnectionCore::waitConnected(std::chrono::milliseconds timeout) const {
    std::unique_lock<std::mutex> lock(stateMutex_);
    stateCv_.wait_for(lock, timeout, [this] {
        return state_ == ConnectionState::Connected || state_ == ConnectionState::Closing ||
               state_ == ConnectionState::Closed || state_ == ConnectionState::Failed;
    });
    return state_ == ConnectionState::Connected;
}

void ConnectionCore::setStateCallback(StateCallback callback) {
    std::lock_guard<std::mutex> lock(callbackMutex_);
    stateCb_ = std::move(callback);
}

void ConnectionCore::setErrorCallback(ErrorCallback callback) {
    std::lock_guard<std::mutex> lock(callbackMutex_);
    errorCb_ = std::move(callback);
}

bool ConnectionCore::post(Task task) {
    if (finished_.load()) return false;
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        tasks_.push_back(std::move(task));
    }
    wake();
    return true;
}

std::uint64_t ConnectionCore::addObserver(ReadyFn ready, LostFn lost) {
    const std::uint64_t id = nextObserverId_.fetch_add(1);
    post([this, id, ready = std::move(ready), lost = std::move(lost)](AMQP::Connection* conn) {
        observers_.push_back(Observer{id, ready, lost});
        if (conn && ready) guarded([&] { ready(*conn); });
    });
    return id;
}

void ConnectionCore::removeObserver(std::uint64_t id) {
    post([this, id](AMQP::Connection*) {
        observers_.erase(std::remove_if(observers_.begin(), observers_.end(),
                                        [id](const Observer& o) { return o.id == id; }),
                         observers_.end());
    });
}

std::uint64_t ConnectionCore::postDelayed(std::chrono::milliseconds delay, Task task) {
    const std::uint64_t id = nextTimerId_.fetch_add(1);
    const TimePoint when = Clock::now() + delay;
    post([this, id, when, task = std::move(task)](AMQP::Connection*) mutable {
        timers_.emplace(when, Timer{id, std::move(task)});
    });
    return id;
}

void ConnectionCore::cancelTimer(std::uint64_t id) {
    post([this, id](AMQP::Connection*) {
        for (auto it = timers_.begin(); it != timers_.end(); ++it) {
            if (it->second.id == id) {
                timers_.erase(it);
                return;
            }
        }
    });
}

void ConnectionCore::wake() noexcept {
    if (wake_[1] < 0) return;
    const char byte = 1;
    ssize_t rc;
    do {
        rc = ::write(wake_[1], &byte, 1);
    } while (rc < 0 && errno == EINTR);
    (void)rc;  // EAGAIN means the pipe is already full of wake-ups: fine
}

void ConnectionCore::drainWake() noexcept {
    char buf[64];
    while (::read(wake_[0], buf, sizeof buf) > 0) {
    }
}

// ===========================================================================
// State / error reporting
// ===========================================================================

void ConnectionCore::setState(ConnectionState state, const std::string& detail) {
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        if (state_ == state) return;
        state_ = state;
    }
    stateCv_.notify_all();

    StateCallback cb;
    {
        std::lock_guard<std::mutex> lock(callbackMutex_);
        cb = stateCb_;
    }
    if (cb) guarded([&] { cb(state, detail); });
}

void ConnectionCore::reportError(const Status& status) {
    ErrorCallback cb;
    {
        std::lock_guard<std::mutex> lock(callbackMutex_);
        cb = errorCb_;
    }
    if (cb) guarded([&] { cb(status); });
}

void ConnectionCore::fail(ErrorCode code, std::string message) {
    if (!failure_) failure_ = Status{code, std::move(message)};
}

std::string ConnectionCore::endpoint() const { return opts_.host + ":" + std::to_string(opts_.port); }

std::chrono::milliseconds ConnectionCore::heartbeatHalf() const {
    return std::chrono::milliseconds(static_cast<int>(heartbeat_) * 500);
}

// ===========================================================================
// AMQP::ConnectionHandler (IO thread)
// ===========================================================================

void ConnectionCore::onData(AMQP::Connection*, const char* buffer, std::size_t size) {
    if (tearingDown_) return;
    outbuf_.insert(outbuf_.end(), buffer, buffer + size);  // flushed by the loop before poll()
}

std::uint16_t ConnectionCore::onNegotiate(AMQP::Connection*, std::uint16_t interval) {
    const std::uint32_t mine = opts_.heartbeat_seconds;
    std::uint32_t chosen = 0;
    if (mine == 0) chosen = interval;
    else if (interval == 0) chosen = mine;
    else chosen = std::min<std::uint32_t>(mine, interval);

    heartbeat_ = static_cast<std::uint16_t>(chosen);
    const auto now = Clock::now();
    lastRx_ = now;
    nextHeartbeat_ = now + heartbeatHalf();
    return heartbeat_;
}

void ConnectionCore::onReady(AMQP::Connection* connection) {
    if (tearingDown_) return;
    ready_ = true;
    hasConnected_ = true;
    backoff_ = opts_.reconnect_min_delay;
    setState(ConnectionState::Connected, "connected to " + endpoint());

    for (auto& observer : observers_) {
        if (observer.ready) guarded([&] { observer.ready(*connection); });
    }
}

void ConnectionCore::onError(AMQP::Connection*, const char* message) {
    if (tearingDown_) return;
    fail(ErrorCode::Protocol, message ? message : "unknown AMQP error");
}

void ConnectionCore::onClosed(AMQP::Connection*) {
    if (tearingDown_) return;
    closed_ = true;
}

// ===========================================================================
// IO thread main loop
// ===========================================================================

void ConnectionCore::run() {
    ioThreadId_.store(std::this_thread::get_id());
#ifdef __linux__
    ::pthread_setname_np(::pthread_self(), "rmq-io");
#endif

    reconnectAt_ = Clock::now();
    setState(ConnectionState::Connecting, "connecting to " + endpoint());

    for (;;) {
        drainTasks();

        const bool stopping = stopRequested_.load();
        if (!stopping) runTimers(Clock::now());
        if (stopping) {
            if (!shutdownStep(Clock::now())) break;
        } else if (!amqp_) {
            if (!exhausted_ && Clock::now() >= reconnectAt_) tryConnect();
        } else {
            serviceTimers(Clock::now());
        }
        if (resolveFailure()) break;

        flushOutput();
        if (resolveFailure()) break;

        pollfd fds[2] = {};
        nfds_t nfds = 0;
        fds[nfds].fd = wake_[0];
        fds[nfds].events = POLLIN;
        ++nfds;
        if (sock_ >= 0) {
            fds[nfds].fd = sock_;
            fds[nfds].events = POLLIN;
            if (outOff_ < outbuf_.size()) fds[nfds].events |= POLLOUT;
            ++nfds;
        }

        const int rc = ::poll(fds, nfds, pollTimeoutMs(Clock::now()));
        if (rc < 0 && errno != EINTR) {
            fail(ErrorCode::Internal, "poll failed: " + errnoMessage(errno));
        } else if (rc > 0) {
            if (fds[0].revents) drainWake();
            // POLLOUT needs no handling here: flushOutput() runs at the top of every iteration.
            if (nfds > 1 && (fds[1].revents & (POLLIN | POLLERR | POLLHUP))) readSocket();
        }
        if (resolveFailure()) break;
    }

    if (failure_) reportError(*failure_);
    const bool wasReady = teardownConnection();
    if (wasReady) notifyLost();
    observers_.clear();
    timers_.clear();

    finished_.store(true);
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        tasks_.clear();
    }
    setState(ConnectionState::Closed, "stopped");
}

void ConnectionCore::runTimers(TimePoint now) {
    while (!timers_.empty() && timers_.begin()->first <= now) {
        Task fn = std::move(timers_.begin()->second.fn);
        timers_.erase(timers_.begin());
        AMQP::Connection* conn = ready_ ? amqp_.get() : nullptr;
        try {
            fn(conn);
        } catch (const std::exception& e) {
            reportError(Status{ErrorCode::Internal, std::string("timer task threw: ") + e.what()});
        } catch (...) {
            reportError(Status{ErrorCode::Internal, "timer task threw an unknown exception"});
        }
    }
}

void ConnectionCore::drainTasks() {
    std::deque<Task> local;
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        local.swap(tasks_);
    }
    for (auto& task : local) {
        AMQP::Connection* conn = ready_ ? amqp_.get() : nullptr;
        try {
            task(conn);
        } catch (const std::exception& e) {
            reportError(Status{ErrorCode::Internal, std::string("task threw: ") + e.what()});
        } catch (...) {
            reportError(Status{ErrorCode::Internal, "task threw an unknown exception"});
        }
    }
}

void ConnectionCore::tryConnect() {
    setState(hasConnected_ ? ConnectionState::Reconnecting : ConnectionState::Connecting,
             "connecting to " + endpoint());

    std::string error;
    const int fd = openSocket(error);
    if (fd < 0) {
        if (!stopRequested_.load()) {
            fail(ErrorCode::ConnectFailed, "connect to " + endpoint() + " failed: " + error);
        }
        return;
    }

    sock_ = fd;
    inbuf_.clear();
    outbuf_.clear();
    outOff_ = 0;
    ready_ = closing_ = closed_ = false;
    heartbeat_ = 0;

    const auto now = Clock::now();
    lastRx_ = now;
    handshakeDeadline_ = now + opts_.connect_timeout;

    try {
        // The constructor already emits the protocol header through onData().
        amqp_ = std::make_unique<AMQP::Connection>(
            this, AMQP::Login(opts_.username, opts_.password), opts_.vhost);
    } catch (const std::exception& e) {
        fail(ErrorCode::Internal, std::string("failed to create AMQP connection: ") + e.what());
    }
}

/// Non-blocking connect with a deadline; stays responsive to stop() through the wake pipe.
int ConnectionCore::openSocket(std::string& error) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    const std::string port = std::to_string(opts_.port);
    const int gai = ::getaddrinfo(opts_.host.c_str(), port.c_str(), &hints, &result);
    if (gai != 0) {
        error = std::string("cannot resolve host: ") + ::gai_strerror(gai);
        return -1;
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(result, &::freeaddrinfo);

    const auto deadline = Clock::now() + opts_.connect_timeout;

    for (const addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
        const int fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0) {
            error = "socket(): " + errnoMessage(errno);
            continue;
        }
        setNonBlocking(fd);

        bool connected = (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0);
        if (!connected && errno != EINPROGRESS) {
            error = errnoMessage(errno);
            ::close(fd);
            continue;
        }

        while (!connected) {
            if (stopRequested_.load()) {
                ::close(fd);
                error = "stopped";
                return -1;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
            if (remaining <= 0) {
                error = "timed out";
                break;
            }

            pollfd fds[2] = {};
            fds[0].fd = fd;
            fds[0].events = POLLOUT;
            fds[1].fd = wake_[0];
            fds[1].events = POLLIN;
            const int rc = ::poll(fds, 2, static_cast<int>(std::min<long long>(remaining, 500)));
            if (rc < 0) {
                if (errno == EINTR) continue;
                error = "poll(): " + errnoMessage(errno);
                break;
            }
            if (fds[1].revents) drainWake();
            if (fds[0].revents) {
                int soError = 0;
                socklen_t len = sizeof soError;
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &len);
                if (soError == 0) {
                    connected = true;
                } else {
                    error = errnoMessage(soError);
                    break;
                }
            }
        }

        if (!connected) {
            ::close(fd);
            continue;
        }

        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
        return fd;
    }

    if (error.empty()) error = "no usable address";
    return -1;
}

void ConnectionCore::serviceTimers(TimePoint now) {
    if (!ready_ && now >= handshakeDeadline_) {
        fail(ErrorCode::Timeout, "AMQP handshake timed out");
        return;
    }
    if (heartbeat_ == 0) return;

    if (now - lastRx_ > 2 * std::chrono::seconds(heartbeat_)) {
        fail(ErrorCode::Timeout, "heartbeat timeout: no data from the broker");
        return;
    }
    if (now >= nextHeartbeat_) {
        amqp_->heartbeat();
        nextHeartbeat_ = now + heartbeatHalf();
    }
}

bool ConnectionCore::shutdownStep(TimePoint now) {
    if (!amqp_) return false;

    if (!closing_) {
        closing_ = true;
        setState(ConnectionState::Closing, "stop requested");
        if (!ready_) return false;  // handshake not finished: just drop the socket
        closeDeadline_ = now + opts_.shutdown_timeout;
        try {
            amqp_->close();
        } catch (...) {
            return false;
        }
        return true;
    }
    return !(closed_ || now >= closeDeadline_);
}

void ConnectionCore::flushOutput() {
    if (sock_ < 0) return;

    while (outOff_ < outbuf_.size()) {
        const ssize_t n = ::send(sock_, outbuf_.data() + outOff_, outbuf_.size() - outOff_, MSG_NOSIGNAL);
        if (n > 0) {
            outOff_ += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        fail(ErrorCode::Io, "send failed: " + errnoMessage(errno));
        return;
    }

    if (outOff_ == outbuf_.size()) {
        outbuf_.clear();
        outOff_ = 0;
    } else if (outOff_ > (1u << 16)) {
        outbuf_.erase(outbuf_.begin(), outbuf_.begin() + static_cast<std::ptrdiff_t>(outOff_));
        outOff_ = 0;
    }
}

void ConnectionCore::readSocket() {
    char buf[16 * 1024];
    bool eof = false;

    for (;;) {
        const ssize_t n = ::recv(sock_, buf, sizeof buf, 0);
        if (n > 0) {
            inbuf_.insert(inbuf_.end(), buf, buf + n);
            lastRx_ = Clock::now();
            if (static_cast<std::size_t>(n) < sizeof buf) break;
            continue;
        }
        if (n == 0) {
            eof = true;
            break;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        fail(ErrorCode::Io, "recv failed: " + errnoMessage(errno));
        break;
    }

    // Parse what we got first: the broker's last words (e.g. connection.close
    // with a reason) usually arrive right before the FIN.
    if (!inbuf_.empty()) processInput();
    if (eof && !failure_ && !closed_) fail(ErrorCode::Io, "connection closed by the broker");
}

void ConnectionCore::processInput() {
    std::size_t consumed = 0;
    try {
        while (amqp_ && !failure_ && consumed < inbuf_.size()) {
            const std::size_t used = amqp_->parse(inbuf_.data() + consumed, inbuf_.size() - consumed);
            if (used == 0) break;  // incomplete frame: wait for more bytes
            consumed += used;
        }
    } catch (const std::exception& e) {
        fail(ErrorCode::Protocol, std::string("AMQP parse error: ") + e.what());
    }
    inbuf_.erase(inbuf_.begin(), inbuf_.begin() + static_cast<std::ptrdiff_t>(consumed));
}

bool ConnectionCore::resolveFailure() {
    if (!failure_) return false;
    if (stopRequested_.load()) return true;
    handleFailure();
    return false;
}

void ConnectionCore::handleFailure() {
    const Status status = *failure_;
    failure_.reset();

    const bool wasReady = teardownConnection();
    if (wasReady) notifyLost();
    reportError(status);

    if (!opts_.auto_reconnect) {
        exhausted_ = true;
        setState(ConnectionState::Failed, status.message);
        return;
    }

    reconnectAt_ = Clock::now() + backoff_;
    backoff_ = std::min(backoff_ * 2, opts_.reconnect_max_delay);
    setState(hasConnected_ ? ConnectionState::Reconnecting : ConnectionState::Connecting,
             status.message);
}

bool ConnectionCore::teardownConnection() {
    const bool wasReady = ready_;

    // AMQP-CPP must be destroyed outside of any of its own callbacks - which
    // is guaranteed here because failures are only recorded by the handlers.
    tearingDown_ = true;
    amqp_.reset();
    tearingDown_ = false;

    closeSocket();
    inbuf_.clear();
    outbuf_.clear();
    outOff_ = 0;
    ready_ = closing_ = closed_ = false;
    heartbeat_ = 0;
    return wasReady;
}

void ConnectionCore::closeSocket() noexcept {
    if (sock_ >= 0) {
        ::close(sock_);
        sock_ = -1;
    }
}

void ConnectionCore::notifyLost() {
    for (auto& observer : observers_) {
        if (observer.lost) guarded([&] { observer.lost(); });
    }
}

int ConnectionCore::pollTimeoutMs(TimePoint now) const {
    std::optional<TimePoint> next;
    const auto consider = [&next](TimePoint t) {
        if (!next || t < *next) next = t;
    };

    if (stopRequested_.load()) {
        if (amqp_ && closing_) consider(closeDeadline_);
    } else if (!amqp_) {
        if (!exhausted_) consider(reconnectAt_);
    } else {
        if (!ready_) consider(handshakeDeadline_);
        if (heartbeat_ != 0) {
            consider(nextHeartbeat_);
            consider(lastRx_ + 2 * std::chrono::seconds(heartbeat_));
        }
    }

    if (!stopRequested_.load() && !timers_.empty()) consider(timers_.begin()->first);

    if (!next) return -1;  // nothing scheduled: sleep until woken
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(*next - now).count() + 1;
    return static_cast<int>(std::clamp<long long>(ms, 0, INT_MAX));
}

}  // namespace rmq::detail
