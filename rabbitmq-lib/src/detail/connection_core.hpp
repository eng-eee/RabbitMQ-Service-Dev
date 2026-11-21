#pragma once

// Private header: the only place (together with connection_core.cpp) that
// includes AMQP-CPP. Not installed, not visible to library users.

#include <rmq/connection.hpp>

#include <amqpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace rmq::detail {

/// Owns the IO thread, the TCP socket and the AMQP::Connection.
///
/// Threading model: every AMQP-CPP object (connection, channels, deferred
/// callbacks) is touched by the IO thread only. Other threads talk to it via
/// post(), which queues a task and wakes the poll() loop through a self-pipe.
class ConnectionCore final : public AMQP::ConnectionHandler {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    /// Runs on the IO thread. The argument is the live AMQP connection when the
    /// handshake is complete and nullptr otherwise.
    using Task = std::function<void(AMQP::Connection*)>;
    using ReadyFn = std::function<void(AMQP::Connection&)>;  // (re)connected: create channels here
    using LostFn = std::function<void()>;                    // connection gone: drop channels here

    explicit ConnectionCore(ConnectionOptions options);
    ~ConnectionCore() override;

    ConnectionCore(const ConnectionCore&) = delete;
    ConnectionCore& operator=(const ConnectionCore&) = delete;

    void start();
    void stop();

    ConnectionState state() const;
    bool waitConnected(std::chrono::milliseconds timeout) const;
    const ConnectionOptions& options() const noexcept { return opts_; }

    void setStateCallback(StateCallback callback);
    void setErrorCallback(ErrorCallback callback);

    /// Thread-safe. Returns false once the IO thread has finished.
    bool post(Task task);

    /// Thread-safe. `ready` is invoked on every (re)connect - immediately if the
    /// connection is already up - and `lost` whenever a ready connection goes away.
    std::uint64_t addObserver(ReadyFn ready, LostFn lost);
    void removeObserver(std::uint64_t id);

    /// Thread-safe. Runs `task` on the IO thread after `delay` (also while disconnected;
    /// not while stopping). Returns an id for cancelTimer().
    std::uint64_t postDelayed(std::chrono::milliseconds delay, Task task);
    void cancelTimer(std::uint64_t id);

    /// Thread-safe. Forwards to the user's ErrorCallback.
    void reportError(const Status& status);

    bool onIoThread() const noexcept { return ioThreadId_.load() == std::this_thread::get_id(); }

private:
    struct Observer {
        std::uint64_t id;
        ReadyFn ready;
        LostFn lost;
    };

    // ---- AMQP::ConnectionHandler (all invoked on the IO thread) -----------
    void onData(AMQP::Connection* connection, const char* buffer, std::size_t size) override;
    std::uint16_t onNegotiate(AMQP::Connection* connection, std::uint16_t interval) override;
    void onReady(AMQP::Connection* connection) override;
    void onError(AMQP::Connection* connection, const char* message) override;
    void onClosed(AMQP::Connection* connection) override;

    // ---- IO thread ---------------------------------------------------------
    void run();
    void drainTasks();
    void runTimers(TimePoint now);
    void tryConnect();
    int openSocket(std::string& error);
    void serviceTimers(TimePoint now);
    bool shutdownStep(TimePoint now);
    void flushOutput();
    void readSocket();
    void processInput();
    bool resolveFailure();  // true => leave the loop (failure while stopping)
    void handleFailure();
    bool teardownConnection();  // returns whether the connection had been ready
    void closeSocket() noexcept;
    int pollTimeoutMs(TimePoint now) const;
    void notifyLost();

    void fail(ErrorCode code, std::string message);
    void setState(ConnectionState state, const std::string& detail = {});
    std::string endpoint() const;
    std::chrono::milliseconds heartbeatHalf() const;

    void wake() noexcept;
    void drainWake() noexcept;

    const ConnectionOptions opts_;

    // lifecycle
    std::mutex threadMutex_;
    std::thread thread_;
    bool started_ = false;
    std::atomic<bool> stopRequested_{false};
    std::atomic<bool> finished_{false};
    std::atomic<std::thread::id> ioThreadId_{};

    // state + callbacks (any thread)
    mutable std::mutex stateMutex_;
    mutable std::condition_variable stateCv_;
    ConnectionState state_ = ConnectionState::Idle;
    std::mutex callbackMutex_;
    StateCallback stateCb_;
    ErrorCallback errorCb_;

    // cross-thread task queue
    std::mutex taskMutex_;
    std::deque<Task> tasks_;
    int wake_[2] = {-1, -1};
    std::atomic<std::uint64_t> nextObserverId_{1};
    std::atomic<std::uint64_t> nextTimerId_{1};

    // ---- IO-thread-only state ----------------------------------------------
    struct Timer {
        std::uint64_t id;
        Task fn;
    };
    std::vector<Observer> observers_;
    std::multimap<TimePoint, Timer> timers_;
    std::unique_ptr<AMQP::Connection> amqp_;
    int sock_ = -1;
    std::vector<char> inbuf_;
    std::vector<char> outbuf_;
    std::size_t outOff_ = 0;

    bool ready_ = false;
    bool hasConnected_ = false;
    bool closing_ = false;
    bool closed_ = false;
    bool tearingDown_ = false;
    bool exhausted_ = false;  // auto_reconnect off and the connection failed
    std::optional<Status> failure_;

    std::uint16_t heartbeat_ = 0;  // negotiated, seconds
    TimePoint lastRx_{};
    TimePoint nextHeartbeat_{};
    TimePoint handshakeDeadline_{};
    TimePoint closeDeadline_{};
    TimePoint reconnectAt_{};
    std::chrono::milliseconds backoff_{0};
};

}  // namespace rmq::detail
