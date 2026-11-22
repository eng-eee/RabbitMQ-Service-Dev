#include "app_common/service.hpp"

#include "log.h"

#include <chrono>
#include <utility>

namespace app {

using namespace std::chrono_literals;

Worker::Worker(std::string name) : name_(std::move(name)) {
    thread_ = std::thread(&Worker::run, this);
}

Worker::~Worker() {
    stop();
}

void Worker::enqueue(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            LOG(WARN) << "worker '" << name_ << "' is stopping, dropping task\n";
            return;
        }
        queue_.push_back(std::move(task));
    }
    cv_.notify_one();
}

void Worker::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Worker::run() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) {
                if (stopping_) break;
                continue;
            }
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        try {
            task();
        } catch (const std::exception& e) {
            LOG(ERROR) << "worker '" << name_ << "' task failed: " << e.what() << "\n";
        } catch (...) {
            LOG(ERROR) << "worker '" << name_ << "' task failed with an unknown error\n";
        }
    }
}

Service::Service(std::string name) : name_(std::move(name)) {}

Service::~Service() {
    stop();
}

bool Service::init() {
    config_ = Config::load();
    exchange_ = config_.getString("RABBITMQ_EXCHANGE", "order.events");
    // Fill the initial config 
    rmq::ConnectionOptions options;
    options.host = config_.getString("RABBITMQ_HOST", "localhost");
    options.port = static_cast<std::uint16_t>(config_.getInt("RABBITMQ_PORT", 5672));
    options.username = config_.getString("RABBITMQ_DEFAULT_USER", "developer");
    options.password = config_.getString("RABBITMQ_DEFAULT_PASS", "developer123");
    options.vhost = config_.getString("RABBITMQ_VHOST", "/");
    options.heartbeat_seconds =
        static_cast<std::uint16_t>(config_.getInt("RABBITMQ_HEARTBEAT_SECONDS", 60));
    options.auto_reconnect = config_.getBool("RABBITMQ_AUTO_RECONNECT", true);

    LOG(INFO) << "connecting to RabbitMQ at " << options.host << ":" << options.port
              << " as '" << options.username << "' vhost='" << options.vhost
              << "' exchange='" << exchange_ << "'\n";

    try {
        // Initialize the connection
        connection_ = std::make_unique<rmq::Connection>(options);
        connection_->setStateCallback([](rmq::ConnectionState state, const std::string& detail) {
            LOG(INFO) << "[rmq] state=" << rmq::to_string(state) << " " << detail << "\n";
        });
        connection_->setErrorCallback([](const rmq::Status& status) {
            LOG(ERROR) << "[rmq] " << rmq::to_string(status.code) << ": " << status.message
                       << "\n";
        });
        connection_->start();

        if (!connection_->waitConnected(10s)) {
            LOG(WARN) << "RabbitMQ not reachable yet; the library keeps retrying in the "
                         "background\n";
        }
        // Initialize the publisher
        rmq::PublisherOptions publisherOptions;
        publisherOptions.exchange = exchange_;
        publisherOptions.exchange_type = "topic";
        publisherOptions.declare_exchange = true;
        publisherOptions.durable = true;
        publisher_ = std::make_unique<rmq::Publisher>(*connection_, publisherOptions);
        // Create a worker 
        worker_ = std::make_unique<Worker>(name_ + "-worker");
    } catch (const rmq::Error& e) {
        LOG(ERROR) << "failed to initialise RabbitMQ (" << rmq::to_string(e.code())
                   << "): " << e.what() << "\n";
        return false;
    }

    if (connection_->isConnected()) {
        ensureExchange();
    }
    return true;
}

void Service::ensureExchange() {
    // The publisher declares the exchange asynchronously. Publishing one
    // unroutable message forces the declaration to complete before subscribers
    // bind, which avoids transient NOT_FOUND errors.
    rmq::Message warmup;
    warmup.body = "{}";
    warmup.content_type = "application/json";
    const rmq::Status status = publisher_->publish("app.warmup", warmup, 5s);
    if (!status) {
        LOG(WARN) << "exchange warm-up for '" << exchange_
                  << "' failed: " << status.message << "\n";
    } else {
        LOG(INFO) << "exchange '" << exchange_ << "' is ready\n";
    }
}

void Service::subscribe(const std::string& queue,
                        std::vector<rmq::Binding> bindings,
                        rmq::Subscriber::Handler handler,
                        std::uint16_t prefetch) {
    rmq::SubscriberOptions options;
    options.queue = queue;
    options.durable = true;
    options.exclusive = false;
    options.auto_delete = false;
    options.bindings = std::move(bindings);
    options.prefetch = prefetch;

    auto subscriber = std::make_unique<rmq::Subscriber>(*connection_, options);
    subscriber->start(std::move(handler));
    subscribers_.push_back(std::move(subscriber));
    LOG(INFO) << "subscribed queue '" << queue << "'\n";
}

bool Service::publishEvent(const Event& event, const std::string& routing_key) {
    if (!publisher_) return false;
    const std::string key = routing_key.empty() ? event.type : routing_key;
    const rmq::Status status = publisher_->publish(key, event.toMessage(), 5s);
    if (!status) {
        LOG(ERROR) << "failed to publish '" << key << "' for order '" << event.order_id
                   << "': " << status.message << "\n";
        return false;
    }
    return true;
}

void Service::enqueue(std::function<void()> task) {
    if (worker_) worker_->enqueue(std::move(task));
}

void Service::stop() {
    if (stopped_.exchange(true)) return;

    subscribers_.clear();
    if (worker_) worker_->stop();
    publisher_.reset();
    if (connection_) connection_->stop();
}

}  // namespace app
