#include "app_common/event.hpp"
#include "app_common/logging.hpp"
#include "app_common/service.hpp"
#include "log.h"

#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

struct PaymentRecord {
    std::string status;
    std::string transaction_id;
    double amount = 0.0;
    bool refunded = false;
};

class PaymentService : public app::Service {
public:
    PaymentService() : Service("payment-service") {}

    bool init() override {
        if (!Service::init()) return false;
        failureRate_ = config().getDouble("PAYMENT_FAILURE_RATE", 0.0);
        maxAmount_ = config().getDouble("PAYMENT_MAX_AMOUNT", 5000.0);
        std::random_device device;
        rng_.seed(device());
        LOG(INFO) << "payment simulation: failure_rate=" << failureRate_
                  << " max_amount=" << maxAmount_ << "\n";
        return true;
    }

    bool start() override {
        subscribe("payment.events",
                  {{exchange(), app::event_type::OrderCreated},
                   {exchange(), app::event_type::OrderCancelled}},
                  [this](const rmq::Delivery& delivery) {
                      enqueue([this, delivery] { handleEvent(delivery); });
                      return rmq::AckAction::Ack;
                  });
        return true;
    }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, PaymentRecord> payments_;
    std::unordered_set<std::string> cancelled_;
    double failureRate_ = 0.0;
    double maxAmount_ = 5000.0;
    std::mt19937 rng_;

    bool shouldFail() {
        std::uniform_real_distribution<double> distribution(0.0, 1.0);
        return distribution(rng_) < failureRate_;
    }

    void handleEvent(const rmq::Delivery& delivery) {
        app::Event event;
        try {
            event = app::Event::fromJson(delivery.message.body);
        } catch (const std::exception& e) {
            LOG(ERROR) << "discarding invalid payment input: " << e.what() << "\n";
            return;
        }

        if (event.type == app::event_type::OrderCreated) {
            handleOrderCreated(event);
        } else if (event.type == app::event_type::OrderCancelled) {
            handleOrderCancelled(event);
        }
    }

    void handleOrderCreated(const app::Event& event) {
        std::vector<app::Event> out;
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (cancelled_.find(event.order_id) != cancelled_.end()) {
                LOG(INFO) << "skipping payment for cancelled order " << event.order_id << "\n";
                return;
            }

            if (payments_.find(event.order_id) != payments_.end()) {
                LOG(INFO) << "duplicate order.created for " << event.order_id
                          << "; payment already handled\n";
                return;
            }

            const double amount = event.data["amount"].asDouble(0.0);
            if (amount <= 0.0) {
                PaymentRecord record;
                record.status = "FAILED";
                payments_[event.order_id] = record;
                out.push_back(makeFailed(event.order_id, "invalid payment amount"));
                LOG(WARN) << "payment rejected for order " << event.order_id
                          << ": invalid amount\n";
            } else if (event.data["force_payment_failure"].asBool(false) ||
                       amount > maxAmount_ || shouldFail()) {
                PaymentRecord record;
                record.status = "FAILED";
                record.amount = amount;
                payments_[event.order_id] = record;

                std::string reason = "simulated payment failure";
                if (amount > maxAmount_) {
                    reason = "amount exceeds the configured payment limit";
                }
                out.push_back(makeFailed(event.order_id, reason));
                LOG(WARN) << "payment failed for order " << event.order_id << ": " << reason
                          << "\n";
            } else {
                PaymentRecord record;
                record.status = "COMPLETED";
                record.transaction_id = app::generateId("TXN");
                record.amount = amount;
                payments_[event.order_id] = record;

                app::json::Value data = app::json::Value::makeObject();
                data["order_id"] = app::json::Value(event.order_id);
                data["status"] = app::json::Value("COMPLETED");
                data["transaction_id"] = app::json::Value(record.transaction_id);
                data["amount"] = app::json::Value(amount);

                app::Event completed;
                completed.type = app::event_type::PaymentCompleted;
                completed.order_id = event.order_id;
                completed.data = std::move(data);
                out.push_back(std::move(completed));

                LOG(INFO) << "payment completed for order " << event.order_id
                          << " transaction=" << record.transaction_id << " amount=" << amount
                          << "\n";
            }
        }

        for (const auto& event : out) {
            publishEvent(event);
        }
    }

    static app::Event makeFailed(const std::string& orderId, const std::string& reason) {
        app::json::Value data = app::json::Value::makeObject();
        data["order_id"] = app::json::Value(orderId);
        data["status"] = app::json::Value("FAILED");
        data["reason"] = app::json::Value(reason);

        app::Event failed;
        failed.type = app::event_type::PaymentFailed;
        failed.order_id = orderId;
        failed.data = std::move(data);
        return failed;
    }

    void handleOrderCancelled(const app::Event& event) {
        std::vector<app::Event> out;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cancelled_.insert(event.order_id);

            const auto it = payments_.find(event.order_id);
            if (it == payments_.end()) {
                LOG(INFO) << "cancel for order " << event.order_id
                          << " without a payment record\n";
                return;
            }

            PaymentRecord& record = it->second;
            if (record.status != "COMPLETED" || record.refunded) {
                LOG(INFO) << "no refund needed for order " << event.order_id << " (status="
                          << record.status << ")\n";
                return;
            }

            record.refunded = true;

            app::json::Value data = app::json::Value::makeObject();
            data["order_id"] = app::json::Value(event.order_id);
            data["status"] = app::json::Value("REFUNDED");
            data["transaction_id"] = app::json::Value(record.transaction_id);
            data["amount"] = app::json::Value(record.amount);

            app::Event refunded;
            refunded.type = app::event_type::PaymentRefunded;
            refunded.order_id = event.order_id;
            refunded.data = std::move(data);
            out.push_back(std::move(refunded));

            LOG(INFO) << "payment refunded for order " << event.order_id
                      << " transaction=" << record.transaction_id << "\n";
        }

        for (const auto& event : out) {
            publishEvent(event);
        }
    }
};

}  // namespace

int main() {
    PaymentService service;
    return app::runService(service);
}
