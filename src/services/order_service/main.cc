#include "app_common/event.hpp"
#include "app_common/logging.hpp"
#include "app_common/service.hpp"
#include "log.h"

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

struct OrderState {
    std::string order_id;
    std::string customer_id;
    app::json::Value items;
    double amount = 0.0;
    std::string status = "CREATED";
    bool inventory_done = false;
    bool inventory_ok = false;
    bool payment_done = false;
    bool payment_ok = false;
    bool finalized = false;
    std::chrono::system_clock::time_point created_at = std::chrono::system_clock::now();
    std::vector<std::string> history;
};

class OrderService : public app::Service {
public:
    OrderService() : Service("order-service") {}

    bool start() override {
        subscribe("order.requests", {{exchange(), app::event_type::OrderRequest}},
                  [this](const rmq::Delivery& delivery) {
                      enqueue([this, delivery] { handleRequest(delivery); });
                      return rmq::AckAction::Ack;
                  });

        subscribe("order.inventory.results",
                  {{exchange(), app::event_type::InventoryReserved},
                   {exchange(), app::event_type::InventoryRejected},
                   {exchange(), app::event_type::InventoryReleased}},
                  [this](const rmq::Delivery& delivery) {
                      enqueue([this, delivery] { handleInventoryEvent(delivery); });
                      return rmq::AckAction::Ack;
                  });

        subscribe("order.payment.results",
                  {{exchange(), app::event_type::PaymentCompleted},
                   {exchange(), app::event_type::PaymentFailed},
                   {exchange(), app::event_type::PaymentRefunded}},
                  [this](const rmq::Delivery& delivery) {
                      enqueue([this, delivery] { handlePaymentEvent(delivery); });
                      return rmq::AckAction::Ack;
                  });
        return true;
    }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, OrderState> orders_;

    static bool validate(const app::json::Value& data, std::string& error) {
        if (data["customer_id"].asString().empty()) {
            error = "customer_id is required";
            return false;
        }
        const app::json::Value& items = data["items"];
        if (!items.isArray() || items.size() == 0) {
            error = "items must be a non-empty array";
            return false;
        }
        for (std::size_t i = 0; i < items.size(); ++i) {
            const app::json::Value& item = items[i];
            if (!item.isObject()) {
                error = "each item must be an object";
                return false;
            }
            if (item["product_id"].asString().empty()) {
                error = "item.product_id is required";
                return false;
            }
            if (item["quantity"].asInt(0) <= 0) {
                error = "item.quantity must be greater than zero";
                return false;
            }
            if (item["unit_price"].asDouble(-1.0) < 0.0) {
                error = "item.unit_price must not be negative";
                return false;
            }
        }
        return true;
    }

    static double totalAmount(const app::json::Value& items) {
        double total = 0.0;
        for (std::size_t i = 0; i < items.size(); ++i) {
            const app::json::Value& item = items[i];
            total += static_cast<double>(item["quantity"].asInt(0)) *
                     item["unit_price"].asDouble(0.0);
        }
        return total;
    }

    static void addHistory(OrderState& order, const std::string& status) {
        order.history.push_back(app::nowIso8601() + " " + status);
    }

    static void statusEvent(const OrderState& order, const std::string& previous,
                            std::vector<app::Event>& out) {
        app::json::Value data = app::json::Value::makeObject();
        data["order_id"] = app::json::Value(order.order_id);
        data["status"] = app::json::Value(order.status);
        data["previous_status"] = app::json::Value(previous);

        app::Event event;
        event.type = app::event_type::OrderStatusUpdated;
        event.order_id = order.order_id;
        event.data = std::move(data);
        out.push_back(std::move(event));
    }

    static void transition(OrderState& order, const std::string& status,
                           std::vector<app::Event>& out) {
        const std::string previous = order.status;
        order.status = status;
        addHistory(order, status);
        statusEvent(order, previous, out);
    }

    static void finalizeComplete(OrderState& order, std::vector<app::Event>& out) {
        if (order.finalized) return;
        order.finalized = true;
        const std::string previous = order.status;
        order.status = "COMPLETED";
        addHistory(order, order.status);

        app::json::Value data = app::json::Value::makeObject();
        data["order_id"] = app::json::Value(order.order_id);
        data["customer_id"] = app::json::Value(order.customer_id);
        data["amount"] = app::json::Value(order.amount);
        data["status"] = app::json::Value(order.status);

        app::Event completed;
        completed.type = app::event_type::OrderCompleted;
        completed.order_id = order.order_id;
        completed.data = data;
        out.push_back(std::move(completed));

        statusEvent(order, previous, out);
        LOG(INFO) << "order " << order.order_id << " COMPLETED (amount=" << order.amount
                  << ")\n";
    }

    static void finalizeCancel(OrderState& order, const std::string& reason,
                               std::vector<app::Event>& out) {
        if (order.finalized) return;
        order.finalized = true;
        const std::string previous = order.status;
        order.status = "CANCELLED";
        addHistory(order, order.status + ": " + reason);

        app::json::Value data = app::json::Value::makeObject();
        data["order_id"] = app::json::Value(order.order_id);
        data["customer_id"] = app::json::Value(order.customer_id);
        data["amount"] = app::json::Value(order.amount);
        data["status"] = app::json::Value(order.status);
        data["reason"] = app::json::Value(reason);

        app::Event cancelled;
        cancelled.type = app::event_type::OrderCancelled;
        cancelled.order_id = order.order_id;
        cancelled.data = data;
        out.push_back(std::move(cancelled));

        statusEvent(order, previous, out);
        LOG(WARN) << "order " << order.order_id << " CANCELLED: " << reason << "\n";
    }

    static void maybeFinalize(OrderState& order, std::vector<app::Event>& out) {
        if (order.finalized) return;

        if (order.inventory_done && !order.inventory_ok) {
            finalizeCancel(order, "inventory rejected", out);
            return;
        }
        if (order.payment_done && !order.payment_ok) {
            finalizeCancel(order, "payment failed", out);
            return;
        }
        if (order.inventory_done && order.payment_done) {
            if (order.inventory_ok && order.payment_ok) {
                finalizeComplete(order, out);
            } else {
                finalizeCancel(order, "inventory or payment failed", out);
            }
        }
    }

    void publishAll(const std::vector<app::Event>& events) {
        for (const auto& event : events) {
            publishEvent(event);
        }
    }

    void handleRequest(const rmq::Delivery& delivery) {
        app::Event request;
        try {
            request = app::Event::fromJson(delivery.message.body);
        } catch (const std::exception& e) {
            LOG(ERROR) << "discarding invalid order request: " << e.what() << "\n";
            return;
        }
        if (request.type != app::event_type::OrderRequest) {
            LOG(WARN) << "ignoring unexpected message '" << request.type << "' on order.requests\n";
            return;
        }

        std::string error;
        if (!validate(request.data, error)) {
            LOG(WARN) << "rejecting invalid order request: " << error << "\n";
            app::json::Value data = app::json::Value::makeObject();
            data["customer_id"] = request.data["customer_id"];
            data["reason"] = app::json::Value(error);
            app::Event rejected;
            rejected.type = app::event_type::OrderRejected;
            rejected.data = std::move(data);
            publishEvent(rejected);
            return;
        }

        std::vector<app::Event> out;
        std::string orderId;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            OrderState order;
            order.order_id = app::generateId("ORD");
            order.customer_id = request.data["customer_id"].asString();
            order.items = request.data["items"];
            order.amount = totalAmount(order.items);
            addHistory(order, order.status);

            if (order.amount <= 0.0) {
                LOG(WARN) << "rejecting order with a non-positive amount\n";
                app::json::Value data = app::json::Value::makeObject();
                data["customer_id"] = app::json::Value(order.customer_id);
                data["reason"] = app::json::Value("order amount must be greater than zero");

                app::Event rejected;
                rejected.type = app::event_type::OrderRejected;
                rejected.data = std::move(data);
                out.push_back(std::move(rejected));
            } else {
                orderId = order.order_id;
                orders_.emplace(order.order_id, order);

                app::json::Value data = app::json::Value::makeObject();
                data["order_id"] = app::json::Value(order.order_id);
                data["customer_id"] = app::json::Value(order.customer_id);
                data["items"] = order.items;
                data["amount"] = app::json::Value(order.amount);
                data["status"] = app::json::Value(order.status);
                if (request.data.contains("force_payment_failure")) {
                    data["force_payment_failure"] =
                        request.data["force_payment_failure"];
                }

                app::Event created;
                created.type = app::event_type::OrderCreated;
                created.order_id = order.order_id;
                created.data = data;
                out.push_back(std::move(created));

                LOG(INFO) << "created order " << order.order_id << " for customer "
                          << order.customer_id << " amount=" << order.amount << "\n";
            }
        }

        publishAll(out);
        if (!orderId.empty()) {
            LOG(INFO) << "published order.created for " << orderId << "\n";
        }
    }

    void handleInventoryEvent(const rmq::Delivery& delivery) {
        app::Event event;
        try {
            event = app::Event::fromJson(delivery.message.body);
        } catch (const std::exception& e) {
            LOG(ERROR) << "discarding invalid inventory event: " << e.what() << "\n";
            return;
        }

        std::vector<app::Event> out;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = orders_.find(event.order_id);
            if (it == orders_.end()) {
                LOG(WARN) << "inventory event for unknown order " << event.order_id << "\n";
                return;
            }
            OrderState& order = it->second;
            if (order.finalized) {
                LOG(INFO) << "ignoring " << event.type << " for finalized order "
                          << order.order_id << "\n";
                return;
            }

            if (event.type == app::event_type::InventoryReserved) {
                if (order.inventory_done) return;
                order.inventory_done = true;
                order.inventory_ok = true;
                transition(order, "INVENTORY_RESERVED", out);
                maybeFinalize(order, out);
            } else if (event.type == app::event_type::InventoryRejected) {
                if (order.inventory_done) return;
                order.inventory_done = true;
                order.inventory_ok = false;
                transition(order, "INVENTORY_REJECTED", out);
                maybeFinalize(order, out);
            } else if (event.type == app::event_type::InventoryReleased) {
                LOG(INFO) << "inventory released for order " << order.order_id << "\n";
            }
        }
        publishAll(out);
    }

    void handlePaymentEvent(const rmq::Delivery& delivery) {
        app::Event event;
        try {
            event = app::Event::fromJson(delivery.message.body);
        } catch (const std::exception& e) {
            LOG(ERROR) << "discarding invalid payment event: " << e.what() << "\n";
            return;
        }

        std::vector<app::Event> out;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = orders_.find(event.order_id);
            if (it == orders_.end()) {
                LOG(WARN) << "payment event for unknown order " << event.order_id << "\n";
                return;
            }
            OrderState& order = it->second;
            if (order.finalized) {
                LOG(INFO) << "ignoring " << event.type << " for finalized order "
                          << order.order_id << "\n";
                return;
            }

            if (event.type == app::event_type::PaymentCompleted) {
                if (order.payment_done) return;
                order.payment_done = true;
                order.payment_ok = true;
                transition(order, "PAYMENT_COMPLETED", out);
                maybeFinalize(order, out);
            } else if (event.type == app::event_type::PaymentFailed) {
                if (order.payment_done) return;
                order.payment_done = true;
                order.payment_ok = false;
                transition(order, "PAYMENT_FAILED", out);
                maybeFinalize(order, out);
            } else if (event.type == app::event_type::PaymentRefunded) {
                LOG(INFO) << "payment refunded for order " << order.order_id << "\n";
            }
        }
        publishAll(out);
    }
};

}  // namespace

int main() {
    OrderService service;
    return app::runService(service);
}
