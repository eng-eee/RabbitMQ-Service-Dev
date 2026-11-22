#include "app_common/event.hpp"
#include "app_common/logging.hpp"
#include "app_common/service.hpp"
#include "log.h"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

struct Reservation {
    app::json::Value items = app::json::Value::makeArray();
    bool reserved = false;
    bool released = false;
};

class InventoryService : public app::Service {
public:
    InventoryService() : Service("inventory-service") {}

    bool init() override {
        if (!Service::init()) return false;
        defaultStock_ = config().getInt("INVENTORY_DEFAULT_STOCK", 100);
        LOG(INFO) << "inventory default stock per product: " << defaultStock_ << "\n";
        return true;
    }

    bool start() override {
        subscribe("inventory.events",
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
    std::unordered_map<std::string, Reservation> reservations_;
    std::unordered_map<std::string, long long> stock_;
    long long defaultStock_ = 100;

    long long stockFor(const std::string& product) const {
        const auto it = stock_.find(product);
        return it == stock_.end() ? defaultStock_ : it->second;
    }

    void handleEvent(const rmq::Delivery& delivery) {
        app::Event event;
        try {
            event = app::Event::fromJson(delivery.message.body);
        } catch (const std::exception& e) {
            LOG(ERROR) << "discarding invalid inventory input: " << e.what() << "\n";
            return;
        }

        if (event.type == app::event_type::OrderCreated) {
            handleOrderCreated(event);
        } else if (event.type == app::event_type::OrderCancelled) {
            handleOrderCancelled(event);
        }
    }

    void handleOrderCreated(const app::Event& event) {
        const app::json::Value& items = event.data["items"];
        if (!items.isArray() || items.size() == 0) {
            LOG(ERROR) << "order " << event.order_id << " has no items to reserve\n";
            return;
        }

        std::vector<app::Event> out;
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (reservations_.find(event.order_id) != reservations_.end()) {
                LOG(INFO) << "duplicate order.created for " << event.order_id
                          << "; reservation already handled\n";
                return;
            }

            app::json::Value missing = app::json::Value::makeArray();
            bool sufficient = true;
            for (std::size_t i = 0; i < items.size(); ++i) {
                const app::json::Value& item = items[i];
                const std::string product = item["product_id"].asString();
                const long long quantity = item["quantity"].asInt(0);
                const long long available = stockFor(product);
                if (quantity > available) {
                    sufficient = false;
                    app::json::Value entry = app::json::Value::makeObject();
                    entry["product_id"] = app::json::Value(product);
                    entry["requested"] = app::json::Value(quantity);
                    entry["available"] = app::json::Value(available);
                    missing.pushBack(std::move(entry));
                }
            }

            Reservation reservation;
            reservation.items = items;

            if (!sufficient) {
                reservation.reserved = false;
                reservations_[event.order_id] = reservation;

                app::json::Value data = app::json::Value::makeObject();
                data["order_id"] = app::json::Value(event.order_id);
                data["status"] = app::json::Value("REJECTED");
                data["reason"] = app::json::Value("insufficient stock");
                data["missing"] = std::move(missing);

                app::Event rejected;
                rejected.type = app::event_type::InventoryRejected;
                rejected.order_id = event.order_id;
                rejected.data = std::move(data);
                out.push_back(std::move(rejected));

                LOG(WARN) << "inventory rejected for order " << event.order_id
                          << " (insufficient stock)\n";
            } else {
                for (std::size_t i = 0; i < items.size(); ++i) {
                    const app::json::Value& item = items[i];
                    const std::string product = item["product_id"].asString();
                    const long long quantity = item["quantity"].asInt(0);
                    stock_[product] = stockFor(product) - quantity;
                }
                reservation.reserved = true;
                reservations_[event.order_id] = reservation;

                app::json::Value data = app::json::Value::makeObject();
                data["order_id"] = app::json::Value(event.order_id);
                data["status"] = app::json::Value("RESERVED");
                data["items"] = items;

                app::Event reserved;
                reserved.type = app::event_type::InventoryReserved;
                reserved.order_id = event.order_id;
                reserved.data = std::move(data);
                out.push_back(std::move(reserved));

                LOG(INFO) << "inventory reserved for order " << event.order_id << "\n";
            }
        }

        for (const auto& event : out) {
            publishEvent(event);
        }
    }

    void handleOrderCancelled(const app::Event& event) {
        std::vector<app::Event> out;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = reservations_.find(event.order_id);
            if (it == reservations_.end()) {
                LOG(INFO) << "cancel for order " << event.order_id
                          << " without an inventory reservation\n";
                return;
            }

            Reservation& reservation = it->second;
            if (!reservation.reserved || reservation.released) {
                LOG(INFO) << "no active reservation to release for order " << event.order_id
                          << "\n";
                return;
            }

            for (std::size_t i = 0; i < reservation.items.size(); ++i) {
                const app::json::Value& item = reservation.items[i];
                const std::string product = item["product_id"].asString();
                const long long quantity = item["quantity"].asInt(0);
                stock_[product] = stockFor(product) + quantity;
            }
            reservation.released = true;

            app::json::Value data = app::json::Value::makeObject();
            data["order_id"] = app::json::Value(event.order_id);
            data["status"] = app::json::Value("RELEASED");
            data["items"] = reservation.items;

            app::Event released;
            released.type = app::event_type::InventoryReleased;
            released.order_id = event.order_id;
            released.data = std::move(data);
            out.push_back(std::move(released));

            LOG(INFO) << "inventory released for order " << event.order_id << "\n";
        }

        for (const auto& event : out) {
            publishEvent(event);
        }
    }
};

}  // namespace

int main() {
    InventoryService service;
    return app::runService(service);
}
