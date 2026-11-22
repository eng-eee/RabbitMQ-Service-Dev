#include "app_common/event.hpp"
#include "app_common/logging.hpp"
#include "app_common/service.hpp"
#include "log.h"

#include <iomanip>
#include <sstream>
#include <string>

namespace {

class NotificationService : public app::Service {
public:
    NotificationService() : Service("notification-service") {}

    bool start() override {
        subscribe("notification.events",
                  {{exchange(), app::event_type::OrderCreated},
                   {exchange(), app::event_type::OrderRejected},
                   {exchange(), app::event_type::OrderCompleted},
                   {exchange(), app::event_type::OrderCancelled},
                   {exchange(), app::event_type::InventoryRejected},
                   {exchange(), app::event_type::InventoryReleased},
                   {exchange(), app::event_type::PaymentCompleted},
                   {exchange(), app::event_type::PaymentFailed},
                   {exchange(), app::event_type::PaymentRefunded}},
                  [this](const rmq::Delivery& delivery) {
                      enqueue([this, delivery] { process(delivery); });
                      return rmq::AckAction::Ack;
                  });
        return true;
    }

private:
    void process(const rmq::Delivery& delivery) {
        try {
            const app::Event event = app::Event::fromJson(delivery.message.body);
            const std::string message = render(event);
            if (message.empty()) {
                LOG(DEBUG) << "[notification] no notification for event '" << event.type << "'\n";
                return;
            }
            LOG(INFO) << "[notification] " << message << "\n";
        } catch (const std::exception& e) {
            LOG(ERROR) << "[notification] failed to process notification: " << e.what() << "\n";
        }
    }

    static std::string formatAmount(double amount) {
        std::ostringstream stream;
        stream << std::fixed << std::setprecision(2) << amount;
        return stream.str();
    }

    static std::string render(const app::Event& event) {
        const std::string& id = event.order_id;

        if (event.type == app::event_type::OrderCreated) {
            return "Order " + id + " created for customer " +
                   event.data["customer_id"].asString() + " (amount " +
                   formatAmount(event.data["amount"].asDouble(0.0)) + ")";
        }
        if (event.type == app::event_type::OrderRejected) {
            return "Order request rejected: " + event.data["reason"].asString();
        }
        if (event.type == app::event_type::PaymentCompleted) {
            return "Payment received for order " + id + " (transaction " +
                   event.data["transaction_id"].asString() + ")";
        }
        if (event.type == app::event_type::PaymentFailed) {
            return "Payment failed for order " + id + ": " + event.data["reason"].asString();
        }
        if (event.type == app::event_type::PaymentRefunded) {
            return "Refund issued for order " + id + " (transaction " +
                   event.data["transaction_id"].asString() + ")";
        }
        if (event.type == app::event_type::InventoryRejected) {
            return "Inventory rejected for order " + id + ": " +
                   event.data["reason"].asString();
        }
        if (event.type == app::event_type::InventoryReleased) {
            return "Inventory released for order " + id;
        }
        if (event.type == app::event_type::OrderCompleted) {
            return "Order " + id + " completed successfully";
        }
        if (event.type == app::event_type::OrderCancelled) {
            return "Order " + id + " cancelled: " + event.data["reason"].asString();
        }
        return "";
    }
};

}  // namespace

int main() {
    NotificationService service;
    return app::runService(service);
}
