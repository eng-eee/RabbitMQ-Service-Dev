#pragma once

#include "app_common/json.hpp"

#include <rmq/message.hpp>

#include <string>

namespace app {

namespace event_type {
inline constexpr const char* OrderRequest = "order.request";
inline constexpr const char* OrderCreated = "order.created";
inline constexpr const char* OrderRejected = "order.rejected";
inline constexpr const char* OrderStatusUpdated = "order.status.updated";
inline constexpr const char* OrderCompleted = "order.completed";
inline constexpr const char* OrderCancelled = "order.cancelled";

inline constexpr const char* InventoryReserved = "inventory.reserved";
inline constexpr const char* InventoryRejected = "inventory.rejected";
inline constexpr const char* InventoryReleased = "inventory.released";

inline constexpr const char* PaymentCompleted = "payment.completed";
inline constexpr const char* PaymentFailed = "payment.failed";
inline constexpr const char* PaymentRefunded = "payment.refunded";
}  // namespace event_type

/// Common envelope for every message exchanged through RabbitMQ.
struct Event {
    std::string type;
    std::string order_id;
    json::Value data = json::Value::makeObject();

    std::string toJson() const;
    static Event fromJson(const std::string& body);

    rmq::Message toMessage() const;
};

/// ISO-8601 UTC timestamp, e.g. 2026-11-22T19:51:00Z.
std::string nowIso8601();

/// Generates a process-unique identifier with the given prefix.
std::string generateId(const std::string& prefix);

}  // namespace app
