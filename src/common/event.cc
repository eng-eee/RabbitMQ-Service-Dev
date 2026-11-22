#include "app_common/event.hpp"

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace app {

namespace {

std::atomic<unsigned long long> g_counter{0};

std::string formatUtc(std::chrono::system_clock::time_point timePoint) {
    const std::time_t seconds = std::chrono::system_clock::to_time_t(timePoint);
    std::tm tm{};
    gmtime_r(&seconds, &tm);
    std::ostringstream stream;
    stream << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return stream.str();
}

}  // namespace

std::string Event::toJson() const {
    json::Value root = json::Value::makeObject();
    root["event_type"] = json::Value(type);
    root["order_id"] = json::Value(order_id);
    root["timestamp"] = json::Value(nowIso8601());
    root["data"] = data;
    return root.dump();
}

Event Event::fromJson(const std::string& body) {
    const json::Value root = json::Value::parse(body);
    if (!root.isObject()) {
        throw json::ParseError("event must be a JSON object");
    }

    Event event;
    event.type = root["event_type"].asString();
    event.order_id = root["order_id"].asString();
    if (event.type.empty()) {
        throw json::ParseError("event is missing event_type");
    }
    if (root["data"].isObject()) {
        event.data = root["data"];
    }
    return event;
}

rmq::Message Event::toMessage() const {
    rmq::Message message;
    message.body = toJson();
    message.content_type = "application/json";
    message.persistent = true;
    if (!order_id.empty()) {
        message.message_id = order_id + ":" + type;
    }
    return message;
}

std::string nowIso8601() {
    return formatUtc(std::chrono::system_clock::now());
}

std::string generateId(const std::string& prefix) {
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    const unsigned long long counter = ++g_counter;
    return prefix + "-" + std::to_string(now) + "-" + std::to_string(counter);
}

}  // namespace app
