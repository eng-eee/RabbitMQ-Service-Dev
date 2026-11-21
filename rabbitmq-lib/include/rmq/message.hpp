#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>

namespace rmq {

/// Transport-neutral message. Bodies are opaque bytes (std::string is used as
/// the byte container).
struct Message {
    std::string body;
    std::string content_type;      // e.g. "application/json"
    std::string content_encoding;
    std::string correlation_id;
    std::string reply_to;
    std::string message_id;
    std::map<std::string, std::string> headers;  // string-valued headers only (for now)
    std::chrono::milliseconds expiration{0};     // 0 = no per-message TTL
    bool persistent = false;                     // delivery-mode 2
};

/// A message as received from the broker.
struct Delivery {
    Message message;
    std::string exchange;
    std::string routing_key;
    std::uint64_t delivery_tag = 0;
    bool redelivered = false;
};

}  // namespace rmq
