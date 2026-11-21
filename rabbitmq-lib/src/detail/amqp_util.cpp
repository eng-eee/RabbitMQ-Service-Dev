#include "amqp_util.hpp"

namespace rmq::detail {

void fillMetadata(const Message& m, AMQP::MetaData& env) {
    if (!m.content_type.empty()) env.setContentType(m.content_type);
    if (!m.content_encoding.empty()) env.setContentEncoding(m.content_encoding);
    if (!m.correlation_id.empty()) env.setCorrelationID(m.correlation_id);
    if (!m.reply_to.empty()) env.setReplyTo(m.reply_to);
    if (!m.message_id.empty()) env.setMessageID(m.message_id);
    if (m.persistent) env.setDeliveryMode(2);
    if (m.expiration.count() > 0) env.setExpiration(std::to_string(m.expiration.count()));
    if (!m.headers.empty()) {
        AMQP::Table table;
        for (const auto& kv : m.headers) table.set(kv.first, kv.second);
        env.setHeaders(std::move(table));
    }
}

Delivery makeDelivery(const AMQP::Message& in, std::uint64_t deliveryTag, bool redelivered) {
    Delivery d;
    d.exchange = in.exchange();
    d.routing_key = in.routingkey();
    d.delivery_tag = deliveryTag;
    d.redelivered = redelivered;

    Message& m = d.message;
    m.body.assign(in.body(), static_cast<std::size_t>(in.bodySize()));
    if (in.hasContentType()) m.content_type = in.contentType();
    if (in.hasContentEncoding()) m.content_encoding = in.contentEncoding();
    if (in.hasCorrelationID()) m.correlation_id = in.correlationID();
    if (in.hasReplyTo()) m.reply_to = in.replyTo();
    if (in.hasMessageID()) m.message_id = in.messageID();
    m.persistent = in.hasDeliveryMode() && in.deliveryMode() == 2;
    if (in.hasExpiration()) {
        try {
            m.expiration = std::chrono::milliseconds(std::stoll(in.expiration()));
        } catch (...) {
        }
    }
    if (in.hasHeaders()) {
        const AMQP::Table& table = in.headers();
        for (const std::string& key : table.keys()) {
            const AMQP::Field& field = table.get(key);
            if (field.isString()) m.headers[key] = static_cast<const std::string&>(field);
        }
    }
    return d;
}

}  // namespace rmq::detail
