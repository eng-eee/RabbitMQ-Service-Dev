#pragma once

// Private: conversions between rmq types and AMQP-CPP types.

#include <rmq/message.hpp>

#include <amqpcpp.h>

namespace rmq::detail {

/// Copies everything except the body into AMQP message properties. Usage:
///   AMQP::Envelope env(m.body.data(), m.body.size());   // body is not copied; publish() does
///   detail::fillMetadata(m, env);
void fillMetadata(const Message& message, AMQP::MetaData& metadata);

/// Fills `out` from a received AMQP message.
Delivery makeDelivery(const AMQP::Message& message, std::uint64_t deliveryTag, bool redelivered);

}  // namespace rmq::detail
