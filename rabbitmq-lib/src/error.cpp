#include <rmq/error.hpp>

namespace rmq {

const char* to_string(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::Ok:              return "Ok";
        case ErrorCode::InvalidArgument: return "InvalidArgument";
        case ErrorCode::NotConnected:    return "NotConnected";
        case ErrorCode::ConnectFailed:   return "ConnectFailed";
        case ErrorCode::Io:              return "Io";
        case ErrorCode::Protocol:        return "Protocol";
        case ErrorCode::Timeout:         return "Timeout";
        case ErrorCode::Closed:          return "Closed";
        case ErrorCode::Channel:         return "Channel";
        case ErrorCode::Overflow:        return "Overflow";
        case ErrorCode::Remote:          return "Remote";
        case ErrorCode::Internal:        return "Internal";
    }
    return "Unknown";
}

}  // namespace rmq
