#pragma once

namespace rmq::detail {

/// User callbacks must never take the IO thread down.
template <typename F>
void guarded(F&& f) noexcept {
    try {
        f();
    } catch (...) {
    }
}

}  // namespace rmq::detail
