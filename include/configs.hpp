#pragma once

#include <cstdint>
#include <type_traits>

static inline constexpr auto page_size = 0x1000;  // 4KB, assumed
static inline constexpr auto n_pages = 8;
static inline constexpr auto rdma_size = page_size * n_pages;

template<typename T>
static inline constexpr auto round_down_to_page_boundary(T address) {
    const auto uptr = [address] {
        if constexpr (std::is_const_v<T>) {
            return reinterpret_cast<const std::uintptr_t>(address);
        } else {
            return reinterpret_cast<std::uintptr_t>(address);
        }
    }();
    return reinterpret_cast<T>(uptr & ~(page_size - 1));
}
