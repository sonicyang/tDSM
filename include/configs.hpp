#pragma once

#include <cstdint>
#include <type_traits>
#include <spdlog/spdlog.h>

#include "utils/logging.hpp"

static inline constexpr auto page_size = 0x1000;  // 4KB, assumed
static inline constexpr auto n_pages = 8;
static inline constexpr auto rdma_size = page_size * n_pages;

extern volatile std::uint8_t rdma_memory[rdma_size] __attribute__((section(".rdma"), aligned(page_size)));
static inline constexpr auto rdma_memory_ptr = const_cast<std::uint8_t*>(rdma_memory);

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

auto inline get_frame_number(void* const addr) {
    const auto uaddr = reinterpret_cast<std::uintptr_t>(addr);
    const auto base = reinterpret_cast<std::uintptr_t>(rdma_memory);
    // sanity check
    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
        !(base <= uaddr && uaddr < base + rdma_size),
        "The address is out-of-bound!"
    );
    return  (uaddr - base) / n_pages;
}

auto inline get_frame_address(const std::size_t frame_id) {
    const auto base = reinterpret_cast<std::uintptr_t>(rdma_memory);
    // sanity check
    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
        !(frame_id < n_pages),
        "The frame id is out-of-bound!"
    );
    return reinterpret_cast<void*>(frame_id * page_size + base);
}
