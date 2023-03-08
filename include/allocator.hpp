/**
 * Copyright © 2023 Yang ChungFan <sonic.tw.tp@gmail.com>
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 *
 * This program is free software. It comes without any warranty, to
 * the extent permitted by applicable law. You can redistribute it
 * and/or modify it under the terms of the Do What The Fuck You Want
 * To Public License, Version 2, as published by Sam Hocevar. See
 * http://www.wtfpl.net/ for more details.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include "configs.hpp"
#include "swapper.hpp"
#include "utils/logging.hpp"

namespace tDSM {

namespace details {

class allocator {
 public:
    // must be power of 2
    static constexpr auto alignment = 16;

 private:
    struct alignas(alignment) block_header {
        bool in_use;
        std::size_t size;
        void* prev;
        void* next;
    } tDSM_PACKED_STRUCT;

    static constexpr auto lock = static_semaphore::allocator_lock;  // use the 0 lock at 0

    allocator() {
        block_header hdr = {
            .in_use = false,
            .size = swapper::get().size(),
            .prev = nullptr,
            .next = nullptr
        };
        std::memcpy(swapper::get().memory(), &hdr, sizeof(hdr));

        swapper::get().sem_put(lock);
    }
    ~allocator() {}

 public:
    // Must be called after swapper initialized!
    static inline auto& get() {
        static allocator instance;
        return instance;
    }

    inline auto alloc(std::size_t size) {
        void* ret;
        swapper::get().sem_get(lock);

        // enforce alignment
        size = (size + alignment - 1) & ~(alignment - 1);

        void* current = swapper::get().memory();
        block_header hdr;
        std::memcpy(&hdr, current, sizeof(hdr));

        size += sizeof(hdr);

        while(true) {
            if (!hdr.in_use && hdr.size >= size) {
                // What is left might not be big enough for another block's meta data.
                if (hdr.size - size < sizeof(hdr)) {
                    size = hdr.size;
                }

                block_header insert = {
                    .in_use = false,
                    .size = hdr.size - size,
                    .prev = current,
                    .next = hdr.next
                };

                hdr.in_use = true;
                hdr.size = size;
                hdr.next = static_cast<std::uint8_t*>(current) + size;

                std::memcpy(current, &hdr, sizeof(hdr));

                if (insert.size > 0) {
                    std::memcpy(hdr.next, &insert, sizeof(hdr));

                    if (insert.next != nullptr) {
                        block_header next;
                        std::memcpy(&next, insert.next, sizeof(hdr));
                        next.prev = hdr.next;
                        std::memcpy(insert.next, &next, sizeof(hdr));
                    }
                }

                ret = static_cast<void*>(static_cast<std::uint8_t*>(current) + sizeof(hdr));
                break;
            }

            if (hdr.next == nullptr) {
                ret = static_cast<void*>(nullptr);
                break;
            }
            current = hdr.next;
            std::memcpy(&hdr, current, sizeof(hdr));
        }

        swapper::get().sem_put(lock);
        return ret;
    }

    inline auto free(void* block) {
        swapper::get().sem_get(lock);

        block_header hdr;
        void* current = static_cast<std::uint8_t*>(block) - sizeof(hdr);
        std::memcpy(&hdr, current, sizeof(hdr));

        hdr.in_use = false;

        if (hdr.next != nullptr) {
            block_header next;
            std::memcpy(&next, hdr.next, sizeof(hdr));
            if (!next.in_use) {
                hdr.size += next.size;
                hdr.next = next.next;
            }
        }

        if (hdr.prev != nullptr) {
            block_header prev;
            std::memcpy(&prev, hdr.prev, sizeof(hdr));
            if (!prev.in_use) {
                hdr.size += prev.size;
                hdr.prev = prev.prev;
                current = hdr.prev;
            }
        }

        std::memcpy(current, &hdr, sizeof(hdr));
        swapper::get().sem_put(lock);
    }
};

}  // details

template<typename T>
struct deleter {
    deleter() noexcept = default;

    template<typename U>
    deleter( const deleter<U>& d ) noexcept {}

    void operator()(T* ptr) const {
        details::allocator::get().free(ptr);
    }
};

template<typename T>
struct deleter<T[]> {
    deleter() noexcept = default;

    template<typename U>
    deleter( const deleter<U[]>& d ) noexcept {}

    template<typename U>
    void operator()(U* ptr) const {
        details::allocator::get().free(ptr);
    }
};


template<typename T, typename... Ts>
static inline auto make_unique(Ts&&... ts) {
    auto memory = details::allocator::get().alloc(sizeof(T));
    if (memory == nullptr) {
        throw std::bad_alloc{};
    }

    return std::unique_ptr<T, deleter<T>>(new (memory) T(std::forward<Ts>(ts)...));
}

template<typename T>
static inline auto make_unique(const std::size_t count) {
    auto memory = details::allocator::get().alloc(sizeof(T) * count);
    if (memory == nullptr) {
        throw std::bad_alloc{};
    }

    return std::unique_ptr<T[], deleter<T[]>>(new (memory) T[count]);
}

template<typename T>
static inline auto make_unique_for_overwrite(const std::size_t count) {
    auto memory = details::allocator::get().alloc(sizeof(T) * count);
    if (memory == nullptr) {
        throw std::bad_alloc{};
    }

    return std::unique_ptr<T[], deleter<T[]>>(memory);
}

}  // namespace tDSM
