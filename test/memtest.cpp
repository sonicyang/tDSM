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

#include <algorithm>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>
#include <cstdint>

#include "configs.hpp"
#include "node.hpp"
#include "swapper.hpp"
#include "rpc_adapter.hpp"

static inline auto barrier_init() {
    auto& swapper = tDSM::swapper::get();
    const auto memory = swapper.memory();
    static const auto mutex = reinterpret_cast<std::uintptr_t>(&memory[0]);
    static const auto count = reinterpret_cast<std::size_t*>(&memory[8]);
    *count = 0;
    swapper.sem_put(mutex);
}

static inline auto barrier(const std::size_t n_threads) {
    auto& swapper = tDSM::swapper::get();
    const auto memory = swapper.memory();
    static const auto mutex = reinterpret_cast<std::uintptr_t>(&memory[0]);
    static const auto barrier = reinterpret_cast<std::uintptr_t>(&memory[1]);
    static const auto count = reinterpret_cast<std::size_t*>(&memory[8]);

    spdlog::info("Barrier enter");
    swapper.sem_get(mutex);
    if (*count == n_threads) {
        *count = 1;
    } else {
        *count += 1;
    }
    const auto current_count = *count;
    swapper.sem_put(mutex);

    spdlog::info("Barrier Count {}", current_count);

    if (current_count == n_threads) {
        swapper.sem_put(barrier);
    }

    swapper.sem_get(barrier);
    swapper.sem_put(barrier);
}


void memtest(const std::size_t testers, const int iter);
__attribute__((noinline))
void memtest(const std::size_t testers, const int iter) {
    auto& swapper = tDSM::swapper::get();
    const auto my_id = swapper.get_id();
    auto memory = reinterpret_cast<std::uint64_t*>(swapper.memory() + page_size);
    memory[my_id] = 0;
    std::uint64_t local_count = 0;

    barrier(testers);

    spdlog::info("Thread {} started", my_id);

    for (auto i = 0; i < iter; i++) {
        if (memory[my_id] != local_count) {
            spdlog::error("Failed! dsm: {}, local: {}", memory[my_id], local_count);
        }
        memory[my_id]++;
        local_count++;
    }
}

static tDSM::rpc::simple_rpc_adapter<memtest> rpc_memtest;

int main() {
    tDSM::initialize();
    auto& swapper = tDSM::swapper::get();

    constexpr auto testers = 2;
    constexpr auto iter = 4096;

    barrier_init();

    std::vector<std::thread> threads(testers - 1);

    for (auto i = 2u; auto& t : threads) {
        swapper.wait_for_peer(i);
        t = std::thread([i] {
            rpc_memtest.remote = i;
            rpc_memtest(testers, iter);
        });
        i++;
    }

    memtest(testers, iter);

    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
