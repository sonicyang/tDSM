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

#include "allocator.hpp"
#include "barrier.hpp"
#include "configs.hpp"
#include "node.hpp"
#include "swapper.hpp"
#include "rpc_adapter.hpp"

void memtest(tDSM::barrier& barrier, const int iter);
__attribute__((noinline))
void memtest(tDSM::barrier& barrier, const int iter) {
    auto& swapper = tDSM::swapper::get();
    const auto my_id = swapper.get_id();
    auto memory = reinterpret_cast<std::uint64_t*>(swapper.memory() + page_size);
    memory[my_id] = 0;
    std::uint64_t local_count = 0;

    barrier();

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

    constexpr auto testers = 3;
    constexpr auto iter = 65535;

    const auto barrier = tDSM::make_unique<tDSM::barrier>(testers);

    std::vector<std::thread> threads(testers - 1);

    for (auto i = 2u; auto& t : threads) {
        swapper.wait_for_peer(i);
        t = std::thread([i, &barrier] {
            rpc_memtest.remote = i;
            rpc_memtest(*barrier, iter);
        });
        i++;
    }

    memtest(*barrier, iter);

    for (auto& t : threads) {
        t.join();
    }

    spdlog::info("OK");

    return 0;
}
