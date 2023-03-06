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
#include <numeric>
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

void vector_add(const std::size_t testers, int& sum, const int* vec, const std::size_t start, const std::size_t end);
__attribute__((noinline))
void vector_add(const std::size_t testers, int& sum, const int* vec, const std::size_t start, const std::size_t end) {
    barrier(testers);

    spdlog::info("Thread Started");

    int lsum = 0;
    for (std::size_t i = start; i < end; ++i) {
        lsum += vec[i];
    }

    spdlog::info("Sum end {} - {} = {}", start, end, lsum);

    auto& swapper = tDSM::swapper::get();
    const auto memory = swapper.memory();
    static const auto sum_mutex = reinterpret_cast<std::uintptr_t>(&memory[2]);

    if (swapper.get_id() == 1) {
        swapper.sem_put(sum_mutex);
    }

    swapper.sem_get(sum_mutex);
    sum += lsum;
    swapper.sem_put(sum_mutex);

    spdlog::info("Thread end");
}

static tDSM::rpc::simple_rpc_adapter<vector_add> rpc_vector_add;

int main() {
    tDSM::initialize();
    auto& swapper = tDSM::swapper::get();

    constexpr auto testers = 3;

    barrier_init();

    std::vector<std::thread> threads(testers - 1);

    constexpr auto size = 11;
    auto vec = new(swapper.memory() + page_size) int[size]();
    auto sum = new(swapper.memory() + page_size * 2) int(0);

    std::iota(vec, vec + size, 0);

    const auto slice_size = size / testers + 1;

    for (auto i = 2u; auto& t : threads) {
        swapper.wait_for_peer(i);
        t = std::thread([i, sum, vec, slice_size] {
            rpc_vector_add.remote = i;
            rpc_vector_add(testers, *sum, vec, (i - 1) * slice_size, std::min(i * slice_size, static_cast<decltype(i * slice_size)>(size)));
        });
        i++;
    }

    vector_add(testers, *sum, vec, 0, std::min(slice_size, size));

    for (auto& t : threads) {
        t.join();
    }

    spdlog::info("Sum: {}", *sum);

    return 0;
}
