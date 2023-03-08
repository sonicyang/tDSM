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

#include "configs.hpp"
#include "swapper.hpp"
#include "semaphore.hpp"
#include "utils/logging.hpp"

namespace tDSM {

class barrier {
 public:
    const std::size_t n_threads;

    barrier(const std::size_t n_threads_) : n_threads(n_threads_) {}

    void operator()() {
        this->mutex.get();
        if (count == n_threads) {
            count = 1;
            this->chain.get();
        } else {
            count += 1;
        }
        const auto current_count = count;
        this->mutex.put();

        if (current_count == this->n_threads) {
            this->chain.put();
            return;
        }

        this->chain.get();
        this->chain.put();
    }

 private:
    semaphore mutex{1};
    semaphore chain{0};
    std::size_t count{0};
};

}  // namespace tDSM
