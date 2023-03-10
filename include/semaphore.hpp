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
#include "utils/logging.hpp"

namespace tDSM {

struct semaphore {
    using type = std::uintptr_t;

    const type address;

    semaphore(const std::size_t initial_count) : address(swapper::get().make_sem(initial_count)) {}

    void put() {
        tDSM::swapper::get().sem_put(this->address);
    }

    void get() {
        tDSM::swapper::get().sem_get(this->address);
    }
};

}  // namespace tDSM
