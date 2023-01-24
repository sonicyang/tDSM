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

class semaphore {
 public:
    using type = std::uintptr_t;

    template<typename T>
    semaphore(volatile T* ptr_) : ptr(reinterpret_cast<std::uintptr_t>(ptr_)) {}

    void put() {
        auto& swapper = tDSM::swapper::get();
        swapper.sem_put(this->ptr);
    }

    void get() {
        auto& swapper = tDSM::swapper::get();
        swapper.sem_get(this->ptr);
    }

 private:
    type ptr;
};

}  // namespace tDSM
