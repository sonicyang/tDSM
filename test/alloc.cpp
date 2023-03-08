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

#include <spdlog/spdlog.h>
#include <cstdint>

#include "allocator.hpp"
#include "configs.hpp"
#include "node.hpp"
#include "swapper.hpp"
#include "rpc_adapter.hpp"

int main() {
    tDSM::initialize();

    auto ptr1 = tDSM::details::allocator::get().alloc(128);
    spdlog::info("Allocated {}", ptr1);

    auto ptr2 = tDSM::details::allocator::get().alloc(64);
    spdlog::info("Allocated {}", ptr2);

    auto ptr3 = tDSM::details::allocator::get().alloc(4096);
    spdlog::info("Allocated {}", ptr3);

    tDSM::details::allocator::get().free(ptr2);
    spdlog::info("Free {}", ptr2);

    auto ptr4 = tDSM::details::allocator::get().alloc(32);
    spdlog::info("Allocated {}", ptr4);

    auto ptr5 = tDSM::details::allocator::get().alloc(32);
    spdlog::info("Allocated {}", ptr5);

    tDSM::details::allocator::get().free(ptr4);
    spdlog::info("Free {}", ptr4);

    tDSM::details::allocator::get().free(ptr3);
    spdlog::info("Free {}", ptr3);

    tDSM::details::allocator::get().free(ptr1);
    spdlog::info("Free {}", ptr1);

    auto ptr6 = tDSM::details::allocator::get().alloc(4096);
    spdlog::info("Allocated {}", ptr6);

    auto ptr7 = tDSM::details::allocator::get().alloc(4096);
    spdlog::info("Allocated {}", ptr7);

    return 0;
}
