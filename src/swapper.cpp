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

#include <atomic>
#include <string>
#include <iostream>

#include "swapper.hpp"

std::uint8_t rdma_memory[rdma_size] __attribute__((section(".rdma"), aligned(page_size)));

namespace tDSM::packet {
    // dirity hack to get around the cyclic dependency problem
    std::size_t my_id;

}  // namespace tDSM::packet
