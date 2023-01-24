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

#include "node.hpp"
#include "swapper.hpp"

int main() {
    auto& swapper = tDSM::swapper::get();
    auto memory = swapper.memory();

    //std::uint8_t i = 0;
    //while(true) {
        //spdlog::info("{:x} {:x}", memory[0], memory[1]);
        //memory[1] = i;
        //i++;
        //sleep(1);
    //}

    swapper.sem_get(reinterpret_cast<std::uintptr_t>(&memory[1]));

    while(true) {
        swapper.sem_get(reinterpret_cast<std::uintptr_t>(&memory[0]));
        spdlog::info("{}", memory[0]);
        memory[0] = memory[0] + 1;
        swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[0]));
        sleep(1);
        if (memory[0] == 80) {
            swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[1]));
        }
    }

    sleep(5);
    return 0;
}
