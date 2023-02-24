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

#include "configs.hpp"
#include "node.hpp"
#include "swapper.hpp"
#include "rpc_adapter.hpp"

int add2(int a, int b);
int add2(int a, int b) {
    spdlog::info("{} + {}", a, b);
    return a + b;
}

void increase(int& a);
void increase(int& a) {
    a++;
}

static tDSM::rpc::simple_rpc_adapter<add2> rpc_add2;
static tDSM::rpc::simple_rpc_adapter<increase> rpc_increase;

int main(const int argc, char* argv[]) {
    tDSM::initialize(argc, argv);
    tDSM::swapper::get().wait_for_peer(2);

    auto& swapper = tDSM::swapper::get();
    auto memory = swapper.memory();

    int a = 1;
    int b = 2;
    spdlog::info("Local {}", add2(a, b));

    rpc_add2.remote = 2;

    spdlog::info("Remote {}", rpc_add2(a, b));

    int *inc = reinterpret_cast<int*>(memory);

    increase(*inc);
    spdlog::info("Inc Local {} {}", *inc, static_cast<void*>(inc));

    rpc_increase.remote = 2;

    rpc_increase(*inc);
    spdlog::info("Inc Remote {}", *inc);

    //std::uint8_t i = 0;
    //while(true) {
        //spdlog::info("{:x} {:x}", memory[0], memory[1]);
        //memory[0] = i;
        //i++;
        //usleep(1000);
        ////assert(tDSM::packet::my_id != 0);
    //}

    //swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[0]));

    //bool stopped = false;
    //while(true) {
        //swapper.sem_get(reinterpret_cast<std::uintptr_t>(&memory[0]));
        //spdlog::info("{}", memory[0]);
        //memory[0] = memory[0] + 1;
        //swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[0]));
        //sleep(1);
        //if (memory[0] == 20) {
            //swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[1]));
        //} else if (memory[0] > 40 && !stopped) {
            //stopped = true;
            //swapper.sem_get(reinterpret_cast<std::uintptr_t>(&memory[1]));
        //}
    //}

    //sleep(100000);
    return 0;
}
