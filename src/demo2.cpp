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

    while(true) {
        swapper.template lock<std::uint8_t>(&memory[0]);
        spdlog::info("{:x}", memory[0]);
        memory[0] = memory[0] + 1;
        swapper.template unlock<std::uint8_t>(&memory[0]);
        sleep(1);
    }

    sleep(5);
    return 0;
}
