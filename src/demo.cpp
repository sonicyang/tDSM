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
        //memory[0] = i;
        //i++;
        //sleep(1);
    //}

    swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[0]));

    bool stopped = false;
    while(true) {
        swapper.sem_get(reinterpret_cast<std::uintptr_t>(&memory[0]));
        spdlog::info("{}", memory[0]);
        memory[0] = memory[0] + 1;
        swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[0]));
        sleep(1);
        if (memory[0] == 20) {
            swapper.sem_put(reinterpret_cast<std::uintptr_t>(&memory[1]));
        } else if (memory[0] > 40 && !stopped) {
            stopped = true;
            swapper.sem_get(reinterpret_cast<std::uintptr_t>(&memory[1]));
        }
    }

    sleep(100000);
    return 0;
}
