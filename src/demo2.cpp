#include <spdlog/spdlog.h>
#include <cstdint>

#include "node.hpp"
#include "swapper.hpp"

int main() {
    auto& swapper = Swapper::get();
    auto memory = swapper.memory();

    std::uint8_t i = 0;
    while(true) {
        spdlog::info("{:x} {:x}", memory[0], memory[1]);
        memory[1] = i;
        i++;
        sleep(1);
    }

    sleep(5);
    return 0;
}
