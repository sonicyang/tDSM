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
        memory[0] = i;
        i++;
        sleep(1);
    }

    /*
    swapper.page_out_page(0);
    swapper.set_frame_state_shared(0);

    memory[0] = 0x16;
    spdlog::info("{:x}", memory[0]);

    spdlog::info("{:x}", memory[0]);
    memory[0] = 0x32;
    spdlog::info("{:x}", memory[0]);

    swapper.page_out_page(0);
    swapper.set_frame_state_shared(0);

    memory[0] = 0x64;
    spdlog::info("{:x}", memory[0]);
    */

    sleep(100000);
    return 0;
}
