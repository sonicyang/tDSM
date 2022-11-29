#include <string>
#include <iostream>

#include "swapper.hpp"

volatile std::uint8_t rdma_memory[rdma_size] __attribute__((section(".rdma"), aligned(page_size)));

/**
 * We need to initialize the swapper using command line arguments.
 * However, the command line arguments parser, absl_flags initializes during dynamic initialization.
 * __attribute__((constructor)) executes in static initialization which is before the dynamic initialization.
 * Using the flags in __attribute__((constructor)) is invalid.
 * We use a static variable to force the initialization of swapper in dynamic initialization.
 * Nonetheless, we still need to use __attribute__((constructor)) to capture the argc and argv.
 * The values are temporary captured in 2 global variables.
 */

ABSL_FLAG(bool, master, false, "Is master node");
ABSL_FLAG(std::string, ip, "127.0.0.1", "IP address of master");
ABSL_FLAG(std::uint16_t, port, 7000, "TCP port for communication");

std::string master_ip;
std::uint16_t my_port;

namespace detail {
static int argc;
static char** argv;

struct StartUpInit {
    StartUpInit() {
        absl::SetProgramUsageMessage(fmt::format("Usage: {} master=<true/false> ip=<IP> port=<Port>\n", argv[0]));
        const auto args = absl::ParseCommandLine(argc, argv);
        ::master_ip = absl::GetFlag(FLAGS_ip);
        ::my_port = absl::GetFlag(FLAGS_port);
        // First call, initialized!
        Swapper::get(absl::GetFlag(FLAGS_master));
    }
};
}  // detail

// argc and argv is an gnu extension
static __attribute__((constructor(102))) inline auto swapper_initialization(const int argc, char* argv[]) {
    detail::argc = argc;
    detail::argv = argv;
}

static detail::StartUpInit startupinit;
