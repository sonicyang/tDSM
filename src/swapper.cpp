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

#include <string>
#include <iostream>

#include "swapper.hpp"

std::uint8_t rdma_memory[rdma_size] __attribute__((section(".rdma"), aligned(page_size)));

/**
 * We need to initialize the swapper using command line arguments.
 * However, the command line arguments parser, absl_flags initializes during dynamic initialization.
 * __attribute__((constructor)) executes in static initialization which is before the dynamic initialization.
 * Using the flags in __attribute__((constructor)) is invalid.
 * We use a static variable to force the initialization of swapper in dynamic initialization.
 * Nonetheless, we still need to use __attribute__((constructor)) to capture the argc and argv.
 * The values are temporary captured in 2 global variables.
 */

ABSL_FLAG(bool, directory, false, "Is directory node");
ABSL_FLAG(bool, compression, false, "Use lz4 compression");
ABSL_FLAG(std::string, directory_addr, "127.0.0.1", "IP address of directory");
ABSL_FLAG(std::string, local_addr, "127.0.0.1", "IP address of this node");
ABSL_FLAG(std::uint16_t, local_port, 7000, "TCP port for communication");

namespace tDSM {

namespace packet {
    // dirity hack to get around the cyclic dependency problem
    std::size_t my_id;
}  // namespace

namespace detail {
static int argc;
static char** argv;

struct StartUpInit {
    StartUpInit() {
        absl::SetProgramUsageMessage(fmt::format("Usage: {} master=<true/false> ip=<IP> port=<Port>\n", argv[0]));

        const auto args = absl::ParseCommandLine(argc, argv);

        // Make sure the directory service is started first
        const auto is_master = absl::GetFlag(FLAGS_directory);
        if (is_master) {
            const auto use_compression = absl::GetFlag(FLAGS_compression);
            master_node::get().initialize(use_compression);
        }

        // First call, initialized!
        const auto directory_addr = absl::GetFlag(FLAGS_directory_addr);
        const auto local_addr = absl::GetFlag(FLAGS_local_addr);
        const auto local_port = absl::GetFlag(FLAGS_local_port);
        swapper::get().initialize(is_master, directory_addr, local_addr, local_port);
    }
};
}  // detail

}  // tDSM

// argc and argv is an gnu extension
static __attribute__((constructor(102))) inline auto swapper_initialization(const int argc, char* argv[]) {
    tDSM::detail::argc = argc;
    tDSM::detail::argv = argv;
}

static tDSM::detail::StartUpInit startupinit;
