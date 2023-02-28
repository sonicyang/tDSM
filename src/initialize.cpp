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

#include <unistd.h>

#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "fmt/core.h"

#include "node.hpp"
#include "swapper.hpp"

ABSL_FLAG(bool          , directory       , false       , "Is directory node");
ABSL_FLAG(bool          , compression     , false       , "Use lz4 compression");
ABSL_FLAG(bool          , rpc_server_only , false       , "Act as a rpc server only");
ABSL_FLAG(std::string   , directory_addr  , "127.0.0.1" , "IP address of directory");
ABSL_FLAG(std::string   , local_addr      , "127.0.0.1" , "IP address of this node");
ABSL_FLAG(std::uint16_t , local_port      , 7000        , "TCP port for communication");

namespace tDSM {

static inline auto init_logging() {
    logger->set_pattern("[%H:%M:%S] [%n] [thread %t] [%^%L%$] %v");

    const auto level = std::getenv("LOGLEVEL");
    if (level) {
        switch(std::stol(level)) {
            case 0:
                logger->set_level(spdlog::level::off);
                break;
            case 1:
                logger->set_level(spdlog::level::err);
                break;
            case 2:
                logger->set_level(spdlog::level::warn);
                break;
            case 3:
                logger->set_level(spdlog::level::info);
                break;
            case 4:
                logger->set_level(spdlog::level::debug);
                break;
            case 5:
                logger->set_level(spdlog::level::trace);
                break;
            default:
                logger->set_level(spdlog::level::warn);
        }
    } else {
        logger->set_level(spdlog::level::warn);
    }
}


void initialize(const int argc, char* argv[]) {
    init_logging();

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

    if (absl::GetFlag(FLAGS_rpc_server_only)) {
        while(true) {
            sleep(86400);  // Temp solution to make main stuck
        }
    }
}

}  // namespace tDSM
