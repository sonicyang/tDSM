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

#include "fmt/core.h"

#include "node.hpp"
#include "swapper.hpp"

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

template<typename T>
static inline T getenv_or_default(const char* env, const T def) {
    const auto var = std::getenv(env);
    if (var) {
        if constexpr (std::is_same_v<bool, T>) {
            return !!(std::stol(var));
        } else if constexpr (std::is_same_v<std::string, T>) {
        } else if constexpr (std::is_unsigned_v<T>) {
            return static_cast<T>(std::stoul(var));
        } else if constexpr (std::is_integral_v<T>) {
            return static_cast<T>(std::stol(var));
        } else {
            return def;
        }
    } else {
        return def;
    }
}

void initialize() {
    init_logging();

    const auto is_directory   = getenv_or_default("DIRECTORY", false);
    const auto compression    = getenv_or_default("COMPRESSION", true);
    const auto rpc_server     = getenv_or_default("RPC_SERVER", false);
    const auto directory_addr = getenv_or_default("DIRECTORY_ADDRESS", "localhost");
    const auto local_addr     = getenv_or_default("ADDRESS", "localhost");
    const auto local_port     = getenv_or_default("PORT", std::uint16_t{7000});

    // Make sure the directory service is started first
    if (is_directory) {
        master_node::get().initialize(compression);
    }

    // First call, initialized!
    swapper::get().initialize(is_directory, directory_addr, local_addr, local_port);

    if (rpc_server) {
        while(true) {
            sleep(86400);  // Temp solution to make main stuck
        }
    }
}

}  // namespace tDSM
