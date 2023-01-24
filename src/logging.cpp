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

#include <cstdlib>

#include <spdlog/spdlog.h>

static __attribute__((constructor(101))) inline auto init_logging() {
    spdlog::set_pattern("[%H:%M:%S] [thread %t] [%^%L%$] %v");

    const auto level = std::getenv("LOGLEVEL");
    if (level) {
        switch(std::stol(level)) {
            case 0:
                spdlog::set_level(spdlog::level::off);
                break;
            case 1:
                spdlog::set_level(spdlog::level::err);
                break;
            case 2:
                spdlog::set_level(spdlog::level::warn);
                break;
            case 4:
                spdlog::set_level(spdlog::level::debug);
                break;
            case 5:
                spdlog::set_level(spdlog::level::trace);
                break;
            case 3:
            default:
                spdlog::set_level(spdlog::level::info);
        }
    }
}
