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
#include <memory>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "utils/logging.hpp"

namespace tDSM {

std::shared_ptr<spdlog::logger> logger = spdlog::stderr_color_mt("tDSM");
std::shared_ptr<spdlog::logger> sem_logger = spdlog::stderr_color_mt(" SEM");

}  // tDSM
