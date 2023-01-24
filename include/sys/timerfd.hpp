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

#pragma once
#include <unistd.h>
#include <sys/timerfd.h>

#include <atomic>
#include <optional>
#include <vector>
#include <tuple>

#include "sys/fd.hpp"
#include "utils/logging.hpp"

namespace tDSM::sys {

class timer_fd : public file_descriptor {
 public:
    timer_fd() : file_descriptor(::timerfd_create(CLOCK_MONOTONIC, 0)) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(this->fd < 0, "Failed to create timerfd");
    }

    timer_fd(const timer_fd&) = delete;
    timer_fd& operator=(const timer_fd&) = delete;
    timer_fd(timer_fd&&) = delete;
    timer_fd& operator=(timer_fd&&) = delete;

    inline void one_shot(const struct timespec& deadline, const int flags = 0) {
        struct itimerspec utmr = {
            .it_interval = timespec{},
            .it_value = deadline
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::timerfd_settime(this->fd, flags, &utmr, nullptr),
            "Failed to arm the timerfd");
    }

    inline void periodic(const struct timespec& start, const struct timespec& period, const int flags = 0) {
        struct itimerspec utmr = {
            .it_interval = start,
            .it_value = period
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::timerfd_settime(this->fd, flags, &utmr, nullptr),
            "Failed to arm the timerfd");
    }

    inline void cancel() {
        struct itimerspec utmr = {
            .it_interval = timespec{},
            .it_value = timespec{}
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::timerfd_settime(this->fd, 0, &utmr, nullptr),
            "Failed to disarm the timerfd");
    }
};

}  // namespace tDSM::sys
