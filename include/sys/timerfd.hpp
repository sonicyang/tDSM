#pragma once
#include <unistd.h>
#include <sys/timerfd.h>

#include <atomic>
#include <optional>
#include <vector>
#include <tuple>

#include "sys/fd.hpp"
#include "utils/logging.hpp"

class TimerFd : public FileDescriptor {
 public:
    TimerFd() : FileDescriptor(::timerfd_create(CLOCK_MONOTONIC, 0)) {
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(this->fd < 0, "Failed to create timerfd");
    }

    TimerFd(const TimerFd&) = delete;
    TimerFd& operator=(const TimerFd&) = delete;
    TimerFd(TimerFd&&) = delete;
    TimerFd& operator=(TimerFd&&) = delete;

    inline void one_shot(const struct timespec& deadline, const int flags = 0) {
        struct itimerspec utmr = {
            .it_interval = timespec{},
            .it_value = deadline
        };
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::timerfd_settime(this->fd, flags, &utmr, nullptr),
            "Failed to arm the timerfd");
    }

    inline void periodic(const struct timespec& start, const struct timespec& period, const int flags = 0) {
        struct itimerspec utmr = {
            .it_interval = start,
            .it_value = period
        };
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::timerfd_settime(this->fd, flags, &utmr, nullptr),
            "Failed to arm the timerfd");
    }

    inline void cancel() {
        struct itimerspec utmr = {
            .it_interval = timespec{},
            .it_value = timespec{}
        };
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::timerfd_settime(this->fd, 0, &utmr, nullptr),
            "Failed to disarm the timerfd");
    }
};
