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
#include <sys/eventfd.h>
#include <poll.h>

#include <array>
#include <atomic>
#include <thread>
#include <utility>
#include <spdlog/spdlog.h>

#include "sys/fd.hpp"

namespace tDSM::utils {

struct cancelable_thread {
    template<typename... Ts>
    cancelable_thread(Ts&&... ts) : evtfd(eventfd(0, 0)), thread(std::forward<Ts>(ts)...) {
        // Initialize eventfd, this is for unblocking the poll
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(this->evtfd.get() < 0, "Failed to get eventfd");
    }

    cancelable_thread& operator=(std::thread&& ts) {
        this->stopped.store(false, std::memory_order_seq_cst);
        this->thread = std::forward<std::thread>(ts);
        return *this;
    }

    cancelable_thread(cancelable_thread&& ts) :
        evtfd(std::move(ts.evtfd)),
        thread(std::move(ts.thread))
    {
    }

    cancelable_thread& operator=(cancelable_thread&& ts) {
        this->stopped.store(false, std::memory_order_seq_cst);
        this->evtfd = std::move(ts.evtfd);
        this->thread = std::move(ts.thread);
        return *this;
    }

    virtual ~cancelable_thread() {
        this->cancel_and_join();
    }

    auto inline wakeup_poll() {
        /* Release the swapper thread from poll */
        constexpr auto val = 1ULL;
        assert(write(this->evtfd.get(), &val, sizeof(val)) == sizeof(val));
    }

    inline void cancel_and_join() {
        this->stopped.store(true, std::memory_order_seq_cst);
        this->wakeup_poll();
        if (this->thread.joinable()) {
            this->thread.join();
        }
    }

    template<typename... Ts>
    auto inline make_pollfds_for_read(const Ts&... fds) {
        return std::array<struct pollfd, sizeof...(Ts) + 1>{{
            {
                .fd = fds.get(),
                .events = POLLIN,
                .revents = 0
            }...,
            {
                .fd = this->evtfd.get(),
                .events = POLLIN,
                .revents = 0
            }
        }};
    }

    sys::file_descriptor evtfd;
    std::thread thread;
    std::atomic_bool stopped{false};
};

}  // namespace tDSM::utils
