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
#include <sys/epoll.h>

#include <atomic>
#include <optional>
#include <vector>
#include <tuple>

#include "fd.hpp"

namespace tDSM::sys {

class epoll : public file_descriptor {
 public:

    template<typename... Ts>
    epoll(const Ts&... ts) : file_descriptor(::epoll_create(16)) {
        if (this->fd < 0) {
            spdlog::error("Failed to create epoll fd");
        }

        /* add the fds */
        (this->add_fd(ts), ...);
    }

    epoll(const epoll&) = delete;
    epoll& operator=(const epoll&) = delete;
    epoll(epoll&&) = delete;
    epoll& operator=(epoll&&) = delete;

    std::atomic_int nfds{0};

    inline void add_fd(const file_descriptor& fd_to_add, enum EPOLL_EVENTS events = EPOLLIN) {
       struct epoll_event ev;
       ev.events = events;
       ev.data.fd = fd_to_add.get();
       if (::epoll_ctl(this->fd, EPOLL_CTL_ADD, fd_to_add.get(), &ev) == -1) {
           spdlog::error("epoll_ctl: failed to add {}: {}", fd_to_add.get(), strerror(errno));
           abort();
       }
       this->nfds.fetch_add(1, std::memory_order_release);
    }

    inline void delete_fd(const file_descriptor& fd_to_delete) {
       this->nfds.fetch_sub(1, std::memory_order_acquire);
       if (::epoll_ctl(this->fd, EPOLL_CTL_DEL, fd_to_delete.get(), nullptr) == -1) {
           spdlog::error("epoll_ctl: failed to delete {}: {}", fd_to_delete.get(), strerror(errno));
           abort();
       }
    }

    inline auto wait(const int timeout = -1) const {
        const auto maxevents = this->nfds.load(std::memory_order_acquire);
        std::vector<struct epoll_event> ret(static_cast<std::size_t>(maxevents));
        const auto count = ::epoll_wait(this->fd, &ret[0], maxevents, timeout);
        return std::make_tuple(count, ret);
    }

    static inline bool check_fd_in_result(const std::vector<struct epoll_event>& events, const file_descriptor& fd_to_check, enum EPOLL_EVENTS to_check = EPOLLIN) {
        for (const auto& event : events) {
            if (event.data.fd == fd_to_check.get()) {
                return !!(event.events & to_check);
            }
        }
        return false;
    }
};

}  // namespace tDSM::sys
