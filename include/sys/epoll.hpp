#pragma once
#include <unistd.h>
#include <sys/epoll.h>

#include <atomic>
#include <optional>
#include <vector>
#include <tuple>

#include "fd.hpp"

class Epoll : public FileDescriptor {
 public:

    template<typename... Ts>
    Epoll(const Ts&... ts) : FileDescriptor(::epoll_create(16)) {
        if (this->fd < 0) {
            spdlog::error("Failed to create epoll fd");
        }

        /* add the fds */
        (this->add_fd(ts), ...);
    }

    Epoll(const Epoll&) = delete;
    Epoll& operator=(const Epoll&) = delete;
    Epoll(Epoll&&) = delete;
    Epoll& operator=(Epoll&&) = delete;

    std::atomic_int nfds{0};

    inline void add_fd(const FileDescriptor& fd_to_add, enum EPOLL_EVENTS events = EPOLLIN) {
       struct epoll_event ev;
       ev.events = events;
       ev.data.fd = fd_to_add.get();
       if (::epoll_ctl(this->fd, EPOLL_CTL_ADD, fd_to_add.get(), &ev) == -1) {
           spdlog::error("epoll_ctl: failed to add {}: {}", fd_to_add.get(), strerror(errno));
           abort();
       }
       this->nfds.fetch_add(1, std::memory_order_release);
    }

    inline void delete_fd(const FileDescriptor& fd_to_delete) {
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

    static inline bool check_fd_in_result(const std::vector<struct epoll_event>& events, const FileDescriptor& fd_to_check, enum EPOLL_EVENTS to_check = EPOLLIN) {
        for (const auto& event : events) {
            if (event.data.fd == fd_to_check.get()) {
                return !!(event.events & to_check);
            }
        }
        return false;
    }
};
