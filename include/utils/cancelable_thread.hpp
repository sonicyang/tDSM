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
        if (this->evtfd.get() < 0) {
            spdlog::error("Failed to get eventfd: {}", strerror(errno));
            abort();
        }
    }

    cancelable_thread& operator=(std::thread&& ts) {
        this->stopped.store(false, std::memory_order_seq_cst);
        this->thread = std::forward<std::thread>(ts);
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
