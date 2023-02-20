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
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/userfaultfd.h>

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include <spdlog/spdlog.h>

#include "sys/fd.hpp"
#include "utils/logging.hpp"

namespace tDSM::sys {

static inline auto userfaultfd(int flags) {
    return static_cast<int>(syscall(__NR_userfaultfd, flags));
}

struct page_fault {
    const bool is_write;
    const bool is_missing;
    const bool is_write_protect;
    const bool is_minor;
    void* const address;
    const pid_t tid;
};

class user_fault_fd : public file_descriptor {
 public:
    user_fault_fd() : file_descriptor(userfaultfd(O_NONBLOCK)) {
        // Initialize userfaulefd, no blocking on normal read
        if (this->fd < 0) {
            spdlog::error("Failed to create user_fault_fd: {}", strerror(errno));
            abort();
        }

        uffdio_api api_msg = {
            .api = UFFD_API,
            .features = UFFD_FEATURE_THREAD_ID,
            .ioctls = 0
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_API, &api_msg),
            "Failed to initialize the UFFD API"
        );
    }

    static inline auto flag_to_string(const std::uint64_t flags) {
        std::string str = "";
        if (flags & UFFD_PAGEFAULT_FLAG_WRITE) {
            str += " WRITE";
        }
        if (flags & UFFD_PAGEFAULT_FLAG_WP) {
            str += " WP";
        }
        if (flags & UFFD_PAGEFAULT_FLAG_MINOR) {
            str += " MINOR";
        }
        return str;
    }

    inline auto watch(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd start watch pages @ {}, length: {}", addr, len);
        uffdio_register register_msg = {
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_REGISTER_MODE_WP | UFFDIO_REGISTER_MODE_MINOR,
            .ioctls = 0
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_REGISTER, &register_msg),
            "Failed to watch pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }

    inline auto stop_watch(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd stop watch pages @ {}, length: {}", addr, len);
        uffdio_range range_msg = {
            .start = reinterpret_cast<std::uintptr_t>(addr),
            .len = len
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_UNREGISTER, &range_msg),
            "Failed to stop watch pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }

    inline auto read() {
        uffd_msg msg;
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ::read(this->fd, &msg, sizeof(msg)) != sizeof(msg),
            "Failed to read a message from UFFD API"
        );

        // Only handle PAGE_FAULT, user should not fork or modify the rdma memory map!
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            msg.event != UFFD_EVENT_PAGEFAULT,
            "Event not supported!"
        );

        spdlog::debug("user_fault_fd captured a page fault from {} @ {}, flags:{}",
            static_cast<pid_t>(msg.arg.pagefault.feat.ptid),
            reinterpret_cast<void*>(msg.arg.pagefault.address),
            flag_to_string(msg.arg.pagefault.flags));

        return page_fault{
            .is_write         = !!(msg.arg.pagefault.flags & (UFFD_PAGEFAULT_FLAG_WP | UFFD_PAGEFAULT_FLAG_WRITE)),
            .is_missing       = !(msg.arg.pagefault.flags & (UFFD_PAGEFAULT_FLAG_WP | UFFD_PAGEFAULT_FLAG_MINOR)),
            .is_write_protect = !!(msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP),
            .is_minor         = !!(msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_MINOR),
            .address          = reinterpret_cast<void*>(msg.arg.pagefault.address),
            .tid              = static_cast<pid_t>(msg.arg.pagefault.feat.ptid)
        };
    }

    inline auto write_protect(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd write protect pages @ {}, length: {}", addr, len);
        uffdio_writeprotect wp_msg {
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_WRITEPROTECT_MODE_WP
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_WRITEPROTECT, &wp_msg),
            "Failed to write protect pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }

    inline auto write_unprotect(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd write unprotect pages @ {}, length: {}", addr, len);

        uffdio_writeprotect wp_msg {
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_WRITEPROTECT_MODE_DONTWAKE
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_WRITEPROTECT, &wp_msg),
            "Failed to write unprotect pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }

    inline auto zero(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd zero a page @ {}, length: {}", addr, len);

        uffdio_zeropage zeropg{
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_ZEROPAGE_MODE_DONTWAKE,
            .zeropage = 0
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_ZEROPAGE, &zeropg),
            "Failed to zero clear pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }

    inline auto continue_(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd continue a page @ {}, length: {}", addr, len);

        uffdio_continue cont{
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_CONTINUE_MODE_DONTWAKE,
            .mapped = 0
        };
        const auto ret = ioctl(this->fd, UFFDIO_CONTINUE, &cont);
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ret && errno != EEXIST,  // skip if the mapping exists
            "Failed to solve minor fault pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }

    inline auto wake(void* const addr, const std::size_t len) {
        spdlog::debug("user_fault_fd wake thread waiting on page @ {}, length: {}", addr, len);

        uffdio_range range_msg = {
            .start = reinterpret_cast<std::uintptr_t>(addr),
            .len = len
        };
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ioctl(this->fd, UFFDIO_WAKE, &range_msg),
            "Failed to wait thread waiting on pages @ {}, length: {} with UFFD API",
            addr, len
        );
    }
};

}  // tDSM::sys
