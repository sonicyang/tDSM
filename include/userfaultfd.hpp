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

#include "fd.hpp"

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

class UserFaultFd : public FileDescriptor {
 public:
    UserFaultFd() : FileDescriptor(userfaultfd(O_NONBLOCK)) {
        // Initialize userfaulefd, no blocking on normal read
        if (this->fd < 0) {
            spdlog::error("Failed to create UserFaultFd: {}", strerror(errno));
            abort();
        }

        uffdio_api api_msg = {
            .api = UFFD_API,
            .features = UFFD_FEATURE_THREAD_ID,
            .ioctls = 0
        };
        assert(!ioctl(this->fd, UFFDIO_API, &api_msg));
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

    inline auto watch(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd start watch pages @ {}, length: {}", const_cast<void*>(addr), len);
        uffdio_register register_msg = {
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_REGISTER_MODE_WP | UFFDIO_REGISTER_MODE_MINOR,
            .ioctls = 0
        };
        assert(!ioctl(this->fd, UFFDIO_REGISTER, &register_msg));
    }

    inline auto stop_watch(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd stop watch pages @ {}, length: {}", const_cast<void*>(addr), len);
        uffdio_range range_msg = {
            .start = reinterpret_cast<std::uintptr_t>(addr),
            .len = len
        };
        assert(!ioctl(this->fd, UFFDIO_UNREGISTER, &range_msg));
    }

    inline auto read() {
        uffd_msg msg;
        assert(::read(this->fd, &msg, sizeof(msg)) == sizeof(msg));

        // Only handle PAGE_FAULT, user should not fork or modify the rdma memory map!
        assert(msg.event == UFFD_EVENT_PAGEFAULT);

        spdlog::debug("UserFaultFd captured a page fault from {} @ {}, flags:{}",
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

    inline auto write_protect(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd write protect pages @ {}, length: {}", const_cast<void*>(addr), len);
        uffdio_writeprotect wp_msg {
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_WRITEPROTECT_MODE_WP
        };
        assert(!ioctl(this->fd, UFFDIO_WRITEPROTECT, &wp_msg));
    }

    inline auto write_unprotect(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd write unprotect pages @ {}, length: {}", const_cast<void*>(addr), len);

        uffdio_writeprotect wp_msg {
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_WRITEPROTECT_MODE_DONTWAKE
        };
        assert(!ioctl(this->fd, UFFDIO_WRITEPROTECT, &wp_msg));
    }

    inline auto zero(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd zero a page @ {}, length: {}", const_cast<void*>(addr), len);

        uffdio_zeropage zeropg{
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_ZEROPAGE_MODE_DONTWAKE,
            .zeropage = 0
        };
        assert(!ioctl(this->fd, UFFDIO_ZEROPAGE, &zeropg));
    }

    inline auto continue_(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd continue a page @ {}, length: {}", const_cast<void*>(addr), len);

        uffdio_continue cont{
            .range = {
                .start = reinterpret_cast<std::uintptr_t>(addr),
                .len = len
            },
            .mode = UFFDIO_CONTINUE_MODE_DONTWAKE,
            .mapped = 0
        };
        assert(!ioctl(this->fd, UFFDIO_CONTINUE, &cont));
    }

    inline auto wake(volatile void* const addr, const std::size_t len) {
        spdlog::debug("UserFaultFd wake thread waiting on page @ {}, length: {}", const_cast<void*>(addr), len);

        uffdio_range range_msg = {
            .start = reinterpret_cast<std::uintptr_t>(addr),
            .len = len
        };
        assert(!ioctl(this->fd, UFFDIO_WAKE, &range_msg));
    }
};
