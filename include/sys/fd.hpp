#pragma once
#include <unistd.h>

#include <cassert>
#include <spdlog/spdlog.h>

namespace tDSM::sys {

class file_descriptor {
 public:
    explicit file_descriptor(const int fd_) : fd(fd_) {}

    file_descriptor(const file_descriptor&) = delete;
    file_descriptor& operator=(const file_descriptor&) = delete;

    file_descriptor(file_descriptor&& rhs) : fd(rhs.fd) {
        rhs.fd = -1;
    }
    file_descriptor& operator=(file_descriptor&& rhs) {
        this->fd = rhs.fd;
        rhs.fd = -1;
        return *this;
    }

    inline auto get() const {
        return fd;
    }

    inline auto release() {
        if (fd >= 0) {
            close(fd);
            fd = -1;
        }
    }

    virtual ~file_descriptor() {
        this->release();
    }

 protected:
    int fd;
};

}  // namesapce tDSM::sys
