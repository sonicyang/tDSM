#pragma once
#include <unistd.h>

#include <cassert>
#include <spdlog/spdlog.h>

class FileDescriptor {
 public:
    explicit FileDescriptor(const int fd_) : fd(fd_) {}

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& rhs) : fd(rhs.fd) {
        rhs.fd = -1;
    }
    FileDescriptor& operator=(FileDescriptor&& rhs) {
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

    virtual ~FileDescriptor() {
        this->release();
    }

 protected:
    int fd;
};
