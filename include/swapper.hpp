#pragma once
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/userfaultfd.h>

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/usage.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <semaphore>
#include <memory>
#include <thread>
#include <deque>
#include <mutex>
#include <vector>

#include "fd.hpp"
#include "userfaultfd.hpp"
#include "cancelable_thread.hpp"
#include "node.hpp"

static inline constexpr auto page_size = 0x1000;  // 4KB, assumed
static inline constexpr auto n_pages = 8;
static inline constexpr auto rdma_size = page_size * n_pages;
extern volatile std::uint8_t rdma_memory[rdma_size] __attribute__((section(".rdma"), aligned(page_size)));
static inline constexpr auto rdma_memory_ptr = const_cast<std::uint8_t*>(rdma_memory);

// initialized by the ELF constructor
static inline std::string master_ip;
static inline std::uint16_t my_port;

class Swapper : public PeerNode{
 public:
    static Swapper& get(bool master = false) {
        if (master) {
            MasterNode::get();
        }
        static Swapper instance;
        return instance;
    }

    inline auto memory() {
        return rdma_memory;
    }

    inline auto size() {
        return rdma_size;
    }


 private:
    Swapper() : PeerNode(::master_ip, ::my_port), backing_memory_fd(memfd_create("test", 0)) {
        spdlog::info("Initializing RDMA Swapper...");

        spdlog::info("Creating backing memory...");

        // Using memfd as the backing memory to allow retaining the data on memory
        if (this->backing_memory_fd.get() < 0) {
            spdlog::error("Failed to get backing memory: {}", strerror(errno));
            abort();
        }
        assert(ftruncate(this->backing_memory_fd.get(), rdma_size) != -1);

        spdlog::info("Mapping backing memory...");
        // For the main process, create backing memory
        assert(!madvise(rdma_memory_ptr, rdma_size, MADV_NOHUGEPAGE));
        assert(mmap(rdma_memory_ptr, rdma_size, PROT_WRITE | PROT_READ, MAP_FIXED | MAP_SHARED, this->backing_memory_fd.get(), 0) != MAP_FAILED);

        // Fill 0
        std::fill_n(rdma_memory, rdma_size, 0);

        spdlog::info("Initializing UserFaultFd...");
        // Initialize userfaulefd
        this->faultfd.watch(rdma_memory, rdma_size);
        this->faultfd.write_protect(rdma_memory, rdma_size);

        // The starting point, all pages are zero and shared between hosts
        std::fill_n(this->states, n_pages, state::SHARED);

        // start the swapper thread
        this->thread = std::thread([this] {
            this->run();
        });
    }

    ~Swapper() override {}

    // Singlton
    Swapper(const Swapper&) = delete;
    Swapper& operator=(const Swapper&) = delete;
    Swapper(Swapper&&) = delete;
    Swapper& operator=(Swapper&&) = delete;

    // The worker thread
    CancelableThread thread{};

    // Backing memory and memory management
    const FileDescriptor backing_memory_fd;
    UserFaultFd faultfd = UserFaultFd();

    enum class state {
        SHARED,
        MODIFIED,
        OWNED
    };

    static auto inline state_to_string(const state s) {
        if (s == state::MODIFIED) {
            return "MODIFIED";
        } else if (s == state::SHARED) {
            return "SHARED";
        } else if (s == state::OWNED) {
            return "OWNED";
        } else {
            return "UNKNOWN";
        }
    }

    state states[n_pages];
    //static inline std::binary_semaphore semaphores[n_pages];

    // Utility functions
    auto inline get_frame_number(void* const addr) {
        const auto uaddr = reinterpret_cast<std::uintptr_t>(addr);
        const auto base = reinterpret_cast<std::uintptr_t>(rdma_memory);
        assert(base <= uaddr && uaddr < base + rdma_size);  // sanity check
        return  (uaddr - base) / n_pages;
    }

    auto inline pull_page(const std::size_t frame) {
        // Download the page data from owner
        spdlog::debug("Pull frame {}", frame);
    }

    auto inline own_page(const std::size_t frame) {
        spdlog::debug("Take ownership of frame {}", frame);
    }

    auto inline lock_page(const std::size_t frame) {
        // Lock the page from remote read write
        // broadcast that the page is modified
        spdlog::debug("Lock frame {}", frame);
    }

    auto inline unlock_page(const std::size_t frame) {
        // unlock the page from remote read write
        spdlog::debug("Unlock frame {}", frame);
    }

    auto inline wait_for_write(const pid_t tid) {
        // Wait for the local thread to finish writing
        spdlog::debug("Wait for write of thread: {}", tid);
    }

 public:  // XXX: for now
    auto inline page_out_page(const std::size_t frame) {
        spdlog::debug("Page out frame: {}", frame);
        assert(!madvise(rdma_memory_ptr + frame * page_size, page_size, MADV_DONTNEED));
    }

    auto inline set_frame_state_modified(const std::size_t frame) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as MODIFIED", frame);
        states[frame] = state::MODIFIED;
    }

    auto inline set_frame_state_shared(const std::size_t frame) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as SHARED", frame);
        states[frame] = state::SHARED;
    }

    auto inline set_frame_state_owned(const std::size_t frame) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as OWNED", frame);
        states[frame] = state::OWNED;
    }

 private:
    // Actual worker thread function
    inline void run() {
        const auto epollfd = Epoll(this->faultfd, this->peers.get_epoll_fd(), this->thread.evtfd);
        while (!this->thread.stopped.load(std::memory_order_acquire)) {
            // Wait for page fault or remote call
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || epollfd.check_fd_in_result(events, this->thread.evtfd)) {
                continue;
            } else if (epollfd.check_fd_in_result(events, this->peers.get_epoll_fd())) {
                // Remote messages
                std::vector<FileDescriptor*> peers_to_recycle;
                const auto [msg_count, msg_events] = this->peers.get_epoll_fd().wait();  // should not block
                for (const auto& event : msg_events) {
                    auto  peer           = this->peers[event.data.fd];
                    if (!peer.has_value()) {
                        // Already closed
                        continue;
                    }
                    auto& fd             = peer.value()->fd;
                    const auto peer_id   = peer.value()->id;
                    const auto& addr_str = peer.value()->addr_str;
                    const auto port      = peer.value()->port;

                    const auto type = Packet::peek_packet_type(fd);
                    if (!type.has_value()) {
                        spdlog::error("Swapper connection to peer {}:{}, ID: {} ended unexpectedly!", addr_str, port, peer_id);
                        this->peers.del(fd);
                        continue;
                    }

                    // Default actions
                    if (this->handle_a_packet(type.value(), addr_str, port, fd)) {
                        spdlog::debug("Swapper connection to peer {}:{}, ID: {} ended!", addr_str, port, peer_id);
                        this->peers.del(fd);
                    }
                }
            } else if (epollfd.check_fd_in_result(events, this->faultfd)) {
                // Page fault?
                if (epollfd.check_fd_in_result(events, this->faultfd)) {
                    // Read the page fault information
                    const auto fault = Swapper::faultfd.read();
                    const auto frame = this->get_frame_number(fault.address);
                    const auto current_state = this->states[frame];

                    spdlog::debug("Swapper processing frame {} @ {}, state: {}, is_write: {}",
                        frame,
                        fault.address,
                        state_to_string(current_state),
                        fault.is_write);

                    if (fault.is_missing || current_state == state::MODIFIED) {
                        // If the page is dirty or not populated, first get it from owner, and make it write protected

                        this->pull_page(frame);  // Download the data from the owner

                        this->set_frame_state_shared(frame);

                        this->faultfd.write_protect(fault.address, page_size);
                        this->faultfd.continue_(fault.address, page_size);
                        this->faultfd.wake(fault.address, page_size);
                    } else if (current_state == state::SHARED) {
                        if (fault.is_write) {
                            // Take ownership and lock
                            this->own_page(frame);
                            this->lock_page(frame);
                        }

                        this->set_frame_state_owned(frame);

                        if (fault.is_minor) {
                            this->faultfd.continue_(fault.address, page_size);
                        }
                        this->faultfd.write_unprotect(fault.address, page_size);
                        this->faultfd.wake(fault.address, page_size);

                        if (fault.is_write) {
                            // Wait for the write to finish and broadcast that the page is modified
                            this->wait_for_write(fault.tid);
                            this->unlock_page(frame);
                        }
                    } else if (current_state == state::OWNED) {
                        assert(false);  // WTF?
                    } else {
                        assert(false);  // WTF?
                    }
                }
            }
        }
        this->peers.clear();
    }
};
