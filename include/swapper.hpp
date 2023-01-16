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
#include <spdlog/spdlog.h>
#include <spdlog/stopwatch.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <semaphore>
#include <set>
#include <thread>
#include <vector>

#include "lz4.h"

#include "configs.hpp"
#include "node.hpp"
#include "sys/fd.hpp"
#include "sys/timerfd.hpp"
#include "sys/userfaultfd.hpp"
#include "utils/cancelable_thread.hpp"
#include "utils/logging.hpp"

namespace tDSM {

// initialized by the ELF constructor
extern std::string master_ip;
extern std::uint16_t my_port;
extern bool is_master;
extern bool use_compression;

class swapper : public peer_node {
 public:
    static swapper& get() {
        // order matters, master must start before swapper, which is a peer
        if (tDSM::is_master) {
            master_node::get();
        }
        static swapper instance;
        return instance;
    }

    inline auto memory() const {
        return rdma_memory;
    }

    inline auto size() const {
        return rdma_size;
    }

    inline auto sem_get(const std::uintptr_t address) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            packet::send(this->master_fd, packet::sem_get_packet{ .address = address }),
            "Failed send sem get message for 0x{:x}",
            address
        );

        const auto frame_id = get_frame_number(reinterpret_cast<void*>(address));
        auto& sem = [&]() -> decltype(auto) {
            std::scoped_lock<std::mutex> lk{this->sem_list_mutex};
            if (!this->sem_list[frame_id].contains(address)) {
                this->sem_list[frame_id].emplace(address, std::make_unique<sem_semaphore>(0));
            }
            return this->sem_list[frame_id][address];
        }();

        spdlog::trace("Semaphore: get {:x}", address);

        sem->acquire();
    }

    inline auto sem_put(const std::uintptr_t address) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            packet::send(this->master_fd, packet::sem_put_packet{ .address = address }),
            "Failed send sem put message for 0x{:x}",
            address
        );
    }

 private:
    swapper() : peer_node(tDSM::master_ip, tDSM::my_port), is_master(tDSM::is_master), backing_memory_fd(memfd_create("test", 0)) {
        spdlog::info("Initializing RDMA swapper...");

        spdlog::info("Creating backing memory...");

        // Using memfd as the backing memory to allow retaining the data on memory
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            this->backing_memory_fd.get() < 0,
            "Failed to get backing memory"
        );
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ftruncate(this->backing_memory_fd.get(), rdma_size) == -1,
            "Failed to truncate backing memory to specified size"
        );

        spdlog::info("Mapping backing memory...");
        // For the main process, create backing memory
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            madvise(rdma_memory_ptr, rdma_size, MADV_NOHUGEPAGE),
            "Failed to prevent the memory to be backed by huge page"
        );
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            mmap(rdma_memory_ptr, rdma_size, PROT_WRITE | PROT_READ, MAP_FIXED | MAP_SHARED, this->backing_memory_fd.get(), 0) == MAP_FAILED,
            "Failed to map the backing memory"
        );

        // Fill 0
        std::fill_n(rdma_memory, rdma_size, 0);

        spdlog::info("Initializing user_fault_fd...");
        // Initialize userfaulefd
        this->faultfd.watch(rdma_memory, rdma_size);

        // The starting point, all pages are zero and shared between hosts
        if (this->is_master) {
            std::fill_n(this->states, n_pages, state::exclusive);
        } else {
            std::fill_n(this->states, n_pages, state::invalid);
            this->faultfd.write_protect(rdma_memory, rdma_size);

            // Remove mapping, for now
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
                madvise(rdma_memory_ptr, rdma_size, MADV_DONTNEED),
                "Failed evict all pages during initialization"
            );
        }

        // start the swapper thread
        this->thread = std::thread([this] {
            this->run();
        });
    }

    ~swapper() override {}

    // Singlton
    swapper(const swapper&) = delete;
    swapper& operator=(const swapper&) = delete;
    swapper(swapper&&) = delete;
    swapper& operator=(swapper&&) = delete;

    bool is_master{};

    // Transmission timeout monitor
    utils::cancelable_thread timeout_monitor{std::thread{[this] { this->timeout_monitor_handler(); }}};
    sys::timer_fd timeout_timer_fd{};

    // The worker thread
    utils::cancelable_thread thread{};
    // Backing memory and memory management
    const sys::file_descriptor backing_memory_fd;
    sys::user_fault_fd faultfd{};

    tDSM_BETTER_ENUM(state, int,
        shared,
        invalid,
        exclusive
    );

    std::atomic<state> states[n_pages]{};

    // Reading and writing coherence control
    mutable std::mutex read_fencing_mutex[n_pages]{};
    std::set<std::size_t> read_fencing_set[n_pages]{};
    bool out_standing_read[n_pages]{};

    mutable std::mutex write_fencing_mutex[n_pages]{};
    std::set<std::size_t> write_fencing_set[n_pages]{};
    bool out_standing_write[n_pages]{};

    // Semaphores, zero initialized, 256 is arbitrary
    using sem_semaphore = std::counting_semaphore<256>;
    mutable std::mutex sem_list_mutex{};
    std::unordered_map<std::uintptr_t, std::unique_ptr<sem_semaphore>> sem_list[n_pages]{};

    auto inline ask_page(const std::size_t frame_id) {
        // Download the page data from owner
        spdlog::trace("Asking frame {}", frame_id);
        {
            std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
            this->read_fencing_set[frame_id].clear();
            this->out_standing_read[frame_id] = true;
        }

        this->peers.broadcast(packet::ask_page_packet{ .frame_id = frame_id });
    }

    auto inline own_page(const std::size_t frame_id) {
        // Make page exclusive for us
        spdlog::debug("Take ownership of frame {}", frame_id);
        {
            std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};
            this->write_fencing_set[frame_id].clear();
            this->out_standing_write[frame_id] = true;
        }

        this->peers.broadcast(packet::my_page_packet{ .frame_id = frame_id });
    }

    auto inline wait_for_write(const pid_t tid) {
        // Wait for the local thread to finish writing
        spdlog::debug("Wait for write of thread: {}", tid);
    }

    auto inline page_out_page(const std::size_t frame_id) {
        spdlog::debug("Page out frame: {}", frame_id);
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            madvise(get_frame_address(frame_id), page_size, MADV_DONTNEED),
            "Cannot discard page mapping"
        );
    }

    auto inline set_frame_state_modified(const std::size_t frame_id, const std::memory_order order = std::memory_order_seq_cst) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as invalid", frame_id);
        states[frame_id].store(state::invalid, order);
    }

    auto inline set_frame_state_shared(const std::size_t frame_id, const std::memory_order order = std::memory_order_seq_cst) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as shared", frame_id);
        states[frame_id].store(state::shared, order);
    }

    auto inline set_frame_state_owned(const std::size_t frame_id, const std::memory_order order = std::memory_order_seq_cst) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as exclusive", frame_id);
        states[frame_id].store(state::exclusive, order);
    }

    bool handle_ask_page(const sys::file_descriptor& fd, const packet::ask_page_packet& msg) final {
        // Someone is asking for a page
        auto peer = this->peers[fd.get()];
        if (!peer.has_value()) {  // Already closed?
            return true;
        }
        const auto peer_id   = peer.value()->id;
        const auto& addr_str = peer.value()->addr_str;
        const auto port      = peer.value()->port;

        const auto frame_id = msg.frame_id;
        const auto base_address = get_frame_address(frame_id);
        const auto current_state = this->states[frame_id].load(std::memory_order_acquire);
        if (current_state == state::exclusive) {
            // no longer exclusive
            this->set_frame_state_shared(frame_id);
            this->faultfd.write_protect(base_address, page_size);

            // Default if no compressed
            const void* ptr_to_send = base_address;
            std::size_t size_to_send = page_size;

            constexpr auto data_max_size = LZ4_COMPRESSBOUND(page_size);
            std::uint8_t compressed[data_max_size];

            if (this->use_compression) {
                ptr_to_send = compressed;

                size_to_send = static_cast<std::size_t>(LZ4_compress_default(reinterpret_cast<char*>(base_address), reinterpret_cast<char*>(compressed), page_size, data_max_size));
                tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                    size_to_send == 0, "Failed to compress page data"
                );

                spdlog::trace("Compression {} to {}, {}%", page_size, size_to_send, static_cast<double>(page_size - size_to_send) / page_size * 100);
            }

            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, packet::send_page_packet{ .frame_id = frame_id, .size = size_to_send }, ptr_to_send, size_to_send),
                "Failed send a page to peer {}:{}, ID {}",
                addr_str, port, peer_id
            );
        } else {
            // Shared or invalid send a empty packet to indicate that it is notified of the read.
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, packet::send_page_packet{ .frame_id = frame_id, .size = 0 }, nullptr, 0),
                "Failed send a page to peer {}:{}, ID {}",
                addr_str, port, peer_id
            );
        }
        return false;
    }

    bool handle_send_page(const sys::file_descriptor& fd, const packet::send_page_packet& msg) final {
        // Someone is sending a data of a page to me
        auto peer = this->peers[fd.get()];
        if (!peer.has_value()) {  // Already closed?
            return true;
        }
        const auto peer_id   = peer.value()->id;
        const auto frame_id = msg.frame_id;

        constexpr auto data_max_size = LZ4_COMPRESSBOUND(page_size);

        std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
        if (this->out_standing_read[frame_id]) {
            const auto base_address = get_frame_address(frame_id);

            if (msg.size > 0) {
                this->faultfd.write_unprotect(base_address, page_size);

                void* address_to_write = base_address;
                std::uint8_t compressed[data_max_size];

                if (this->use_compression) {
                    address_to_write = compressed;
                }

                tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                    packet::recv(fd, address_to_write, msg.size),
                    "Failed recv a page, frame = {}",
                    frame_id
                );


                if (this->use_compression) {
                    const auto recv_size = LZ4_decompress_safe(reinterpret_cast<char*>(address_to_write), reinterpret_cast<char*>(base_address), msg.size, page_size);
                    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                        recv_size == 0, "Failed to decompress page data"
                    );
                }
            }

            this->read_fencing_set[frame_id].emplace(peer_id);

            if (this->read_fencing_set[frame_id].size() == this->peers.size()) {
                this->out_standing_read[frame_id] = false;
                this->set_frame_state_shared(frame_id);
                this->faultfd.write_protect(base_address, page_size);
                this->faultfd.wake(base_address, page_size);
            }
        } else {
            spdlog::warn("Frame {} is received twice, discarding", frame_id);

            std::uint8_t dummy[std::max(page_size, data_max_size)];

            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::recv(fd, dummy, msg.size),
                "Failed recv a page, frame = {}",
                frame_id
            );
        }

        return false;
    }

    bool handle_my_page(const sys::file_descriptor& fd, const packet::my_page_packet& msg) final {
        // Someone is taking the ownership of a page
        const auto frame_id = msg.frame_id;
        const auto base_address = get_frame_address(msg.frame_id);

        auto peer = this->peers[fd.get()];
        if (!peer.has_value()) {  // Already closed?
            return true;
        }
        const auto peer_id   = peer.value()->id;
        const auto& addr_str = peer.value()->addr_str;
        const auto port      = peer.value()->port;

        {
            std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
            if (this->out_standing_read[frame_id]) {
                // Oh oh, we are competing with some other peers, we are read, they are writing
                // Read have priority, ignore the request
                return false;
            }
        }

        std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};

        if (this->out_standing_write[frame_id] && peer_id > this->my_id) {
            // Oh oh, we are competing with some other peers
            // We have priority, ignore the request, tell them WE OWN THE PAGE!!
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, packet::my_page_packet{ .frame_id = msg.frame_id }),
                "Failed notify peer {}:{}, ID {}, that we own the page",
                addr_str, port, peer_id
            );
        } else {
            // Give up the ownership, or acknowledge the ownership
            this->set_frame_state_modified(msg.frame_id);
            this->faultfd.write_protect(base_address, page_size);
            this->page_out_page(msg.frame_id);

            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, packet::your_page_packet{ .frame_id = msg.frame_id }),
                "Failed notify peer {}:{}, ID {}, that he owns the page",
                addr_str, port, peer_id
            );

            if (this->out_standing_write[frame_id]) {
                // Oh oh, we are competing with some other peers
                // Allow peers with smaller id to proceed first to prevent starvation
                // Revert the page state to modified, and restart from there
                this->out_standing_write[frame_id] = false;
                this->faultfd.wake(base_address, page_size);
            }
        }

        return false;
    }

    bool handle_your_page(const sys::file_descriptor& fd, const packet::your_page_packet& msg) final {
        // Someone is responding the request we want to take ownership of the page
        auto peer = this->peers[fd.get()];
        if (!peer.has_value()) {  // Already closed?
            return true;
        }
        const auto peer_id   = peer.value()->id;
        const auto frame_id  = msg.frame_id;

        std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};
        this->write_fencing_set[frame_id].emplace(peer_id);

        if (this->write_fencing_set[frame_id].size() == this->peers.size()) {
            const auto base_address = get_frame_address(msg.frame_id);
            this->faultfd.write_unprotect(base_address, page_size);

            this->out_standing_write[frame_id] = false;
            this->set_frame_state_owned(msg.frame_id, std::memory_order_release);

            this->faultfd.wake(base_address, page_size);
        }

        return false;
    }

    // Actual worker thread function
    inline void run() {
        const auto epollfd = sys::epoll(this->faultfd, this->peers.get_epoll_fd(), this->thread.evtfd);
        while (!this->thread.stopped.load(std::memory_order_acquire)) {
            // Wait for page fault or remote call
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || epollfd.check_fd_in_result(events, this->thread.evtfd)) {
                continue;
            } else if (epollfd.check_fd_in_result(events, this->peers.get_epoll_fd())) {
                // Remote messages
                std::vector<sys::file_descriptor*> peers_to_recycle;
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

                    const auto type = packet::peek_packet_type(fd);
                    if (!type.has_value()) {
                        spdlog::error("swapper connection to peer {}:{}, ID: {} ended unexpectedly!", addr_str, port, peer_id);
                        this->peers.del(fd);
                        continue;
                    }

                    // Default actions
                    if (this->handle_a_packet(type.value(), addr_str, port, fd)) {
                        spdlog::info("swapper connection to peer {}:{}, ID: {} ended!", addr_str, port, peer_id);
                        this->peers.del(fd);
                    }
                }
            } else if (epollfd.check_fd_in_result(events, this->faultfd)) {
                // Page fault?
                // Read the page fault information
                const auto fault = swapper::faultfd.read();
                const auto frame_id = get_frame_number(fault.address);
                const auto base_address = round_down_to_page_boundary(fault.address);
                const auto current_state = this->states[frame_id].load(std::memory_order_acquire);

                spdlog::trace("swapper processing frame {} @ {}, state: {}, is_write: {}",
                    frame_id,
                    fault.address,
                    state_to_string(current_state),
                    fault.is_write);

                if (fault.is_missing) {
                    this->set_frame_state_modified(frame_id);
                }

                if (current_state == state::invalid) {
                    // If the page is dirty or not populated, first get it from owner, and make it write protected

                    if (fault.is_missing) {  // UFFD require us to zero or copy into a missing page
                        this->faultfd.zero(base_address, page_size);
                    } else {
                        this->faultfd.continue_(base_address, page_size);
                    }

                    this->ask_page(frame_id);  // Download the data from the owner, can timeout or fail during owner ship transition
                    // continue after we receive a SEND_PAGE packet
                } else if (current_state == state::shared) {
                    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(!fault.is_write, "A shared page trigger a fault, which is not a write");
                    // Take ownership
                    this->own_page(frame_id);

                    if (fault.is_minor) {
                        this->faultfd.continue_(base_address, page_size);
                    }
                    // continue with YOUR_PAGE response handling
                } else if (current_state == state::exclusive) {
                    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(true, "A exclusive page trigger a fault");
                }
            }
        }
        this->peers.clear();
    }

    void timeout_monitor_handler() {
        this->timeout_timer_fd.periodic(
            timespec {
                .tv_sec = 1,
                .tv_nsec = 0
            },
            timespec {
                .tv_sec = 2,
                .tv_nsec = 0
            }
        );

        const auto epollfd = sys::epoll(this->timeout_timer_fd, this->thread.evtfd);
        while (!this->thread.stopped.load(std::memory_order_acquire)) {
            // Wait for page fault or remote call
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || epollfd.check_fd_in_result(events, this->thread.evtfd)) {
                continue;
            }

            std::uint64_t expire_count;
            ::read(this->timeout_timer_fd.get(), &expire_count, sizeof(expire_count));

            for (std::size_t frame_id = 0; frame_id < n_pages; ++frame_id) {
                {
                    std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
                    if (this->out_standing_read[frame_id] == true) {
                        spdlog::debug("Timeout, asking frame {} again", frame_id);
                        this->peers.broadcast(packet::ask_page_packet{ .frame_id = frame_id });
                    }
                }

                {
                    std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};
                    if (this->out_standing_write[frame_id] == true) {
                        spdlog::debug("Timeout, taking ownership of frame {} again", frame_id);
                        this->peers.broadcast(packet::my_page_packet{ .frame_id = frame_id });
                    }
                }
            }
        }
    }

    bool handle_sem_put(const sys::file_descriptor&, const packet::sem_put_packet& msg) final {
        const auto frame_id = get_frame_number(reinterpret_cast<void*>(msg.address));
        auto& sem = [&]() -> decltype(auto) {
            std::scoped_lock<std::mutex> lk{this->sem_list_mutex};
            if (!this->sem_list[frame_id].contains(msg.address)) {
                this->sem_list[frame_id].emplace(msg.address, std::make_unique<sem_semaphore>(0));
            }
            return this->sem_list[frame_id][msg.address];
        }();

        spdlog::trace("Semaphore: put {:x}", msg.address);

        sem->release(1);
        return false;
    }
};

}  // namespace tDSM
