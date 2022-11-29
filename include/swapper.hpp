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
#include <semaphore>
#include <memory>
#include <thread>
#include <deque>
#include <mutex>
#include <vector>
#include <set>

#include "lz4.h"

#include "configs.hpp"
#include "logging.hpp"
#include "fd.hpp"
#include "userfaultfd.hpp"
#include "cancelable_thread.hpp"
#include "node.hpp"
#include "timerfd.hpp"

#define COMPRESSION

// initialized by the ELF constructor
extern std::string master_ip;
extern std::uint16_t my_port;

class Swapper : public PeerNode {
 public:
    static Swapper& get(bool master = false) {
        // order matters, master must start before swapper, which is a peer
        if (master) {
            MasterNode::get();
        }
        static Swapper instance{master};
        return instance;
    }

    inline auto memory() const {
        return rdma_memory;
    }

    inline auto size() const {
        return rdma_size;
    }

    template<typename T>
    inline auto lock(volatile T* ptr) {
        const auto address = reinterpret_cast<std::uintptr_t>(ptr);
        const auto size = 64;

        const auto line_boundary = address & ~(size - 1);
        if (address != line_boundary) {
            spdlog::warn("Locking the whole 64bytes line 0x{:x} resides, possible deadlock");
        }

        this->lock(line_boundary, size);
    }

    template<typename T>
    inline auto lock_page(volatile T* ptr) {
        const auto address = reinterpret_cast<std::uintptr_t>(ptr);
        const auto size = page_size;

        const auto page_boundary = round_down_to_page_boundary(address);
        if (address != page_boundary) {
            spdlog::warn("Locking the whole page 0x{:x} resides");
        }

        this->lock(page_boundary, size);
    }

    template<typename T>
    inline auto unlock(volatile T* ptr) {
        const auto address = reinterpret_cast<std::uintptr_t>(ptr);
        const auto size = 64;

        const auto line_boundary = address & ~(size - 1);
        this->unlock(line_boundary, size);
    }

    template<typename T>
    inline auto unlock_page(volatile T* ptr) {
        const auto address = reinterpret_cast<std::uintptr_t>(ptr);
        const auto size = page_size;

        const auto page_boundary = round_down_to_page_boundary(address);
        this->unlock(page_boundary, size);
    }

 private:
    Swapper(bool is_master_) : PeerNode(::master_ip, ::my_port), is_master(is_master_), backing_memory_fd(memfd_create("test", 0)) {
        spdlog::info("Initializing RDMA Swapper...");

        spdlog::info("Creating backing memory...");

        // Using memfd as the backing memory to allow retaining the data on memory
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            this->backing_memory_fd.get() < 0,
            "Failed to get backing memory"
        );
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ftruncate(this->backing_memory_fd.get(), rdma_size) == -1,
            "Failed to truncate backing memory to specified size"
        );

        spdlog::info("Mapping backing memory...");
        // For the main process, create backing memory
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            madvise(rdma_memory_ptr, rdma_size, MADV_NOHUGEPAGE),
            "Failed to prevent the memory to be backed by huge page"
        );
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            mmap(rdma_memory_ptr, rdma_size, PROT_WRITE | PROT_READ, MAP_FIXED | MAP_SHARED, this->backing_memory_fd.get(), 0) == MAP_FAILED,
            "Failed to map the backing memory"
        );

        // Fill 0
        std::fill_n(rdma_memory, rdma_size, 0);

        spdlog::info("Initializing UserFaultFd...");
        // Initialize userfaulefd
        this->faultfd.watch(rdma_memory, rdma_size);

        // The starting point, all pages are zero and shared between hosts
        if (this->is_master) {
            std::fill_n(this->states, n_pages, state::OWNED);
        } else {
            std::fill_n(this->states, n_pages, state::MODIFIED);
            this->faultfd.write_protect(rdma_memory, rdma_size);

            // Remove mapping, for now
            SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
                madvise(rdma_memory_ptr, rdma_size, MADV_DONTNEED),
                "Failed evict all pages during initialization"
            );
        }

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

    bool is_master{};

    // Transmission timeout monitor
    CancelableThread timeout_monitor{std::thread{[this] { this->timeout_monitor_handler(); }}};
    TimerFd timeout_timer_fd{};

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

    std::atomic<state> states[n_pages]{};

    // Reading and writing coherence control
    std::atomic<bool> out_standing_reads[n_pages]{};

    mutable std::mutex fencing_mutex[n_pages]{};
    bool out_standing_writes[n_pages]{};
    std::set<std::size_t> fencing_set[n_pages]{};

    // Locks
    using Location_t = std::tuple<std::uintptr_t, std::size_t>;
    struct LocationHash {
        std::size_t operator()(const Location_t& key) const {
           return std::get<0>(key) ^ std::get<1>(key);
         }
    };
    mutable std::mutex lock_mutex{};
    mutable std::mutex unlock_mutex{};
    std::unordered_map<Location_t, std::pair<bool, std::condition_variable>, LocationHash> out_standing_locks{};
    std::unordered_map<Location_t, std::pair<bool, std::condition_variable>, LocationHash> out_standing_unlocks{};

    // Utility functions
    inline auto lock(const std::uintptr_t address, const std::size_t size) {
        SPDLOG_ASSERT_DUMP_IF_ERROR(
            Packet::send(this->master_fd, Packet::LockPacket{ .address = address, .size = size }),
            "Failed send locking message for 0x{:x}, size: {}",
            address, size
        );

        {
            std::unique_lock<std::mutex> lk{this->lock_mutex};
            const auto key = std::make_tuple(address, size);
            auto& ref = this->out_standing_locks[key];
            ref.first = false;
            ref.second.wait(lk, [&ref] { return ref.first; });
            this->out_standing_locks.erase(key);
        }

        return;
    }

    inline auto unlock(const std::uintptr_t address, const std::size_t size) {
        SPDLOG_ASSERT_DUMP_IF_ERROR(
            Packet::send(this->master_fd, Packet::UnlockPacket{ .address = address, .size = size }),
            "Failed send unlocking message for 0x{:x}, size: {}",
            address, size
        );

        {
            std::unique_lock<std::mutex> lk{this->unlock_mutex};
            auto& ref = this->out_standing_unlocks[std::make_tuple(address, size)];
            ref.first = false;
            ref.second.wait(lk, [&ref] { return ref.first; });
            this->out_standing_unlocks.erase(std::make_tuple(address, size));
        }
    }

    auto inline ask_page(const std::size_t frame_id) {
        // Download the page data from owner
        spdlog::trace("Asking frame {}", frame_id);
        bool request_outstanding = false;
        if(this->out_standing_reads[frame_id].compare_exchange_strong(request_outstanding, true, std::memory_order_seq_cst)) {
            this->peers.broadcast(Packet::AskPagePacket{ .frame_id = frame_id });
        }
    }

    auto inline own_page(const std::size_t frame_id) {
        spdlog::debug("Take ownership of frame {}", frame_id);
        std::scoped_lock<std::mutex> lk{this->fencing_mutex[frame_id]};
        this->fencing_set[frame_id].clear();
        this->out_standing_writes[frame_id] = true;
        this->peers.broadcast(Packet::MyPagePacket{ .frame_id = frame_id });
    }

    auto inline wait_for_write(const pid_t tid) {
        // Wait for the local thread to finish writing
        spdlog::debug("Wait for write of thread: {}", tid);
    }

    auto inline page_out_page(const std::size_t frame_id) {
        spdlog::debug("Page out frame: {}", frame_id);
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            madvise(get_frame_address(frame_id), page_size, MADV_DONTNEED),
            "Cannot discard page mapping"
        );
    }

    auto inline set_frame_state_modified(const std::size_t frame_id, const std::memory_order order = std::memory_order_seq_cst) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as MODIFIED", frame_id);
        states[frame_id].store(state::MODIFIED, order);
    }

    auto inline set_frame_state_shared(const std::size_t frame_id, const std::memory_order order = std::memory_order_seq_cst) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as SHARED", frame_id);
        states[frame_id].store(state::SHARED, order);
    }

    auto inline set_frame_state_owned(const std::size_t frame_id, const std::memory_order order = std::memory_order_seq_cst) {
        // Wait for the local thread to finish writing
        spdlog::debug("Set frame {} state as OWNED", frame_id);
        states[frame_id].store(state::OWNED, order);
    }

    bool handle_ask_page(const FileDescriptor& fd, const Packet::AskPagePacket& msg) final {
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
        if (this->states[frame_id].load(std::memory_order_acquire) == state::OWNED) {
#ifdef COMPRESSION
            constexpr auto data_max_size = LZ4_COMPRESSBOUND(page_size);
            std::uint8_t compressed[data_max_size];
            {
                const auto send_size = static_cast<std::size_t>(LZ4_compress_default(reinterpret_cast<char*>(base_address), reinterpret_cast<char*>(compressed), page_size, data_max_size));
                SPDLOG_ASSERT_DUMP_IF_ERROR(
                    send_size == 0, "Failed to compress page data"
                );

                spdlog::trace("Compression {} to {}, {}%", page_size, send_size, static_cast<double>(page_size - send_size) / page_size * 100);

                SPDLOG_ASSERT_DUMP_IF_ERROR(
                    Packet::send(fd, Packet::SendPagePacketHdr{ .frame_id = frame_id, .size = send_size }, compressed, send_size),
                    "Failed send a page to peer {}:{}, ID {}",
                    addr_str, port, peer_id
                );
            }
#else
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::send(fd, Packet::SendPagePacketHdr{ .frame_id = frame_id, .size = page_size }, base_address, page_size),
                "Failed send a page to peer {}:{}, ID {}",
                addr_str, port, peer_id
            );
#endif
        }
        return false;
    }

    bool handle_send_page(const FileDescriptor& fd, const Packet::SendPagePacketHdr& msg) final {
        // Someone is sending a data of a page to me
        const auto frame_id = msg.frame_id;
        bool request_outstanding = true;

#ifdef COMPRESSION
        constexpr auto data_max_size = LZ4_COMPRESSBOUND(page_size);
        std::uint8_t compressed[data_max_size];
#else
        std::uint8_t dummy[page_size];
#endif

        if(this->out_standing_reads[frame_id].compare_exchange_strong(request_outstanding, false, std::memory_order_seq_cst)) {
            const auto base_address = get_frame_address(frame_id);
#ifdef COMPRESSION
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::recv(fd, compressed, msg.size),
                "Failed recv a page, frame = {}",
                frame_id
            );

            const auto recv_size = LZ4_decompress_safe(reinterpret_cast<char*>(compressed), reinterpret_cast<char*>(base_address), msg.size, page_size);
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                recv_size == 0, "Failed to decompress page data"
            );
#else
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::recv(fd, base_address, msg.size),
                "Failed recv a page, frame = {}",
                frame_id
            );
#endif

            this->faultfd.write_protect(base_address, page_size);
            this->set_frame_state_shared(frame_id);
            this->faultfd.wake(base_address, page_size);
        } else {
            spdlog::warn("Frame {} is received twice, discarding", frame_id);
#ifdef COMPRESSION
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::recv(fd, compressed, msg.size),
                "Failed recv a page, frame = {}",
                frame_id
            );
#else
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::recv(fd, dummy, msg.size),
                "Failed recv a page, frame = {}",
                frame_id
            );
#endif
        }
        return false;
    }

    bool handle_my_page(const FileDescriptor& fd, const Packet::MyPagePacket& msg) final {
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

        std::scoped_lock<std::mutex> lk{this->fencing_mutex[frame_id]};

        if (this->out_standing_writes[frame_id] && peer_id > this->my_id) {
            // Oh oh, we are competing with some other peers
            // We have priority, ignore the request, tell them WE OWN THE PAGE!!
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::send(fd, Packet::MyPagePacket{ .frame_id = msg.frame_id }),
                "Failed notify peer {}:{}, ID {}, that we own the page",
                addr_str, port, peer_id
            );
        } else {
            // Give up the ownership, or acknowledge the ownership
            this->set_frame_state_modified(msg.frame_id);
            this->faultfd.write_protect(base_address, page_size);
            this->page_out_page(msg.frame_id);

            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::send(fd, Packet::YourPagePacket{ .frame_id = msg.frame_id }),
                "Failed notify peer {}:{}, ID {}, that he owns the page",
                addr_str, port, peer_id
            );

            if (this->out_standing_writes[frame_id]) {
                // Oh oh, we are competing with some other peers
                // Allow peers with smaller id to proceed first to prevent starvation
                // Revert the page state to modified, and restart from there
                this->out_standing_writes[frame_id] = false;
                this->faultfd.wake(base_address, page_size);
            }
        }

        return false;
    }

    bool handle_your_page(const FileDescriptor& fd, const Packet::YourPagePacket& msg) final {
        // Someone is responding the request we want to take ownership of the page
        auto peer = this->peers[fd.get()];
        if (!peer.has_value()) {  // Already closed?
            return true;
        }
        const auto peer_id   = peer.value()->id;
        const auto frame_id  = msg.frame_id;

        std::scoped_lock<std::mutex> lk{this->fencing_mutex[frame_id]};
        this->fencing_set[frame_id].emplace(peer_id);

        if (this->fencing_set[frame_id].size() == this->peers.size()) {
            const auto base_address = get_frame_address(msg.frame_id);
            this->faultfd.write_unprotect(base_address, page_size);

            this->out_standing_writes[frame_id] = false;
            this->set_frame_state_owned(msg.frame_id, std::memory_order_release);

            this->faultfd.wake(base_address, page_size);
        }

        return false;
    }

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
                        spdlog::info("Swapper connection to peer {}:{}, ID: {} ended!", addr_str, port, peer_id);
                        this->peers.del(fd);
                    }
                }
            } else if (epollfd.check_fd_in_result(events, this->faultfd)) {
                // Page fault?
                // Read the page fault information
                const auto fault = Swapper::faultfd.read();
                const auto frame_id = get_frame_number(fault.address);
                const auto base_address = round_down_to_page_boundary(fault.address);
                const auto current_state = this->states[frame_id].load(std::memory_order_acquire);

                spdlog::trace("Swapper processing frame {} @ {}, state: {}, is_write: {}",
                    frame_id,
                    fault.address,
                    state_to_string(current_state),
                    fault.is_write);

                if (fault.is_missing) {
                    this->set_frame_state_modified(frame_id);
                }

                if (current_state == state::MODIFIED) {
                    // If the page is dirty or not populated, first get it from owner, and make it write protected

                    if (fault.is_missing) {  // UFFD require us to zero or copy into a missing page
                        this->faultfd.zero(base_address, page_size);
                    } else {
                        this->faultfd.continue_(base_address, page_size);
                    }

                    this->ask_page(frame_id);  // Download the data from the owner, can timeout or fail during owner ship transition
                    // continue after we receive a SEND_PAGE packet
                } else if (current_state == state::SHARED) {
                    SPDLOG_ASSERT_DUMP_IF_ERROR(!fault.is_write, "A SHARED page trigger a fault, which is not a write");
                    // Take ownership
                    this->own_page(frame_id);

                    if (fault.is_minor) {
                        this->faultfd.continue_(base_address, page_size);
                    }
                    // continue with YOUR_PAGE response handling
                } else if (current_state == state::OWNED) {
                    SPDLOG_ASSERT_DUMP_IF_ERROR(true, "A OWNED page trigger a fault");
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

        const auto epollfd = Epoll(this->timeout_timer_fd, this->thread.evtfd);
        while (!this->thread.stopped.load(std::memory_order_acquire)) {
            // Wait for page fault or remote call
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || epollfd.check_fd_in_result(events, this->thread.evtfd)) {
                continue;
            }

            std::uint64_t expire_count;
            ::read(this->timeout_timer_fd.get(), &expire_count, sizeof(expire_count));

            for (std::size_t frame_id = 0; frame_id < n_pages; ++frame_id) {
                if (this->out_standing_reads[frame_id].load(std::memory_order_acquire)) {
                    spdlog::debug("Timeout, asking frame {} again", frame_id);
                    this->peers.broadcast(Packet::AskPagePacket{ .frame_id = frame_id });
                }

                {
                    std::scoped_lock<std::mutex> lk{this->fencing_mutex[frame_id]};
                    if (this->out_standing_writes[frame_id] == true) {
                        spdlog::debug("Timeout, taking ownership of frame {} again", frame_id);
                        this->peers.broadcast(Packet::MyPagePacket{ .frame_id = frame_id });
                    }
                }
            }
        }
    }

    bool handle_lock(const FileDescriptor&, const Packet::LockPacket& msg) final {
        {
            std::scoped_lock<std::mutex> lk{this->lock_mutex};
            auto& ref = this->out_standing_locks[std::make_tuple(msg.address, msg.size)];
            ref.first = true;
            ref.second.notify_one();
        }
        return false;
    }

    bool handle_no_lock(const FileDescriptor&, const Packet::NoLockPacket& msg) final {
        SPDLOG_ASSERT_DUMP_IF_ERROR(true, "Failed to Lock 0x{:x}, size: {}, not on page or 64 byte boundary?", msg.address, msg.size);
        return false;
    }

    bool handle_unlock(const FileDescriptor&, const Packet::UnlockPacket& msg) final {
        {
            std::scoped_lock<std::mutex> lk{this->unlock_mutex};
            auto& ref = this->out_standing_unlocks[std::make_tuple(msg.address, msg.size)];
            ref.first = true;
            ref.second.notify_one();
        }
        return false;
    }

    bool handle_no_unlock(const FileDescriptor&, const Packet::NoUnlockPacket& msg) final {
        SPDLOG_ASSERT_DUMP_IF_ERROR(true, "Failed to Unlock 0x{:x}, size: {}, unlock something that is not locked?", msg.address, msg.size);
        return false;
    }
};
