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
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/userfaultfd.h>

#include "spdlog/spdlog.h"
#include "spdlog/stopwatch.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
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

class swapper : public peer_node {
 public:
    static swapper& get() {
        static swapper instance;
        return instance;
    }

    inline auto memory() const {
        return rdma_memory;
    }

    inline auto size() const {
        return static_cast<std::size_t>(rdma_size);
    }

    inline auto make_sem(const std::size_t initial_count) {
        const auto current_req_num = this->make_sem_req_count.fetch_add(1);

        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            this->atomic_rpc_send_request(packet::make_sem_packet{ .initial_count = initial_count, .request_num = current_req_num }),
            "Failed to make a new semaphore"
        );

        auto& rendezvous = [&]()  -> decltype(auto){
            std::scoped_lock<std::mutex> lk(this->out_standing_make_sem_mutex);
            this->out_standing_make_sem[current_req_num] = std::make_unique<make_sem_rendezvous>();
            return this->out_standing_make_sem[current_req_num];
        }();

        rendezvous->sem.acquire();

        std::scoped_lock<std::mutex> lk(this->out_standing_make_sem_mutex);
        const auto address = this->out_standing_make_sem[current_req_num]->address;
        this->out_standing_make_sem.erase(current_req_num);

        return address;
    }

    inline auto sem_get(const std::uintptr_t address) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            this->atomic_rpc_send_request(packet::sem_get_packet{ .address = address }),
            "Failed send sem get message for {}",
            address
        );

        auto& sem = [&]() -> decltype(auto) {
            std::shared_lock<std::shared_mutex> lk(this->sem_list_mutex);
            if (!this->sem_list.contains(address)) {
                lk.unlock();
                std::unique_lock<std::shared_mutex> lk2(this->sem_list_mutex);
                if (!this->sem_list.contains(address)) {
                    this->sem_list[address] = std::make_unique<sem_semaphore>(0);
                }
                return this->sem_list[address];
            }

            return this->sem_list[address];
        }();

        sem_logger->trace("Semaphore: get {}", address);

        sem->acquire();
    }

    inline auto sem_put(const std::uintptr_t address) {
        // Compiler fence
        asm inline (""::: "memory");

        sem_logger->trace("Semaphore: putting {}", address);

        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            this->atomic_rpc_send_request(packet::sem_put_packet{ .address = address }),
            "Failed send sem get message for {}",
            address
        );
    }

    inline auto wait_for_peer(const id_t peer_id) {
        this->peers.wait_for_peer(peer_id);
    }

    inline auto initialize(const bool is_master, const std::string& directory_addr_, const std::string& my_addr_, const std::uint16_t my_port_) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(initialized, "Cannot initialize twice");

        logger->info("Initializing communication...");

        peer_node::initialize(directory_addr_, my_addr_, my_port_);

        logger->info("Initializing RDMA swapper...");

        logger->info("Creating backing memory...");

        // Using memfd as the backing memory to allow retaining the data on memory
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            this->backing_memory_fd.get() < 0,
            "Failed to get backing memory"
        );
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            ftruncate(this->backing_memory_fd.get(), rdma_size) == -1,
            "Failed to truncate backing memory to specified size"
        );

        logger->info("Mapping backing memory...");
        // For the main process, create backing memory
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            madvise(rdma_memory_ptr, rdma_size, MADV_NOHUGEPAGE),
            "Failed to prevent the memory to be backed by huge page"
        );
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            mmap(rdma_memory_ptr, rdma_size, PROT_WRITE | PROT_READ, MAP_FIXED | MAP_SHARED, this->backing_memory_fd.get(), 0) == MAP_FAILED,
            "Failed to map the backing memory"
        );

        logger->info("Initializing user_fault_fd...");

        this->rdma_region_rw = reinterpret_cast<unsigned char*>(mmap(nullptr, rdma_size, PROT_WRITE | PROT_READ, MAP_SHARED, this->backing_memory_fd.get(), 0));
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            this->rdma_region_rw == MAP_FAILED,
            "Failed to map the backing memory"
        );

        // Initialize userfaulefd
        this->faultfd.watch(rdma_memory, rdma_size);

        // Fill 0
        std::fill_n(this->rdma_region_rw, rdma_size, 0);

        // The starting point, all pages are zero and shared between hosts
        if (is_master) {
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


        {
            std::scoped_lock<std::mutex> lk(this->additional_timeout_handler_mutex);
            this->additional_timeout_handler.emplace_back([this] { this->check_timeout_ask_page(); });
            this->additional_timeout_handler.emplace_back([this] { this->check_timeout_send_page(); });
        }

        // start the swapper thread
        this->thread = std::thread([this] {
            this->run();
        });

        // initialized was set by node initialize
    }

 private:

    // Singlton
    swapper() : backing_memory_fd(memfd_create("rdma", 0)) {
    }

    ~swapper() override {}
    swapper(const swapper&) = delete;
    swapper& operator=(const swapper&) = delete;
    swapper(swapper&&) = delete;
    swapper& operator=(swapper&&) = delete;

    // Backing memory and memory management
    const sys::file_descriptor backing_memory_fd;
    sys::user_fault_fd faultfd{};

    // The worker thread
    utils::cancelable_thread thread{};

    tDSM_BETTER_ENUM(state, int,
        shared,
        shared_had_owned_page,
        invalid,
        exclusive
    )

    std::atomic<state> states[n_pages]{};
    unsigned char* rdma_region_rw;

    // Reading and writing coherence control
    mutable std::mutex read_fencing_mutex[n_pages]{};
    std::set<frame_id_t> read_fencing_set[n_pages]{};
    bool out_standing_read[n_pages]{};

    mutable std::mutex write_fencing_mutex[n_pages]{};
    std::set<frame_id_t> write_fencing_set[n_pages]{};
    bool out_standing_write[n_pages]{};

    // Semaphores, zero initialized, 256 is arbitrary
    using sem_semaphore = std::counting_semaphore<256>;
    mutable std::shared_mutex sem_list_mutex{};
    std::unordered_map<std::uintptr_t, std::unique_ptr<sem_semaphore>> sem_list{};

    std::mutex out_standing_make_sem_mutex{};
    std::atomic_size_t make_sem_req_count{0};
    struct make_sem_rendezvous {
        std::binary_semaphore sem{0};
        std::uintptr_t address{};
    };
    std::unordered_map<std::size_t, std::unique_ptr<make_sem_rendezvous>> out_standing_make_sem{};

    inline auto ask_page(const frame_id_t frame_id) {
        // Download the page data from owner
        logger->debug("Asking frame {} {} {}", frame_id, packet::ask_page_packet{ .frame_id = frame_id }.hdr.from, packet::my_id);
        {
            std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
            this->read_fencing_set[frame_id].clear();
            this->out_standing_read[frame_id] = true;
        }

        (void)this->atomic_pub_send(packet::ask_page_packet{ .frame_id = frame_id });
    }

    inline auto own_page(const frame_id_t frame_id) {
        // Make page exclusive for us
        logger->debug("Take ownership of frame {}", frame_id);
        {
            std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};
            this->write_fencing_set[frame_id].clear();
            this->out_standing_write[frame_id] = true;
        }

        (void)this->atomic_pub_send(packet::my_page_packet{ .frame_id = frame_id });
    }

    inline auto page_out_page(const frame_id_t frame_id) {
        logger->debug("Page out frame: {}", frame_id);
        while (true) {
            const auto err = madvise(get_frame_address(frame_id), page_size, MADV_DONTNEED);
            if (err == 0) {
                return;
            }
            logger->error("Failed to page out a frame {} : {}", frame_id, strerror(errno));
        }
    }

    inline auto set_frame_state(const frame_id_t frame_id, const state s,  const std::memory_order order = std::memory_order_seq_cst) {
        logger->trace("Set frame {} state as {}", frame_id, state_to_string(s));
        states[frame_id].store(s, order);
    }

    bool handle_ask_page(zmq::socket_ref, zmq::message_t&, const packet::ask_page_packet& msg) final {
        // Someone is asking for a page
        const auto peer_id       = msg.hdr.from;
        const auto frame_id      = msg.frame_id;
        const auto base_address  = get_frame_address(frame_id);
        const auto rw_address    = get_frame_address(frame_id, this->rdma_region_rw);

        logger->debug("{} asked for page {}", peer_id, frame_id);

        const auto current_state = this->states[frame_id].load(std::memory_order_acquire);
        if (current_state == state::exclusive || current_state == state::shared_had_owned_page) {
            // no longer exclusive
            if (current_state == state::exclusive) {
                this->set_frame_state(frame_id, state::shared_had_owned_page);
                this->faultfd.write_protect(base_address, page_size);
            }

            // Default if no compressed
            const void* ptr_to_send = rw_address;
            std::size_t size_to_send = page_size;

            constexpr auto data_max_size = LZ4_COMPRESSBOUND(page_size);
            std::uint8_t compressed[data_max_size];

            if (this->use_compression) {
                ptr_to_send = compressed;

                size_to_send = static_cast<std::size_t>(LZ4_compress_default(reinterpret_cast<char*>(rw_address), reinterpret_cast<char*>(compressed), page_size, data_max_size));
                tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                    size_to_send == 0, "Failed to compress frame data"
                );

                logger->trace("Compression {} to {}, {}%", page_size, size_to_send, static_cast<double>(page_size - size_to_send) / page_size * 100);
            }

            if (this->atomic_pub_send(packet::send_page_packet{ .frame_id = frame_id, .size = size_to_send }.to(peer_id), zmq::const_buffer(ptr_to_send, size_to_send))) {
                logger->error("Failed send a frame");
                // timeout will handle the rest and a retransmission should happen
            }
        } else {
            // node with shared or invalid state do nothing
            if (this->atomic_pub_send(packet::send_page_packet{ .frame_id = frame_id, .size = 0 }.to(peer_id))) {
                logger->error("Failed send a dummy frame");
                // timeout will handle the rest and a retransmission should happen
            }
        }
        return false;
    }

    bool handle_send_page(zmq::socket_ref sock, zmq::message_t&, const packet::send_page_packet& msg) final {
        // Someone is sending a data of a page to me
        const auto peer_id  = msg.hdr.from;
        const auto frame_id = msg.frame_id;

        logger->debug("{} sent frame {}", peer_id, frame_id);

        constexpr auto data_max_size = LZ4_COMPRESSBOUND(page_size);

        std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};

        if (this->out_standing_read[frame_id]) {
            const auto base_address = get_frame_address(frame_id);
            const auto writable_address = get_frame_address(frame_id, this->rdma_region_rw);

            if (msg.size > 0) {
                auto address_to_write = writable_address;
                std::uint8_t compressed[data_max_size];

                if (this->use_compression) {
                    address_to_write = compressed;
                }

                const auto nbytes = sock.recv(zmq::mutable_buffer(address_to_write, msg.size), zmq::recv_flags::none);
                if (!nbytes.has_value() || nbytes.value().size != msg.size) {
                    logger->error("Error on receive frame");
                    return has_error;
                }

                if (this->use_compression) {
                    const auto recv_size = LZ4_decompress_safe(reinterpret_cast<char*>(address_to_write), reinterpret_cast<char*>(writable_address), static_cast<int>(msg.size), page_size);
                    if (recv_size == 0) {
                        logger->error("Error on decompression");
                        return has_error;
                    }
                }
            }

            this->read_fencing_set[frame_id].emplace(peer_id);

            if (this->read_fencing_set[frame_id].size() == this->peers.size()) {
                this->out_standing_read[frame_id] = false;
                this->set_frame_state(frame_id, state::shared);
                this->faultfd.write_protect(base_address, page_size);
                this->faultfd.wake(base_address, page_size);
            }
        } else {
            logger->trace("Frame {} received, discarding", frame_id);

            std::uint8_t dummy[std::max(page_size, data_max_size)];

            const auto nbytes = sock.recv(zmq::mutable_buffer(dummy, msg.size), zmq::recv_flags::none);
            if (!nbytes.has_value() || nbytes.value().size != msg.size) {
                logger->error("Error on receive frame");
                return has_error;
            }
        }

        return OK;
    }

    bool handle_my_page(zmq::socket_ref, zmq::message_t&, const packet::my_page_packet& msg) final {
        // Someone is taking the ownership of a page
        const auto peer_id  = msg.hdr.from;
        const auto frame_id = msg.frame_id;

        logger->debug("{} take ownership of frame {}", peer_id, frame_id);

        {
            std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
            if (this->out_standing_read[frame_id]) {
                // Oh oh, we are competing with some other peers, we are read, they are writing
                // Read have priority, ignore the request
                return OK;
            }
        }

        std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};

        if (this->out_standing_write[frame_id] && peer_id > this->my_id) {
            // Oh oh, we are competing with some other peers
            // We have priority, ignore the request, tell them WE OWN THE PAGE!!
            if (this->atomic_pub_send(packet::my_page_packet{ .frame_id = msg.frame_id })) {
                logger->error("Error to notify ownership of frame : {}", frame_id);
                return has_error;
            }
        } else {
            const auto base_address = get_frame_address(frame_id);

            // Give up the ownership, or acknowledge the ownership
            this->set_frame_state(msg.frame_id, state::invalid);
            this->faultfd.write_protect(base_address, page_size);
            this->page_out_page(msg.frame_id);

            if (this->atomic_pub_send(packet::your_page_packet{ .frame_id = msg.frame_id }.to(peer_id))) {
                logger->error("Error to handout ownership of frame : {}", frame_id);
                return has_error;
            }

            if (this->out_standing_write[frame_id]) {
                // Oh oh, we are competing with some other peers
                // Allow peers with smaller id to proceed first to prevent starvation
                // Revert the page state to modified, and restart from there
                this->out_standing_write[frame_id] = false;
                this->faultfd.wake(base_address, page_size);
            }
        }

        return OK;
    }

    bool handle_your_page(zmq::socket_ref, zmq::message_t&, const packet::your_page_packet& msg) final {
        // Someone is responding the request we want to take ownership of the page
        const auto peer_id  = msg.hdr.from;
        const auto frame_id = msg.frame_id;

        logger->debug("{} give ownership of frame {}", peer_id, frame_id);

        std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};
        this->write_fencing_set[frame_id].emplace(peer_id);

        if (this->write_fencing_set[frame_id].size() == this->peers.size()) {
            const auto base_address = get_frame_address(msg.frame_id);
            this->faultfd.write_unprotect(base_address, page_size);

            this->out_standing_write[frame_id] = false;
            this->set_frame_state(msg.frame_id, state::exclusive, std::memory_order_release);

            this->faultfd.wake(base_address, page_size);
        }

        return false;
    }

    // Actual worker thread function
    inline void run() {
        auto& poller = this->peers.get_poller();
        poller.add(this->directory_rpc_endpoint, zmq::event_flags::pollin);
        poller.add_fd(this->faultfd.get(), zmq::event_flags::pollin);
        poller.add_fd(this->thread.evtfd.get(), zmq::event_flags::pollin);

        while (!this->thread.stopped.load(std::memory_order_acquire)) {
            // Wait for page fault or remote events
            std::vector<zmq::poller_event<>> events(poller.size());
            const auto count = poller.wait_all(events, std::chrono::milliseconds{0});

            if (count <= 0) {
                continue;
            }

            bool faulted = false;
            for (auto& event : events) {
                if (event.fd == this->thread.evtfd.get()) {
                    break;
                } else if (event.fd == this->faultfd.get()) {
                    faulted = true;  // page fault are handled last
                    continue;
                } else if (event.socket == nullptr) {
                    continue;
                }

                // Remote messages
                zmq::message_t message;
                const auto nbytes = event.socket.recv(message, zmq::recv_flags::none);
                if (nbytes < sizeof(packet::packet_header)) {
                    logger->error("Received invalid packet. size {}", nbytes.value());
                    continue;
                }

                const auto header  = message.data<packet::packet_header>();
                const auto peer_id = header->from;

                // Default actions
                zmq::message_t id;
                if (this->handle_a_packet(event.socket, id, message)) {
                    logger->debug("swapper connection to peer {} ended!", peer_id);
                }
            }

            if (faulted && this->faultfd.get() != -1) {
                // Read the page fault information
                const auto fault         = swapper::faultfd.read();
                const auto frame_id      = get_frame_number(fault.address);
                const auto base_address  = round_down_to_page_boundary(fault.address);
                const auto current_state = this->states[frame_id].load(std::memory_order_acquire);

                logger->trace("swapper processing frame {} @ {}, state: {}, is_write: {}",
                    frame_id,
                    fault.address,
                    state_to_string(current_state),
                    fault.is_write);

                if (fault.is_missing) {
                    this->set_frame_state(frame_id, state::invalid);
                }

                if (current_state == state::invalid) {
                    // If the page is dirty or not populated, first get it from owner, and make it write protected

                    if (fault.is_missing) {  // UFFD require us to zero or copy into a missing page
                        this->faultfd.zero(base_address, page_size);
                    } else if (fault.is_minor) {
                        this->faultfd.continue_(base_address, page_size);
                    }

                    this->ask_page(frame_id);  // Download the data from the owner, can timeout or fail during owner ship transition
                    // continue after we receive a SEND_PAGE packet
                } else if (current_state == state::shared || current_state == state::shared_had_owned_page) {
                    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(!fault.is_write, "A shared frame trigger a fault, which is not a write");
                    // Take ownership
                    this->own_page(frame_id);

                    if (fault.is_minor) {
                        this->faultfd.continue_(base_address, page_size);
                    }
                    // continue with YOUR_PAGE response handling
                } else if (current_state == state::exclusive) {
                    if (fault.is_minor) {
                        this->faultfd.continue_(base_address, page_size);
                        this->faultfd.wake(base_address, page_size);
                    } else {
                        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(true, "A exclusive frame trigger a fault");
                    }
                }
            }
        }

        poller.remove_fd(this->faultfd.get());
        poller.remove_fd(this->thread.evtfd.get());
        this->peers.clear();
    }

    inline void check_timeout_ask_page() {
        for (frame_id_t frame_id = 0; frame_id < n_pages; ++frame_id) {
            {
                std::scoped_lock<std::mutex> lk{this->read_fencing_mutex[frame_id]};
                if (this->out_standing_read[frame_id] == true) {
                    logger->debug("Timeout, asking frame {} again {}", frame_id, packet::my_id);
                    (void)this->atomic_pub_send(packet::ask_page_packet{ .frame_id = frame_id });
                }
            }
        }
    }

    inline void check_timeout_send_page() {
        for (frame_id_t frame_id = 0; frame_id < n_pages; ++frame_id) {
            {
                std::scoped_lock<std::mutex> lk{this->write_fencing_mutex[frame_id]};
                if (this->out_standing_write[frame_id] == true) {
                    logger->debug("Timeout, taking ownership of frame {} again", frame_id);
                    (void)this->atomic_pub_send(packet::my_page_packet{ .frame_id = frame_id });
                }
            }
        }
    }

    bool handle_sem_put(zmq::socket_ref, zmq::message_t&, const packet::sem_put_packet& msg) final {
        auto& sem = [&]() -> decltype(auto) {
            std::shared_lock<std::shared_mutex> lk(this->sem_list_mutex);
            if (!this->sem_list.contains(msg.address)) {
                lk.unlock();
                std::unique_lock<std::shared_mutex> lk2(this->sem_list_mutex);
                if (!this->sem_list.contains(msg.address)) {
                    this->sem_list[msg.address] = std::make_unique<sem_semaphore>(0);
                }
                return this->sem_list[msg.address];
            }

            return this->sem_list[msg.address];
        }();

        sem_logger->trace("Semaphore: put {}", msg.address);

        sem->release(1);
        return OK;
    }

    bool handle_new_sem(zmq::socket_ref, zmq::message_t&, const packet::new_sem_packet& msg) final {
        const auto address = msg.address;
        const auto request_num = msg.request_num;

        // Must be first touch
        {
            std::unique_lock<std::shared_mutex> lk(this->sem_list_mutex);
            this->sem_list[address] = std::make_unique<sem_semaphore>(0);
        }

        sem_logger->trace("Semaphore: made {}", address);

        std::scoped_lock<std::mutex> lk(this->out_standing_make_sem_mutex);
        this->out_standing_make_sem[request_num]->address = address;
        this->out_standing_make_sem[request_num]->sem.release(1);

        return OK;
    }
};

}  // namespace tDSM
