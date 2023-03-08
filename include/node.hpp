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

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <semaphore>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "fmt/core.h"
#include "spdlog/spdlog.h"
#include "zmq.hpp"

#include "configs.hpp"
#include "packet.hpp"
#include "sys/fd.hpp"
#include "sys/epoll.hpp"
#include "sys/timerfd.hpp"
#include "utils/cancelable_thread.hpp"
#include "utils/logging.hpp"

namespace tDSM {

namespace rpc {

struct rpc_adapter_base;
zmq::message_t dispatch_rpc(const std::size_t action, const zmq::message_t& args);

} // namesapce rpc

namespace details {

enum static_semaphore : std::uintptr_t {
    allocator_lock,
    static_semaphore_count
};

class peers {
 public:
    using id_t = std::size_t;

    template<typename... Ts>
    inline auto add(const id_t id, zmq::socket_t&& endpoint, Ts&&... ts) {
        std::scoped_lock<std::mutex> lk{this->mutex};
        poller.add(endpoint, zmq::event_flags::pollin);
        this->peers.emplace(id, peer{std::move(endpoint), std::forward<Ts>(ts)...});
        this->condvar.notify_all();
    }

    inline auto del(const id_t id) {
        std::scoped_lock<std::mutex> lk{this->mutex};
        poller.remove(this->peers[id].endpoint);
        this->peers.erase(id);
    }

    inline auto clear() {
        std::scoped_lock<std::mutex> lk{this->mutex};
        for (auto& [id, peer] : this->peers) {
            poller.remove(peer.endpoint);
        }
        this->peers.clear();
    }

    inline auto& get_poller() {
        return this->poller;
    }

    inline auto& get_peers() {
        return this->peers;
    }

    inline auto size() const {
        return this->peers.size();
    }

    inline auto wait_for_peer(const id_t peer_id) {
        std::unique_lock<std::mutex> lk{this->mutex};
        this->condvar.wait(lk, [&] { return this->peers.contains(peer_id); });
    }

    ~peers() {
        this->clear();
    }

    struct peer {
        zmq::socket_t endpoint;
        std::string addr;
        std::uint16_t port;
    };

 private:
    mutable std::mutex mutex{};
    mutable std::condition_variable condvar{};
    std::unordered_map<id_t, peer> peers{};
    zmq::poller_t<> poller;
};

} // namesapce details

static inline constexpr auto directory_rpc_port = 9634;
static inline constexpr auto directory_pub_port = directory_rpc_port + 1;

using id_t = std::size_t;
using frame_id_t = std::size_t;

class Context {
 public:
    static inline Context& get() {
        static Context instance;
        return instance;
    }

    virtual ~Context() {}

    zmq::context_t zmq_ctx;
 private:
     Context() {}
};

class Node {
 public:
    Node() {}

    virtual ~Node() {}

    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

    bool use_compression = false;

    static constexpr auto has_error = true;
    static constexpr auto OK = false;

    inline auto get_id() const {
        return this->my_id;
    }

 protected:
    id_t my_id{};

    // Receiving
    inline void handle(zmq::socket_ref sock, utils::cancelable_thread& this_thread) {
        zmq::poller_t<> poller;
        poller.add(sock, zmq::event_flags::pollin);
        poller.add_fd(this_thread.evtfd.get(), zmq::event_flags::pollin);

        while (!this_thread.stopped.load(std::memory_order_acquire)) {
            // Wait for event
            std::vector<zmq::poller_event<>> events(poller.size());
            const auto count = poller.wait_all(events, std::chrono::milliseconds{0});

            if (count <= 0) {
                continue;
            }

            for (auto& event : events) {
                if (event.socket == nullptr) {
                    continue;
                }

                zmq::message_t message;
                const auto nbyte = event.socket.recv(message, zmq::recv_flags::none);
                if (nbyte < sizeof(packet::packet_header) || this->handle_a_packet(event.socket, message)) {
                    this_thread.stopped.store(true, std::memory_order_relaxed);
                    break;
                }
            }
        }

        // Don't care even this failed, the socket might be a pub-sub pair
        try {
            (void)sock.send(packet::disconnect_packet{}, zmq::send_flags::none);
        } catch(zmq::error_t&) {
        }

        poller.remove(sock);
        poller.remove_fd(this_thread.evtfd.get());
    }

    template<typename T>
    inline auto forward_packet(zmq::socket_ref sock, const zmq::message_t& message, const auto& func) {
        const auto packet = message.data<T>();
        return (this->*func)(sock, *packet);
    }

    inline bool handle_a_packet(zmq::socket_ref sock, const zmq::message_t& message) {
        bool err = false;
        bool ret = false;

        const auto packet = message.data<packet::packet_header>();
        const auto type = packet->type;
        const auto to = packet->to;
        const auto from = packet->from;

        if (type >= packet::packet_type::max_types) {
            logger->debug("Unknown packet type {}", static_cast<std::uint32_t>(type));
            return ret;
        }

        logger->debug("Handling packet: {} from {} to {}", packet::packet_type_to_string(type), from, to);

        if (to != this->my_id && to != 0) {
            logger->debug("Not packet for me, discarding {} from {} to {}", packet::packet_type_to_string(type), from, to);
            if (message.more()) {
                zmq::message_t more_msg;
                do {
                    const auto nbyte = sock.recv(more_msg, zmq::recv_flags::none);
                    if (!nbyte.has_value()) {
                        break;
                    }
                } while (more_msg.more());
            }
            return ret;
        }

#define HANDLE(X) case packet::packet_type::X: err = forward_packet<packet::X##_packet>(sock, message, &Node::handle_##X); break

        switch (type) {
            case packet::packet_type::disconnect:
                // No need to recv, socket is close by remote
                logger->trace("Peer disconnected!");
                (void)forward_packet<packet::disconnect_packet>(sock, message, &Node::handle_disconnect);
                ret = true;
                break;
            HANDLE(noop);
            HANDLE(connect);
            HANDLE(configure);
            HANDLE(register_peer);
            HANDLE(my_id);
            HANDLE(ask_page);
            HANDLE(send_page);
            HANDLE(my_page);
            HANDLE(your_page);
            HANDLE(make_sem);
            HANDLE(new_sem);
            HANDLE(sem_get);
            HANDLE(sem_put);
            HANDLE(call);
            HANDLE(ret);
            HANDLE(call_ack);
            HANDLE(ret_ack);
            case packet::packet_type::max_types:
                break;  // Should never happen
        }

#undef HANDLE

        if (err) {
            logger->error("Reading packet type {} from {} to {} cause an error!", packet::packet_type_to_string(type), from, to);
        }
        return ret;
    }

#define HANDLER(X) \
    virtual bool handle_##X(zmq::socket_ref, const packet::X##_packet&) { \
        logger->warn("Ignoring a X packet"); \
        return OK; \
    }

    HANDLER(noop)
    HANDLER(disconnect)
    HANDLER(connect)
    HANDLER(configure)
    HANDLER(register_peer)
    HANDLER(my_id)
    HANDLER(ask_page)
    HANDLER(send_page)
    HANDLER(my_page)
    HANDLER(your_page)
    HANDLER(make_sem)
    HANDLER(new_sem)
    HANDLER(sem_get)
    HANDLER(sem_put)
    HANDLER(call)
    HANDLER(ret)
    HANDLER(call_ack)
    HANDLER(ret_ack)

#undef HANDLER
};

class master_node : public Node {
 public:
    static master_node& get() {
        static master_node instance;
        return instance;
    }

    inline auto initialize(const bool use_compression_) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(initialized, "Cannot initialize twice");

        this->use_compression = use_compression_;

        initialized = true;
    }

 private:
    master_node() {
        const auto local_pub_uri = fmt::format("tcp://*:{}", directory_pub_port);
        pub_endpoint.bind(local_pub_uri);

        const auto local_rpc_uri = fmt::format("tcp://*:{}", directory_rpc_port);
        rpc_endpoint.bind(local_rpc_uri);
        this->rpc_thread = std::thread([this] { this->handle(this->rpc_endpoint, this->rpc_thread); });
    }
    ~master_node() override {}

    master_node(const master_node&) = delete;
    master_node& operator=(const master_node&) = delete;
    master_node(master_node&&) = delete;
    master_node& operator=(master_node&&) = delete;

    bool initialized = false;

    zmq::socket_t pub_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::pub);
    zmq::socket_t rpc_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::rep);
    utils::cancelable_thread rpc_thread{};

    std::atomic_size_t number_of_peers{1};  // Number 0 is reserved as id not set.
    struct peer {
        std::string addr;
        std::uint16_t port;
    };
    mutable std::mutex peers_mutex{};
    std::unordered_map<id_t, peer> peers{};

    bool handle_disconnect(zmq::socket_ref, const packet::disconnect_packet& msg) final {
        std::scoped_lock<std::mutex> lk(this->peers_mutex);

        const auto peer_id = msg.hdr.from;
        peers.erase(peer_id);
        return OK;
    }

    bool handle_connect(zmq::socket_ref, const packet::connect_packet& msg) final {
        const auto peer_id = number_of_peers.fetch_add(1);
        logger->trace("Accepting new peer : ID: {}", peer_id);

        // One at a time, no race
        {
            const auto nbytes = this->rpc_endpoint.send(packet::configure_packet{ .peer_id = peer_id, .use_compression = use_compression }, zmq::send_flags::none);
            if (nbytes != sizeof(packet::configure_packet)) {
                return has_error;
            }
        }

        {
            std::scoped_lock<std::mutex> lk(this->peers_mutex);
            for (const auto& [id, peer] : this->peers) {
                packet::register_peer_packet pkt;
                pkt.peer_id = id;
                std::copy_n(peer.addr.c_str(), sizeof(pkt.addr), pkt.addr);
                pkt.port = peer.port;

                (void)this->pub_endpoint.send(pkt, zmq::send_flags::none);
            }

            peer p;
            this->peers.emplace(peer_id, peer{msg.addr, msg.port});
        }

        packet::register_peer_packet pkt;
        pkt.peer_id = peer_id;
        std::copy_n(msg.addr, sizeof(pkt.addr), pkt.addr);
        pkt.port = msg.port;
        (void)this->pub_endpoint.send(pkt, zmq::send_flags::none);

        return OK;
    }

    // semaphore list
    std::atomic_uint64_t free_sem_head{details::static_semaphore::static_semaphore_count};
    mutable std::shared_mutex sem_queues_mutex{};
    struct sem_state {
        mutable std::mutex mutex{};
        std::size_t count{};
        std::deque<id_t> queue{};
    };
    std::unordered_map<std::uintptr_t, sem_state> sem_queues{};

    bool handle_make_sem(zmq::socket_ref, const packet::make_sem_packet& msg) final {
        logger->trace("Creating new sem from ID: {}", msg.hdr.from);
        const auto new_sem_address = this->free_sem_head.fetch_add(1);

        {
            std::unique_lock<std::shared_mutex> lk(this->sem_queues_mutex);
            this->sem_queues[new_sem_address].count = msg.initial_count;
        }

        {
            const auto nbytes = this->rpc_endpoint.send(packet::new_sem_packet{ .address = new_sem_address }, zmq::send_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::new_sem_packet), "Failed to send new semaphore address");
        }

        return OK;
    }

    bool handle_sem_get(zmq::socket_ref, const packet::sem_get_packet& msg) final {
        logger->trace("Queue sem get at 0x{:x} from ID: {}", msg.address, msg.hdr.from);

        {
            const auto nbytes = this->rpc_endpoint.send(packet::noop_packet{}, zmq::send_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::noop_packet), "Failed to send noop for sem_get");
        }

        std::shared_lock<std::shared_mutex> lk(this->sem_queues_mutex);
        std::unique_lock<std::mutex> lk2(this->sem_queues[msg.address].mutex);
        if (this->sem_queues[msg.address].count > 0) {
            this->sem_queues[msg.address].count--;

            const auto nbytes = this->pub_endpoint.send(packet::sem_put_packet{ .address = msg.address }.to(msg.hdr.from), zmq::send_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::sem_put_packet), "Failed to send sem_put");
        } else {
            this->sem_queues[msg.address].queue.emplace_back(msg.hdr.from);
        }

        return OK;
    }

    bool handle_sem_put(zmq::socket_ref, const packet::sem_put_packet& msg) final {
        logger->trace("sem put at 0x{:x} from ID: {}", msg.address, msg.hdr.from);

        {
            const auto nbytes = this->rpc_endpoint.send(packet::noop_packet{}, zmq::send_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::noop_packet), "Failed to send noop for sem_put");
        }

        std::shared_lock<std::shared_mutex> lk(this->sem_queues_mutex);
        std::unique_lock<std::mutex> lk2(this->sem_queues[msg.address].mutex);
        if (this->sem_queues[msg.address].queue.empty()) {
            this->sem_queues[msg.address].count++;
        } else {
            const auto id_to_wake = this->sem_queues[msg.address].queue.front();
            this->sem_queues[msg.address].queue.pop_front();

            const auto nbytes = this->pub_endpoint.send(packet::sem_put_packet{ .address = msg.address }.to(id_to_wake), zmq::send_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::sem_put_packet), "Failed to send sem_put");
        }

        return OK;
    }
};

class peer_node : public Node {
 public:
    static peer_node& get() {
        static peer_node instance;
        return instance;
    }

    inline auto call(const std::size_t action, const std::size_t remote, zmq::message_t&& args) {
        const auto call_id = call_counter.fetch_add(1);

        // Create rendezvous
        auto& rendezvous = [&]() -> decltype(auto) {
            std::scoped_lock<std::mutex> lk(this->outstanding_rpcs_mutex);
            return this->outstanding_rpcs[call_id] = std::make_unique<rpc_rendezvous>();
        }();

        rendezvous->call_id = call_id;
        rendezvous->remote = remote;
        rendezvous->action = action;
        rendezvous->args.copy(args);

        // Send call msg
        logger->debug("Call of RPC action {}, ID {} {}", action, call_id, args.size());
        // Error handled by timeout
        (void)this->atomic_pub_send(packet::call_packet{ .call_id = call_id, .action = action, .size = args.size() }.to(remote), args);

        rendezvous->called.store(true, std::memory_order_release);

        // wait for result
        logger->debug("Wait return of RPC action {}, ID {}", action, call_id);
        rendezvous->sem.acquire();
        auto ret = std::move(rendezvous->ret);

        {  // Delete the rendezvous
            std::scoped_lock<std::mutex> lk(this->outstanding_rpcs_mutex);
            this->outstanding_rpcs.erase(call_id);
        }

        return ret;
    }

 protected:
    peer_node() {
        for (auto i = 0u; i < this->number_of_rpc_thread; i++) {
            this->rpc_thread_pool.emplace_back(std::thread([this] { this->rpc_runner(); }));
        }
    }
    ~peer_node() override {
        if (initialized) {
            {
                const auto nbytes = this->directory_rpc_endpoint.send(packet::disconnect_packet{}, zmq::send_flags::none);
                tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::disconnect_packet), "Failed to disconnect from directory");
            }
            this->atomic_pub_send(packet::disconnect_packet{});
        }

        {
            std::scoped_lock<std::mutex> lk(this->rpc_queue_mutex);
            for (auto& thread : this->rpc_thread_pool) {
                (void)thread;
                this->rpc_request_queue.emplace_back(rpc_request{true, 0, 0, 0, zmq::message_t{}});
                this->rpc_queue_condvar.notify_one();
            }
        }
        for (auto& thread : this->rpc_thread_pool) {
            thread.join();
        }
    }

    peer_node(const peer_node&) = delete;
    peer_node& operator=(const peer_node&) = delete;
    peer_node(peer_node&&) = delete;
    peer_node& operator=(peer_node&&) = delete;

    details::peers peers{};
    std::string directory_addr;
    std::string my_addr;
    std::uint16_t my_port;
    bool initialized = false;

    mutable std::mutex pub_endpoint_mutex;
    zmq::socket_t pub_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::pub);
    zmq::socket_t directory_sub_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::sub);
    zmq::socket_t directory_rpc_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::req);
    utils::cancelable_thread directory_sub_thread{};

    inline bool atomic_pub_send(const zmq::const_buffer& msg) {
        std::scoped_lock<std::mutex> lk(this->pub_endpoint_mutex);
        const auto nbytes = this->pub_endpoint.send(msg, zmq::send_flags::none);
        return nbytes != msg.size();
    }

    inline bool atomic_pub_send(const zmq::const_buffer& msg, zmq::message_t& payload) {
        std::scoped_lock<std::mutex> lk(this->pub_endpoint_mutex);
        const auto flags = payload.size() != 0 ? zmq::send_flags::sndmore : zmq::send_flags::none;
        const auto nbytes = this->pub_endpoint.send(msg, flags);
        if (!(nbytes != msg.size()) && payload.size() != 0) {
            // Destination id followed the header above
            const auto payloadbytes = this->pub_endpoint.send(payload, zmq::send_flags::none).value();
            return payloadbytes != payload.size();
        }
        return true;
    }

    inline bool atomic_pub_send(const zmq::const_buffer& msg, const zmq::const_buffer& payload) {
        std::scoped_lock<std::mutex> lk(this->pub_endpoint_mutex);
        const auto flags = payload.size() != 0 ? zmq::send_flags::sndmore : zmq::send_flags::none;
        const auto nbytes = this->pub_endpoint.send(msg, flags);
        if (!(nbytes != msg.size()) && payload.size() != 0) {
            // Destination id followed the header above
            const auto payloadbytes = this->pub_endpoint.send(payload, zmq::send_flags::none).value();
            return payloadbytes != payload.size();
        }
        return true;
    }

    inline auto initialize(const std::string& directory_addr_, const std::string& my_addr_, const std::uint16_t my_port_) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(initialized, "Cannot initialize twice");

        this->directory_addr = directory_addr_;
        this->my_addr = my_addr_;
        this->my_port = my_port_;

        const auto local_pub_uri = fmt::format("tcp://*:{}", my_port_);
        pub_endpoint.bind(local_pub_uri);

        logger->info("Connecting to directory {}:{}", directory_addr, directory_rpc_port);

        const auto directory_sub_uri = fmt::format("tcp://{}:{}", directory_addr, directory_pub_port);
        directory_sub_endpoint.connect(directory_sub_uri);
        packet::subscribe_to_id(directory_sub_endpoint, 0);

        const auto directory_rpc_uri = fmt::format("tcp://{}:{}", directory_addr, directory_rpc_port);
        directory_rpc_endpoint.connect(directory_rpc_uri);

        {
            packet::connect_packet pkt;
            std::strncpy(pkt.addr, this->my_addr.c_str(), sizeof(pkt.addr) - 1);
            pkt.port = this->my_port;
            const auto nbytes = this->directory_rpc_endpoint.send(pkt, zmq::send_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(pkt), "Failed to connect to directory");
        }
        {
            zmq::message_t message;
            const auto nbytes = this->directory_rpc_endpoint.recv(message, zmq::recv_flags::none);
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::configure_packet), "Failed configuration");

            const auto msg = message.data<packet::configure_packet>();
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(msg->hdr.type != packet::packet_type::configure, "Failed configuration, packet invalid");

            this->my_id = msg->peer_id;
            packet::my_id = this->my_id;
            logger->trace("My ID is assigned as {}", this->my_id);

            assert(tDSM::packet::my_id != 0);

            use_compression = msg->use_compression;
            if (use_compression) {
                logger->trace("Compression enabled");
            }
        }

        // Start the processing thread after configuration, otherwise will race before acquiring our ID and subscribe to ourself because of the bypass guard failing in handle_register_peer
        // Only subscribe to those packets for ID 0 and my_id
        packet::subscribe_to_id(directory_sub_endpoint, this->my_id);
        this->directory_sub_thread = std::thread([this] { this->handle(this->directory_sub_endpoint, this->directory_sub_thread); });

        initialized = true;
    }

    // Transmission timeout monitor
    utils::cancelable_thread timeout_monitor_thread = std::thread([this] { this->timeout_monitor(); });
    sys::timer_fd timeout_timer_fd{};
    mutable std::mutex additional_timeout_handler_mutex;
    std::deque<std::function<void()>> additional_timeout_handler{
        [this] { this->check_rpc_call_ack(); },
        [this] { this->check_rpc_ret_ack(); }
    };

    inline void timeout_monitor() {
        this->timeout_timer_fd.periodic(
            timespec {
                .tv_sec = 1,
                .tv_nsec = 0
            },
            timespec {
                .tv_sec = 5,
                .tv_nsec = 0
            }
        );

        const auto epollfd = sys::epoll(this->timeout_timer_fd, this->timeout_monitor_thread.evtfd);
        while (!this->timeout_monitor_thread.stopped.load(std::memory_order_acquire)) {
            // Wait for page fault or remote call
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || epollfd.check_fd_in_result(events, this->timeout_monitor_thread.evtfd)) {
                continue;
            }

            std::uint64_t expire_count;
            const auto nbytes = ::read(this->timeout_timer_fd.get(), &expire_count, sizeof(expire_count));
            if (nbytes != sizeof(expire_count)) {
                logger->error("Reading from timer fd failed! : {}", strerror(errno));
                continue;
            }

            {
                std::scoped_lock<std::mutex> lk(this->additional_timeout_handler_mutex);
                for (const auto& handler : this->additional_timeout_handler) {
                    handler();
                }
            }
        }
    }

 private:
    bool handle_disconnect(zmq::socket_ref, const packet::disconnect_packet& msg) final {
        const auto peer_id = msg.hdr.from;
        peers.del(peer_id);
        return OK;
    }

    bool handle_register_peer(zmq::socket_ref, const packet::register_peer_packet& msg) final {
        if (msg.peer_id == this->my_id || this->peers.get_peers().contains(msg.peer_id)) {  // no loop back, and duplication
            return OK;
        }

        logger->info("Registering and subscribing to a new peer : {}:{}", msg.addr, msg.port);

        zmq::socket_t sub_endpoint(Context::get().zmq_ctx, zmq::socket_type::sub);
        const auto sub_uri = fmt::format("tcp://{}:{}", msg.addr, msg.port);
        sub_endpoint.connect(sub_uri);
        // Only subscribe to those packets for ID 0 and my_id
        sub_endpoint.set(zmq::sockopt::subscribe, "");
        //packet::subscribe_to_id(sub_endpoint, 0);
        //packet::subscribe_to_id(sub_endpoint, this->my_id);

        logger->trace("New peer {} at {}:{} subscribed!", msg.peer_id, msg.addr, msg.port);

        // Add to list of peers
        this->peers.add(msg.peer_id, std::move(sub_endpoint), msg.addr, msg.port);

        return OK;
    }

    // User defined rpc
    // Caller
    std::atomic_size_t call_counter{1};
    struct rpc_rendezvous {
        std::atomic_bool called{false};
        std::atomic_bool ack{false};
        std::size_t call_id;
        std::size_t remote;
        std::size_t action;
        zmq::message_t args;
        std::binary_semaphore sem{0};
        zmq::message_t ret;
    };
    mutable std::mutex outstanding_rpcs_mutex{};
    std::unordered_map<std::size_t, std::unique_ptr<rpc_rendezvous>> outstanding_rpcs{};

    // Callee
    static inline const auto number_of_rpc_thread = std::thread::hardware_concurrency();
    std::deque<std::thread> rpc_thread_pool;
    mutable std::mutex rpc_queue_mutex;
    mutable std::condition_variable rpc_queue_condvar;
    struct rpc_request {
        const bool stop = false;
        const std::size_t call_id;
        const std::size_t remote;
        const std::size_t action;
        zmq::message_t args;
    };
    std::deque<rpc_request> rpc_request_queue;
    std::unordered_map<id_t, std::size_t> call_id_tracker;

    mutable std::mutex outstanding_rpc_rets_mutex{};
    std::unordered_map<id_t, std::unordered_map<std::size_t, zmq::message_t>> outstanding_rpc_rets{};

    void rpc_runner() {
        while (true) {
            std::unique_lock<std::mutex> lk(this->rpc_queue_mutex);

            this->rpc_queue_condvar.wait(lk, [&] { return !this->rpc_request_queue.empty(); });

            const auto req = std::move(this->rpc_request_queue.front());
            this->rpc_request_queue.pop_front();

            lk.unlock();  // Execute RPC request without lock

            if (req.stop) {
                break;
            }

            logger->debug("Run RPC ID {} for remote {}/{}", req.action, req.remote, req.call_id);

            // Call dispatcher -> proxy
            auto ret = rpc::dispatch_rpc(req.action, req.args);
            zmq::message_t ret_clone;
            ret_clone.copy(ret);

            // Send return
            // Further error handled by timeout
            (void)this->atomic_pub_send(packet::ret_packet{ .call_id = req.call_id, .size = ret.size() }.to(req.remote), ret);

            std::scoped_lock<std::mutex> lk2(this->outstanding_rpc_rets_mutex);  // prevent deletion
            this->outstanding_rpc_rets[req.remote][req.call_id] = std::move(ret_clone);
        }
    }

    bool handle_call(zmq::socket_ref sock, const packet::call_packet& msg) final {
        std::unique_lock<std::mutex> lk(this->rpc_queue_mutex);

        // Did the call came?
        if (call_id_tracker[msg.hdr.from] >= msg.call_id) {
            return has_error;
        }

        call_id_tracker[msg.hdr.from] = msg.call_id;

        auto req = rpc_request{false, msg.call_id, msg.hdr.from, msg.action, zmq::message_t{}};

        if (msg.size > 0) {
            const auto nbytes = sock.recv(req.args, zmq::recv_flags::none);
            if (nbytes != msg.size) {
                logger->error("Error on receive ret");
                return has_error;
            }
        }

        logger->debug("Enqueue RPC action {} for remote {}/{}", req.action, req.remote, req.call_id);
        this->rpc_request_queue.emplace_back(std::move(req));

        this->rpc_queue_condvar.notify_one();

        return this->atomic_pub_send(packet::call_ack_packet{ .call_id = msg.call_id }.to(msg.hdr.from));
    }

    bool handle_call_ack(zmq::socket_ref, const packet::call_ack_packet& msg) final {
        std::scoped_lock<std::mutex> lk(this->outstanding_rpcs_mutex);  // prevent deletion or insertion
        if (!this->outstanding_rpcs.contains(msg.call_id)) {
            return has_error;
        }

        this->outstanding_rpcs[msg.call_id]->ack.store(true);

        return OK;
    }

    bool handle_ret(zmq::socket_ref sock, const packet::ret_packet& msg) final {
        std::unique_lock<std::mutex> lk(this->outstanding_rpcs_mutex);
        if (!this->outstanding_rpcs.contains(msg.call_id)) {
            logger->warn("Return for RPC ID {} read multiple times!", msg.call_id);
            return has_error;
        }

        auto& rendezvous = this->outstanding_rpcs[msg.call_id];
        rendezvous->ack = true;
        lk.unlock();  // Reference grabbed, delete of rendezvous depends on this function, no race condition

        // Read the return value
        if (msg.size > 0) {
            const auto nbytes = sock.recv(rendezvous->ret, zmq::recv_flags::none);
            if (nbytes != msg.size) {
                logger->error("Error on receive ret");
                return has_error;
            }
        }

        logger->debug("Return of RPC ID {}", msg.call_id);
        // Let the original thread continue
        rendezvous->sem.release();

        return this->atomic_pub_send(packet::ret_ack_packet{ .call_id = msg.call_id }.to(msg.hdr.from));
    }

    bool handle_ret_ack(zmq::socket_ref, const packet::ret_ack_packet& msg) final {
        std::scoped_lock<std::mutex> lk(this->outstanding_rpc_rets_mutex);  // prevent deletion
        if (!this->outstanding_rpc_rets.contains(msg.hdr.from) || !this->outstanding_rpc_rets[msg.hdr.from].contains(msg.call_id)) {
            return has_error;
        }

        this->outstanding_rpc_rets[msg.hdr.from].erase(msg.call_id);

        return OK;
    }

    inline void check_rpc_call_ack() {
        std::scoped_lock<std::mutex> lk(this->outstanding_rpcs_mutex);  // prevent deletion and insertion
        for (auto& [id, rendezvous] : this->outstanding_rpcs) {
            if (!rendezvous->ack && rendezvous->called) {
                zmq::message_t clone;
                clone.copy(rendezvous->args);
                (void)this->atomic_pub_send(packet::call_packet{ .call_id = rendezvous->call_id, .action = rendezvous->action, .size = rendezvous->args.size() }.to(rendezvous->remote), clone);
            }
        }
    }

    inline void check_rpc_ret_ack() {
        std::scoped_lock<std::mutex> lk(this->outstanding_rpc_rets_mutex);  // prevent deletion and insertion

        for (auto& [remote_id, list_of_rets] : this->outstanding_rpc_rets) {
            for (auto& [id, ret] : list_of_rets) {
                zmq::message_t clone;
                clone.copy(ret);
                (void)this->atomic_pub_send(packet::ret_packet{ .call_id = id, .size = ret.size() }.to(remote_id), clone);
            }
        }
    }
};

}  // namespace tDSM
