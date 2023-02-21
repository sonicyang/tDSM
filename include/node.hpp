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
#include "utils/cancelable_thread.hpp"
#include "utils/logging.hpp"

namespace tDSM {

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

 protected:
    static constexpr auto has_error = true;
    static constexpr auto OK = false;

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

        // Don't care even this failed
        (void)sock.send(packet::disconnect_packet{}, zmq::send_flags::dontwait);

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

        spdlog::debug("Handling packet: {}", packet::packet_type_to_string(type));

#define HANDLE(X) case packet::packet_type::X: err = forward_packet<packet::X##_packet>(sock, message, &Node::handle_##X); break

        switch (type) {
            case packet::packet_type::disconnect:
                // No need to recv, socket is close by remote
                spdlog::info("Peer disconnected!");
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
            HANDLE(sem_get);
            HANDLE(sem_put);
        }

#undef HANDLE

        if (err) {
            spdlog::error("Reading packet type {} cause an error!", packet::packet_type_to_string(type));
        }
        return ret;
    }

#define HANDLER(X) \
    virtual bool handle_##X(zmq::socket_ref, const packet::X##_packet&) { \
        spdlog::warn("Ignoring a X packet"); \
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
    HANDLER(sem_get)
    HANDLER(sem_put)

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
        spdlog::trace("Accepting new peer : ID: {}", peer_id);

        // One at a time, no race
        const auto nbytes = this->rpc_endpoint.send(packet::configure_packet{ .peer_id = peer_id, .use_compression = use_compression }, zmq::send_flags::dontwait);
        if (nbytes != sizeof(packet::configure_packet)) {
            return has_error;
        }

        {
            std::scoped_lock<std::mutex> lk(this->peers_mutex);
            for (const auto& [id, peer] : this->peers) {
                while (true) {
                    packet::register_peer_packet pkt;
                    pkt.peer_id = id;
                    std::copy_n(peer.addr.c_str(), sizeof(pkt.addr), pkt.addr);
                    pkt.port = peer.port;

                    const auto nbytes = this->pub_endpoint.send(pkt, zmq::send_flags::dontwait);
                    if (nbytes == sizeof(pkt)) {
                        break;
                    }
                }
            }

            peer p;
            this->peers.emplace(peer_id, peer{msg.addr, msg.port});
        }

        packet::register_peer_packet pkt;
        pkt.peer_id = peer_id;
        std::copy_n(msg.addr, sizeof(pkt.addr), pkt.addr);
        pkt.port = msg.port;
        this->pub_endpoint.send(pkt, zmq::send_flags::dontwait);

        return OK;
    }

    // semaphore list
    mutable std::shared_mutex sem_queues_mutex{};
    struct sem_state {
        mutable std::mutex mutex{};
        std::size_t count{};
        std::deque<id_t> queue{};
    };
    std::unordered_map<std::uintptr_t, sem_state> sem_queues{};

    bool handle_sem_get(zmq::socket_ref, const packet::sem_get_packet& msg) final {
        spdlog::trace("Queue sem get at 0x{:x} from ID: {}", msg.address, msg.hdr.from);

        {
            const auto nbytes = this->rpc_endpoint.send(packet::noop_packet{});
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::noop_packet), "Failed to send noop for sem_get");
        }

        std::shared_lock<std::shared_mutex> lk(this->sem_queues_mutex);
        if (!this->sem_queues.contains(msg.address)) {
            lk.unlock();
            std::unique_lock<std::shared_mutex> lk2(this->sem_queues_mutex);
            if (!this->sem_queues.contains(msg.address)) {
                this->sem_queues[msg.address].count = 0;
            }
            lk2.unlock();
            lk.lock();
        }

        std::unique_lock<std::mutex> lk2(this->sem_queues[msg.address].mutex);
        if (this->sem_queues[msg.address].count > 0) {
            this->sem_queues[msg.address].count--;

            const auto nbytes = this->pub_endpoint.send(packet::sem_put_packet{ .address = msg.address }.to(msg.hdr.from));
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::sem_put_packet), "Failed to send sem_put packet");
        } else {
            this->sem_queues[msg.address].queue.emplace_back(msg.hdr.from);
        }

        return OK;
    }

    bool handle_sem_put(zmq::socket_ref, const packet::sem_put_packet& msg) final {
        spdlog::trace("sem put at 0x{:x} from ID: {}", msg.address, msg.hdr.from);

        {
            const auto nbytes = this->rpc_endpoint.send(packet::noop_packet{});
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::noop_packet), "Failed to send noop for sem_put");
        }

        std::shared_lock<std::shared_mutex> lk(this->sem_queues_mutex);
        if (!this->sem_queues.contains(msg.address)) {
            lk.unlock();
            std::unique_lock<std::shared_mutex> lk2(this->sem_queues_mutex);
            if (!this->sem_queues.contains(msg.address)) {
                this->sem_queues[msg.address].count = 0;
            }
            lk2.unlock();
            lk.lock();
        }

        std::unique_lock<std::mutex> lk2(this->sem_queues[msg.address].mutex);
        if (this->sem_queues[msg.address].queue.empty()) {
            this->sem_queues[msg.address].count++;
        } else {
            const auto id_to_wake = this->sem_queues[msg.address].queue.front();
            this->sem_queues[msg.address].queue.pop_front();

            const auto nbytes = this->pub_endpoint.send(packet::sem_put_packet{ .address = msg.address }.to(id_to_wake));
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::sem_put_packet), "Failed to send sem_put packet");
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

 protected:
    peer_node() {}
    ~peer_node() override {
        if (initialized) {
            {
                const auto nbytes = this->directory_rpc_endpoint.send(packet::disconnect_packet{}, zmq::send_flags::dontwait);
                tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::disconnect_packet), "Failed to disconnect from directory");
            }
            {
                const auto nbytes = this->pub_endpoint.send(packet::disconnect_packet{}, zmq::send_flags::dontwait);
                tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(nbytes != sizeof(packet::disconnect_packet), "Failed to publish disconnect");
            }
        }
    }

    peer_node(const peer_node&) = delete;
    peer_node& operator=(const peer_node&) = delete;
    peer_node(peer_node&&) = delete;
    peer_node& operator=(peer_node&&) = delete;

    class peers {
     public:
        template<typename... Ts>
        inline auto add(const id_t id, zmq::socket_t&& endpoint, Ts&&... ts) {
            std::scoped_lock<std::mutex> lk{this->mutex};
            poller.add(endpoint, zmq::event_flags::pollin);
            this->peers.emplace(id, peer{std::move(endpoint), std::forward<Ts>(ts)...});
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
        std::unordered_map<id_t, peer> peers{};
        zmq::poller_t<> poller;

        //std::optional<peer*> operator[](const int fd_to_find) {
            //std::scoped_lock<std::mutex> lk{this->mutex};
            //if (this->peers.contains(fd_to_find)) {
                //return {&this->peers.at(fd_to_find)};
            //} else {
                //return {};
            //}
        //}
    };

    peers peers{};
    id_t my_id{};
    std::string directory_addr;
    std::string my_addr;
    std::uint16_t my_port;
    bool initialized = false;

    zmq::socket_t pub_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::pub);

    zmq::socket_t directory_sub_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::sub);
    zmq::socket_t directory_rpc_endpoint = zmq::socket_t(Context::get().zmq_ctx, zmq::socket_type::req);
    utils::cancelable_thread directory_sub_thread{};

    inline auto initialize(const std::string& directory_addr_, const std::string& my_addr_, const std::uint16_t my_port_) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(initialized, "Cannot initialize twice");

        this->directory_addr = directory_addr_;
        this->my_addr = my_addr_;
        this->my_port = my_port_;

        const auto local_pub_uri = fmt::format("tcp://*:{}", my_port_);
        pub_endpoint.bind(local_pub_uri);

        spdlog::info("Connecting to directory {}:{}", directory_addr, directory_rpc_port);

        const auto directory_sub_uri = fmt::format("tcp://{}:{}", directory_addr, directory_pub_port);
        directory_sub_endpoint.connect(directory_sub_uri);
        packet::subscribe_to_id(directory_sub_endpoint, 0);

        const auto directory_rpc_uri = fmt::format("tcp://{}:{}", directory_addr, directory_rpc_port);
        directory_rpc_endpoint.connect(directory_rpc_uri);

        {
            packet::connect_packet pkt;
            std::strncpy(pkt.addr, this->my_addr.c_str(), sizeof(pkt.addr) - 1);
            pkt.port = this->my_port;
            const auto nbytes = this->directory_rpc_endpoint.send(pkt, zmq::send_flags::dontwait);
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
            spdlog::trace("My ID is assigned as {}", this->my_id);

            assert(tDSM::packet::my_id != 0);

            use_compression = msg->use_compression;
            if (use_compression) {
                spdlog::trace("Compression enabled");
            }
        }

        // Start the processing thread after configuration, otherwise will race before acquiring our ID and subscribe to ourself because of the bypass guard failing in handle_register_peer
        // Only subscribe to those packets for ID 0 and my_id
        packet::subscribe_to_id(directory_sub_endpoint, this->my_id);
        this->directory_sub_thread = std::thread([this] { this->handle(this->directory_sub_endpoint, this->directory_sub_thread); });

        initialized = true;
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

        spdlog::info("Registering and subscribing to a new peer : {}:{}", msg.addr, msg.port);

        zmq::socket_t sub_endpoint(Context::get().zmq_ctx, zmq::socket_type::sub);
        const auto sub_uri = fmt::format("tcp://{}:{}", msg.addr, msg.port);
        sub_endpoint.connect(sub_uri);
        // Only subscribe to those packets for ID 0 and my_id
        sub_endpoint.set(zmq::sockopt::subscribe, "");
        //packet::subscribe_to_id(sub_endpoint, 0);
        //packet::subscribe_to_id(sub_endpoint, this->my_id);

        spdlog::trace("New peer {} at {}:{} subscribed!", msg.peer_id, msg.addr, msg.port);

        // Add to list of peers
        this->peers.add(msg.peer_id, std::move(sub_endpoint), msg.addr, msg.port);

        return OK;
    }
};

}  // namespace tDSM
