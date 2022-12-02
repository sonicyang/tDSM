#pragma once

#include <arpa/inet.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <spdlog/spdlog.h>

#include "configs.hpp"
#include "packet.hpp"
#include "sys/fd.hpp"
#include "sys/epoll.hpp"
#include "utils/cancelable_thread.hpp"
#include "utils/logging.hpp"

namespace tDSM {

static inline constexpr auto master_port = 9634;
extern bool use_compression;

class Node {
 public:
    Node(const std::uint16_t port) : listener_fd(socket(AF_INET, SOCK_STREAM, 0)) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            listener_fd.get() < 0,
            "Failed to create the listener fd"
        );

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            bind(listener_fd.get(), reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)),
            "Failed to bind to port: {}", port
        );
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            listen(this->listener_fd.get(), max_concurrent_connection),
            "Failed to listen to socket: {}", listener_fd.get()
        );
        constexpr auto enabled = 1;
        setsockopt(listener_fd.get(), SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled));
        this->listener_thread = std::thread{[this]() {
            this->listener();
        }};
    }

    virtual ~Node() {}

    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;

 protected:
    static constexpr auto max_concurrent_connection = 16;

    bool use_compression = tDSM::use_compression;

    // The listener thread
    sys::file_descriptor listener_fd;
    utils::cancelable_thread listener_thread{};

    static inline auto addr_to_string(const struct in_addr& addr) {
        auto addr_str = std::string(INET_ADDRSTRLEN, ' ');
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            !inet_ntop(AF_INET, &addr, addr_str.data(), INET_ADDRSTRLEN),
            "Failed convert a inet addr to string"
        );

        auto it = std::find_if(addr_str.rbegin(), addr_str.rend(),
            [](char c) {
                return !std::isspace<char>(c, std::locale::classic());
            });
        addr_str.erase(it.base(), addr_str.end());
        return addr_str;
    }

    virtual void listener() = 0;

    inline void handler(const std::string& addr_str, const std::uint16_t port, sys::file_descriptor& fd, utils::cancelable_thread& this_thread) {
        const auto epollfd = sys::epoll(fd, this_thread.evtfd);
        while (!this_thread.stopped.load(std::memory_order_acquire)) {
            // Wait for event
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || !epollfd.check_fd_in_result(events, fd)) {
                continue;
            }

            const auto type = packet::peek_packet_type(fd);
            if (!type.has_value()) {
                spdlog::error("Connection to peer {}:{} ended unexpectedly!", addr_str, port);
                this_thread.stopped.store(true, std::memory_order_relaxed);
                continue;
            }

            if (this->handle_a_packet(type.value(), addr_str, port, fd)) {
                this_thread.stopped.store(true, std::memory_order_relaxed);
            }
        }
        if (fd.get() >= 0) {
            // Tell the remote we are disconnecting
            packet::send(fd, packet::disconnect_packet{});
        }
        spdlog::info("Connection to peer {}:{} ended!", addr_str, port);
        fd.release();
    }

    template<typename T>
    inline auto forward_packet(const sys::file_descriptor& fd, const auto& func) {
        const auto msg = packet::recv<T>(fd);
        if (!msg.has_value()) {
            return true;
        }
        return func(this, fd, msg.value());
    }

    inline bool handle_a_packet(const packet::packet_type& type, const std::string& addr_str, const std::uint16_t port, const sys::file_descriptor& fd) {
        bool err = false;
        bool ret = false;

        spdlog::debug("Handling packet: {} from {}:{}", packet::packet_type_to_string(type), addr_str, port);

        switch (type) {
            case packet::packet_type::disconnect:
                // No need to recv, socket is close by remote
                spdlog::info("Peer {}:{} disconnected!", addr_str, port);
                ret = true;
                break;
            case packet::packet_type::ping:
                err = forward_packet<packet::ping_packet>(fd, std::mem_fn(&Node::handle_ping));
                break;
            case packet::packet_type::pong:
                err = forward_packet<packet::pong_packet>(fd, std::mem_fn(&Node::handle_pong));
                break;
            case packet::packet_type::ack:
                err = forward_packet<packet::ack_packet>(fd, std::mem_fn(&Node::handle_ack));
                break;
            case packet::packet_type::ask_port:
                err = forward_packet<packet::ask_port_packet>(fd, std::mem_fn(&Node::handle_ask_port));
                break;
            case packet::packet_type::report_port:
                err = forward_packet<packet::report_port_packet>(fd, std::mem_fn(&Node::handle_report_port));
                break;
            case packet::packet_type::register_peer:
                err = forward_packet<packet::register_peer_packet>(fd, std::mem_fn(&Node::handle_register_peer));
                break;
            case packet::packet_type::unregister_peer:
                err = forward_packet<packet::unregister_peer_packet>(fd, std::mem_fn(&Node::handle_unregister_peer));
                break;
            case packet::packet_type::my_id:
                err = forward_packet<packet::my_id_packet>(fd, std::mem_fn(&Node::handle_my_id));
                break;
            case packet::packet_type::ask_page:
                err = forward_packet<packet::ask_page_packet>(fd, std::mem_fn(&Node::handle_ask_page));
                break;
            case packet::packet_type::send_page:
                err = forward_packet<packet::send_page_packet>(fd, std::mem_fn(&Node::handle_send_page));
                break;
            case packet::packet_type::my_page:
                err = forward_packet<packet::my_page_packet>(fd, std::mem_fn(&Node::handle_my_page));
                break;
            case packet::packet_type::your_page:
                err = forward_packet<packet::your_page_packet>(fd, std::mem_fn(&Node::handle_your_page));
                break;
            case packet::packet_type::lock:
                err = forward_packet<packet::lock_packet>(fd, std::mem_fn(&Node::handle_lock));
                break;
            case packet::packet_type::no_lock:
                err = forward_packet<packet::no_lock_packet>(fd, std::mem_fn(&Node::handle_no_lock));
                break;
            case packet::packet_type::unlock:
                err = forward_packet<packet::unlock_packet>(fd, std::mem_fn(&Node::handle_unlock));
                break;
            case packet::packet_type::no_unlock:
                err = forward_packet<packet::no_unlock_packet>(fd, std::mem_fn(&Node::handle_no_unlock));
                break;
        }

        if (err) {
            spdlog::error("Reading packet type {} from peer {} cause an error!", packet::packet_type_to_string(type), addr_str);
        }
        return ret;
    }

    bool handle_ping(const sys::file_descriptor& fd, const packet::ping_packet& msg) {
        spdlog::trace("ping!");
        return packet::send(fd, packet::pong_packet{ .magic = msg.magic });
    }

    bool handle_pong(const sys::file_descriptor&, const packet::pong_packet&) {
        spdlog::trace("pong!");
        return false;
    }

    bool handle_ack(const sys::file_descriptor&, const packet::ack_packet&) {
        spdlog::trace("Got ack!");
        return false;
    }

    virtual bool handle_ask_port(const sys::file_descriptor&, const packet::ask_port_packet&) {
        spdlog::warn("Ignoring a ask_port packet");
        return false;
    }

    virtual bool handle_report_port(const sys::file_descriptor&, const packet::report_port_packet&) {
        spdlog::warn("Ignoring a report_port packet");
        return false;
    }

    virtual bool handle_register_peer(const sys::file_descriptor&, const packet::register_peer_packet&) {
        spdlog::warn("Ignoring a register_peer packet");
        return false;
    }

    virtual bool handle_unregister_peer(const sys::file_descriptor&, const packet::unregister_peer_packet&) {
        spdlog::warn("Ignoring a unregister_peer packet");
        return false;
    }

    virtual bool handle_my_id(const sys::file_descriptor&, const packet::my_id_packet&) {
        spdlog::warn("Ignoring a my_id packet");
        return false;
    }

    virtual bool handle_ask_page(const sys::file_descriptor&, const packet::ask_page_packet&) {
        spdlog::warn("Ignoring a ask_page packet");
        return false;
    }

    virtual bool handle_send_page(const sys::file_descriptor&, const packet::send_page_packet&) {
        spdlog::warn("Ignoring a send_page packet");
        return false;
    }

    virtual bool handle_my_page(const sys::file_descriptor&, const packet::my_page_packet&) {
        spdlog::warn("Ignoring a my_page packet");
        return false;
    }

    virtual bool handle_your_page(const sys::file_descriptor&, const packet::your_page_packet&) {
        spdlog::warn("Ignoring a your_page packet");
        return false;
    }

    virtual bool handle_lock(const sys::file_descriptor&, const packet::lock_packet&) {
        spdlog::warn("Ignoring a lock packet");
        return false;
    }

    virtual bool handle_no_lock(const sys::file_descriptor&, const packet::no_lock_packet&) {
        spdlog::warn("Ignoring a no_lock packet");
        return false;
    }

    virtual bool handle_unlock(const sys::file_descriptor&, const packet::unlock_packet&) {
        spdlog::warn("Ignoring a unlock packet");
        return false;
    }

    virtual bool handle_no_unlock(const sys::file_descriptor&, const packet::no_unlock_packet&) {
        spdlog::warn("Ignoring a no_unlock packet");
        return false;
    }
};

class master_node : public Node {
 public:
    static master_node& get() {
        static master_node instance;
        return instance;
    }

 private:
    master_node() : Node(master_port) {}
    ~master_node() override {}

    master_node(const master_node&) = delete;
    master_node& operator=(const master_node&) = delete;
    master_node(master_node&&) = delete;
    master_node& operator=(master_node&&) = delete;

    // The clients for master
    struct peer {
        std::size_t id;
        std::string addr_str;
        struct in_addr addr;
        std::uint16_t port;
        sys::file_descriptor fd;
        utils::cancelable_thread thread{};
        peer(
            const std::size_t id_,
            const std::string& addr_str_,
            const struct in_addr& addr_,
            const std::uint16_t port_,
            sys::file_descriptor&& fd_) : id(id_), addr_str(addr_str_), addr(addr_), port(port_), fd(std::move(fd_)) {}
    };
    std::atomic_uint64_t number_of_peers{0};
    std::map<int, std::unique_ptr<peer>> peers{};
    mutable std::mutex peers_mutex{};

    inline decltype(auto) add_peer(const std::uint64_t peer_id, const std::string& addr_str, const struct in_addr& addr, const std::uint16_t port, sys::file_descriptor&& fd) {
        std::scoped_lock<std::mutex> lk{this->peers_mutex};
        // Add to the list of clients/peers
        const auto fd_value = fd.get();
        auto& ref = peers.emplace(fd_value, std::make_unique<peer>(
            peer_id,
            addr_str,
            addr,
            port,
            std::move(fd)
        )).first->second;
        return ref;
    }

    void listener() override {
        const auto epollfd = sys::epoll(this->listener_fd, this->listener_thread.evtfd);
        while (!this->listener_thread.stopped.load(std::memory_order_acquire)) {
            spdlog::trace("Master waiting for peers to connect");

            // Wait for event
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || !epollfd.check_fd_in_result(events, this->listener_fd)) {
                continue;
            }

            struct sockaddr_in incomming_peer{};
            socklen_t incomming_peer_size = sizeof(incomming_peer);
            auto client_fd = sys::file_descriptor{::accept(this->listener_fd.get(), reinterpret_cast<struct sockaddr*>(&incomming_peer), &incomming_peer_size)};
            tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(client_fd.get() < 0, "Failed to accept a connection") {
                continue;
            }
            constexpr auto enabled = 1;
            setsockopt(client_fd.get(), SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled));

            const auto addr_str = addr_to_string(incomming_peer.sin_addr);
            const auto peer_id = number_of_peers.fetch_add(1);
            spdlog::trace("Accepting new peer : {}, ID: {}", addr_str, peer_id);

            // Get port
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(client_fd, packet::ask_port_packet{ .peer_id = peer_id, .use_compression = this->use_compression }),
                "Failed send a ask_port packet to peer {}, ID {}",
                addr_str, peer_id
            );
            const auto response = packet::recv<packet::report_port_packet>(client_fd);

            if (!response.has_value()) {
                spdlog::error("Failed to acquire the port of : {}", addr_str);
                (void)packet::send(client_fd, packet::disconnect_packet{});
                continue;
            }

            spdlog::trace("New peer {} at {}:{} connected!", peer_id, addr_str, response.value().port);

            {
                std::scoped_lock<std::mutex> lk{this->peers_mutex};
                for (const auto& [id, other_peer] : this->peers) {
                    if (!other_peer->thread.stopped.load(std::memory_order_acquire)) {
                        if(packet::send(other_peer->fd, packet::register_peer_packet{
                            .peer_id = peer_id,
                            .addr = incomming_peer.sin_addr.s_addr,
                            .port = response.value().port,
                        })) {
                            spdlog::error("Failed to notify peer {}@{}:{} to register {}@{}:{}",
                                id, other_peer->addr_str, other_peer->port,
                                peer_id, addr_str, response.value().port);
                        }
                    }
                }
            }

            auto& ref = this->add_peer(
                peer_id,
                addr_str,
                incomming_peer.sin_addr,
                response.value().port,
                std::move(client_fd));

            // Start the thread to handle the communication between master and client
            ref->thread = std::thread{[this, &ref]() {
                this->handler(ref->addr_str, ref->port, ref->fd, ref->thread);
            }};
        }
    }

    // Locks, one bit per byte, dictionary address at 64 bytes boundary
    mutable std::shared_mutex page_mutex[n_pages]{};
    mutable std::mutex state_mutex[n_pages]{};
    std::unordered_map<std::uintptr_t, std::mutex> line_mutex[n_pages];

    bool handle_lock(const sys::file_descriptor& fd, const packet::lock_packet& msg) final {
        auto& peer           = this->peers[fd.get()];
        const auto peer_id   = peer->id;
        const auto& addr_str = peer->addr_str;
        const auto port      = peer->port;

        const auto frame_id = get_frame_number(reinterpret_cast<void*>(msg.address));
        const auto aligned_64_address = msg.address & ~(0x40 - 1);  // round down to 64 bytes boundary
        if (msg.address == reinterpret_cast<std::uintptr_t>(get_frame_address(frame_id)) && msg.size == page_size) {
            this->page_mutex[frame_id].lock();
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, msg),
                "Failed notify peer {}:{}, ID {} that lock is acquired",
                addr_str, port, peer_id
            );
        } else if (msg.address == aligned_64_address && msg.size == 64) {
            this->page_mutex[frame_id].lock_shared();
            {
                std::scoped_lock<std::mutex> lk{this->state_mutex[frame_id]};
                this->line_mutex[frame_id][aligned_64_address].lock();
            }

            spdlog::trace("Locking 0x{:x}, {} bytes for {}:{}, ID: {}", msg.address, msg.size, addr_str, port, peer_id);

            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, msg),
                "Failed notify peer {}:{}, ID {} that lock is acquired",
                addr_str, port, peer_id
            );
        } else {
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, packet::no_lock_packet{ .address = msg.address, .size = msg.size }),
                "Failed notify peer {}:{}, ID {} that lock failed",
                addr_str, port, peer_id
            );
        }

        return false;
    }

    bool handle_unlock(const sys::file_descriptor& fd, const packet::unlock_packet& msg) final {
        auto& peer           = this->peers[fd.get()];
        const auto peer_id   = peer->id;
        const auto& addr_str = peer->addr_str;
        const auto port      = peer->port;

        const auto frame_id = get_frame_number(reinterpret_cast<void*>(msg.address));
        const auto aligned_64_address = msg.address & ~(0x40 - 1);  // round down to 64 bytes boundary

        if (msg.address == reinterpret_cast<std::uintptr_t>(get_frame_address(frame_id)) && msg.size == page_size) {
            this->page_mutex[frame_id].unlock();
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, msg),
                "Failed notify peer {}:{}, ID {} that unlock succeed",
                addr_str, port, peer_id
            );
        } else if (msg.address == aligned_64_address && msg.size == 64) {
            spdlog::trace("Unlocking 0x{:x}, {} bytes for {}:{}, ID: {}", msg.address, msg.size, addr_str, port, peer_id);

            {
                std::scoped_lock<std::mutex> lk{this->state_mutex[frame_id]};
                this->line_mutex[frame_id][aligned_64_address].unlock();
            }
            this->page_mutex[frame_id].unlock_shared();

            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, msg),
                "Failed notify peer {}:{}, ID {} that unlock succeed",
                addr_str, port, peer_id
            );
        } else {
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                packet::send(fd, packet::no_unlock_packet{ .address = msg.address, .size = msg.size }),
                "Failed notify peer {}:{}, ID {} that unlock failed",
                addr_str, port, peer_id
            );
        }

        return false;
    }
};

class peer_node : public Node {
 protected:
    class peers {
         public:
            template<typename... Ts>
            auto inline add(sys::file_descriptor&& fd, Ts&&... ts) {
                std::scoped_lock<std::mutex> lk{this->mutex};
                this->epollfd.add_fd(fd);
                const auto fd_value = fd.get();
                this->peers.emplace(fd_value, peer{std::move(fd), std::forward<Ts>(ts)...});
            }

            auto inline del(sys::file_descriptor& fd) {
                std::scoped_lock<std::mutex> lk{this->mutex};
                if (fd.get() >= 0) {
                    packet::send(fd, packet::disconnect_packet{});
                }
                this->epollfd.delete_fd(fd);
                this->peers.erase(fd.get());
                fd.release();
            }

            auto inline clear() {
                std::scoped_lock<std::mutex> lk{this->mutex};
                for (auto& [fd, peer] : this->peers) {
                    if (peer.fd.get() >= 0) {
                        packet::send(peer.fd, packet::disconnect_packet{});
                    }
                    this->epollfd.delete_fd(peer.fd);
                }
                this->peers.clear();
            }

            const sys::epoll& get_epoll_fd() const {
                return this->epollfd;
            }

            struct peer {
                sys::file_descriptor fd;
                std::uint64_t id;
                std::string addr_str;
                std::uint16_t port;
                peer(sys::file_descriptor&& fd_, const std::uint64_t id_, const std::string& addr_str_, const std::uint16_t port_)
                    : fd(std::move(fd_)), id(id_), addr_str(addr_str_), port(port_) {}
            };
            std::unordered_map<int, peer> peers{};
            sys::epoll epollfd{};
            mutable std::mutex mutex{};

            std::optional<peer*> operator[](const int fd_to_find) {
                std::scoped_lock<std::mutex> lk{this->mutex};
                if (this->peers.contains(fd_to_find)) {
                    return {&this->peers.at(fd_to_find)};
                } else {
                    return {};
                }
            }

            inline auto size() const {
                return this->peers.size();
            }

            template<typename T>
            auto inline broadcast(const T& packet) {
                std::scoped_lock<std::mutex> lk(this->mutex);
                for (const auto& [fd, peer] : this->peers) {
                    tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                        packet::send(peer.fd, packet),
                        "Cannot send packet, type = {}, during broadcast", packet.hdr.type);
                }
            }
    };

    peers peers{};
    std::uint64_t my_id{};
    const std::string master_ip;
    const std::uint16_t my_port;

    // master <-> client
    sys::file_descriptor master_fd;
    utils::cancelable_thread master_communication_thread{};

    peer_node(const std::string master_ip_, const std::uint16_t my_port_) : Node(my_port_), master_ip(master_ip_), my_port(my_port_), master_fd(socket(AF_INET, SOCK_STREAM, 0)) {
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            master_fd.get() < 0,
            "Failed to create the socket for master"
        );

        spdlog::info("Connecting to master {}:{}", master_ip, master_port);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(master_port);
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            !inet_aton(master_ip.c_str(), &addr.sin_addr),
            "Failed convert {} to a inet address",
            master_ip
        );
        tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
            connect(master_fd.get(), reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)),
            "Failed to connect to master at {}:{}",
            master_ip, master_port
        );
        this->master_communication_thread = std::thread{[this]() {
            this->handler(this->master_ip, master_port, this->master_fd, this->master_communication_thread);
        }};
    }

    ~peer_node() override {}

    peer_node(const peer_node&) = delete;
    peer_node& operator=(const peer_node&) = delete;
    peer_node(peer_node&&) = delete;
    peer_node& operator=(peer_node&&) = delete;

 private:
    void listener() override {
        const auto epollfd = sys::epoll(this->listener_fd, this->listener_thread.evtfd);
        while (!this->listener_thread.stopped.load(std::memory_order_acquire)) {
            spdlog::trace("Waiting for peers to connect");

            // Wait for event
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || !epollfd.check_fd_in_result(events, this->listener_fd)) {
                continue;
            }

            struct sockaddr_in incomming_peer{};
            socklen_t incomming_peer_size = sizeof(incomming_peer);
            auto peer_fd = sys::file_descriptor{::accept(this->listener_fd.get(), reinterpret_cast<struct sockaddr*>(&incomming_peer), &incomming_peer_size)};
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
                peer_fd.get() < 0,
                "Failed to accept a connection"
            );
            if (peer_fd.get() < 0) {
                continue;
            }
            constexpr auto enabled = 1;
            setsockopt(peer_fd.get(), SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled));

            const auto addr_str = addr_to_string(incomming_peer.sin_addr);
            spdlog::trace("Swapper accepting new peer : {}", addr_str);

            // What is their ID?
            const auto response = packet::recv<packet::my_id_packet>(peer_fd);
            if (!response.has_value()) {
                spdlog::error("Fail to receive peer ID : {}", addr_str);
                continue;
            }

            // Add to the list of peers
            const auto peer_id = response.value().peer_id;
            const auto peer_port = ntohs(incomming_peer.sin_port);
            this->peers.add(std::move(peer_fd), peer_id, addr_str, peer_port);

            spdlog::info("Swapper register a peer {}:{}, ID: peer_id: {}", addr_str, peer_port, peer_id);
        }
    }

    bool handle_register_peer(const sys::file_descriptor& fd, const packet::register_peer_packet& msg) final {
        if (msg.peer_id > my_id) {  // New peer connected into the federation, connect and make p2p channel
            struct sockaddr_in peer_addr{};
            peer_addr.sin_family = AF_INET;
            peer_addr.sin_addr.s_addr = msg.addr;
            peer_addr.sin_port = htons(msg.port);

            auto peer_fd = sys::file_descriptor{socket(AF_INET, SOCK_STREAM, 0)};
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
                peer_fd.get() < 0,
                "Failed to create the socket for peer"
            );

            const auto addr_str = addr_to_string(peer_addr.sin_addr);
            spdlog::trace("Registering and connecting to a new peer : {}:{}", addr_str, msg.port);

            // Connect to the remote peer
            tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(
                connect(peer_fd.get(), reinterpret_cast<struct sockaddr*>(&peer_addr), sizeof(peer_addr)),
                "Failed to connect to peer at {}:{}, ID: {}",
                addr_str, msg.port, msg.peer_id
            );

            // Tell them our ID
            if (packet::send(peer_fd, packet::my_id_packet{ .peer_id = my_id })) {
                spdlog::error("Fail to transmit peer ID : {}", addr_str);
                return true;
            }

            spdlog::trace("New peer {} at {}:{} connected!", msg.peer_id, addr_str, msg.port);

            // Add to list of peers
            this->peers.add(std::move(peer_fd), msg.peer_id, addr_str, msg.port);

            spdlog::info("Register a peer {}:{}, ID: {}", addr_str, msg.port, msg.peer_id);
        }
        return packet::send(fd, packet::ack_packet{});
    }

    bool handle_ask_port(const sys::file_descriptor& fd, const packet::ask_port_packet& msg) final {
        this->my_id = msg.peer_id;
        this->use_compression = msg.use_compression;
        spdlog::trace("Report my port is {}", this->my_port);
        spdlog::trace("My ID is assigned as {}", this->my_id);
        return packet::send(fd, packet::report_port_packet{ .port = my_port });
    }
};

}  // namespace tDSM
