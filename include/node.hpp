#pragma once

#include <arpa/inet.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <thread>
#include <map>
#include <functional>
#include <mutex>
#include <unordered_map>

#include "configs.hpp"
#include "logging.hpp"
#include "fd.hpp"
#include "epoll.hpp"
#include "packet.hpp"
#include "cancelable_thread.hpp"

static inline constexpr auto master_port = 9634;

class Node {
 public:
    Node(const std::uint16_t port) : listener_fd(socket(AF_INET, SOCK_STREAM, 0)) {
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            listener_fd.get() < 0,
            "Failed to create the listener fd"
        );

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        SPDLOG_ASSERT_DUMP_IF_ERROR(
            bind(listener_fd.get(), reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)),
            "Failed to bind to port: {}", port
        );
        SPDLOG_ASSERT_DUMP_IF_ERROR(
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

    // The listener thread
    FileDescriptor listener_fd;
    CancelableThread listener_thread{};

    static inline auto addr_to_string(const struct in_addr& addr) {
        auto addr_str = std::string(INET_ADDRSTRLEN, ' ');
        SPDLOG_ASSERT_DUMP_IF_ERROR(
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

    inline void handler(const std::string& addr_str, const std::uint16_t port, FileDescriptor& fd, CancelableThread& this_thread) {
        const auto epollfd = Epoll(fd, this_thread.evtfd);
        while (!this_thread.stopped.load(std::memory_order_acquire)) {
            // Wait for event
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || !epollfd.check_fd_in_result(events, fd)) {
                continue;
            }

            const auto type = Packet::peek_packet_type(fd);
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
            Packet::send(fd, Packet::DisconnectPacket{});
        }
        spdlog::info("Connection to peer {}:{} ended!", addr_str, port);
        fd.release();
    }

    template<typename T>
    inline auto forward_packet(const FileDescriptor& fd, const auto& func) {
        const auto msg = Packet::recv<T>(fd);
        if (!msg.has_value()) {
            return true;
        }
        return func(this, fd, msg.value());
    }

    inline bool handle_a_packet(const Packet::PacketType& type, const std::string& addr_str, const std::uint16_t port, const FileDescriptor& fd) {
        bool err = false;
        bool ret = false;

        spdlog::debug("Handling packet: {} from {}:{}", Packet::packet_type_to_string(type), addr_str, port);

        switch (type) {
            case Packet::PacketType::DISCONNECT:
                // No need to recv, socket is close by remote
                spdlog::info("Peer {}:{} disconnected!", addr_str, port);
                ret = true;
                break;
            case Packet::PacketType::PING:
                err = forward_packet<Packet::PingPacket>(fd, std::mem_fn(&Node::handle_ping));
                break;
            case Packet::PacketType::PONG:
                err = forward_packet<Packet::PongPacket>(fd, std::mem_fn(&Node::handle_pong));
                break;
            case Packet::PacketType::ACK:
                err = forward_packet<Packet::AckPacket>(fd, std::mem_fn(&Node::handle_ack));
                break;
            case Packet::PacketType::ASK_PORT:
                err = forward_packet<Packet::AskPortPacket>(fd, std::mem_fn(&Node::handle_ask_port));
                break;
            case Packet::PacketType::REPORT_PORT:
                err = forward_packet<Packet::ReportPortPacket>(fd, std::mem_fn(&Node::handle_report_port));
                break;
            case Packet::PacketType::REGISTER_PEER:
                err = forward_packet<Packet::RegisterPeerPacket>(fd, std::mem_fn(&Node::handle_register_peer));
                break;
            case Packet::PacketType::UNREGISTER_PEER:
                err = forward_packet<Packet::UnregisterPeerPacket>(fd, std::mem_fn(&Node::handle_unregister_peer));
                break;
            case Packet::PacketType::MYID:
                err = forward_packet<Packet::MyIDPacket>(fd, std::mem_fn(&Node::handle_my_id));
                break;
            case Packet::PacketType::ASK_PAGE:
                err = forward_packet<Packet::AskPagePacket>(fd, std::mem_fn(&Node::handle_ask_page));
                break;
            case Packet::PacketType::SEND_PAGE:
                err = forward_packet<Packet::SendPagePacketHdr>(fd, std::mem_fn(&Node::handle_send_page));
                break;
            case Packet::PacketType::MY_PAGE:
                err = forward_packet<Packet::MyPagePacket>(fd, std::mem_fn(&Node::handle_my_page));
                break;
            case Packet::PacketType::YOUR_PAGE:
                err = forward_packet<Packet::YourPagePacket>(fd, std::mem_fn(&Node::handle_your_page));
                break;
            case Packet::PacketType::NUM_PACKET_TYPE:
                spdlog::warn("Unknown or unhandled packet type {}", Packet::packet_type_to_string(type));
                break;
        }

        if (err) {
            spdlog::error("Reading packet type {} from peer {} cause an error!", Packet::packet_type_to_string(type), addr_str);
        }
        return ret;
    }

    bool handle_ping(const FileDescriptor& fd, const Packet::PingPacket& msg) {
        spdlog::trace("Ping!");
        return Packet::send(fd, Packet::PongPacket{ .magic = msg.magic });
    }

    bool handle_pong(const FileDescriptor&, const Packet::PongPacket&) {
        spdlog::trace("Pong!");
        return false;
    }

    bool handle_ack(const FileDescriptor&, const Packet::AckPacket&) {
        spdlog::trace("Got ACK!");
        return false;
    }

    virtual bool handle_ask_port(const FileDescriptor&, const Packet::AskPortPacket&) {
        spdlog::warn("Ignoring a ASK_PORT packet");
        return false;
    }

    virtual bool handle_report_port(const FileDescriptor&, const Packet::ReportPortPacket&) {
        spdlog::warn("Ignoring a REPORT_PORT packet");
        return false;
    }

    virtual bool handle_register_peer(const FileDescriptor&, const Packet::RegisterPeerPacket&) {
        spdlog::warn("Ignoring a REGISTER_PEER packet");
        return false;
    }

    virtual bool handle_unregister_peer(const FileDescriptor&, const Packet::UnregisterPeerPacket&) {
        spdlog::warn("Ignoring a UNREGISTER_PEER packet");
        return false;
    }

    virtual bool handle_my_id(const FileDescriptor&, const Packet::MyIDPacket&) {
        spdlog::warn("Ignoring a MY_ID packet");
        return false;
    }

    virtual bool handle_ask_page(const FileDescriptor&, const Packet::AskPagePacket&) {
        spdlog::warn("Ignoring a ASK_PAGE packet");
        return false;
    }

    virtual bool handle_send_page(const FileDescriptor& fd, const Packet::SendPagePacketHdr&) {
        /* discard a page */
        std::uint8_t dummy[page_size];
        (void)Packet::recv(fd, dummy, page_size);
        (void)dummy;
        spdlog::warn("Ignoring a SEND_PAGE packet");
        return false;
    }

    virtual bool handle_my_page(const FileDescriptor&, const Packet::MyPagePacket&) {
        spdlog::warn("Ignoring a MY_PAGE packet");
        return false;
    }

    virtual bool handle_your_page(const FileDescriptor&, const Packet::YourPagePacket&) {
        spdlog::warn("Ignoring a YOUR_PAGE packet");
        return false;
    }
};

class MasterNode : public Node {
 public:
    static MasterNode& get() {
        static MasterNode instance;
        return instance;
    }

 private:
    MasterNode() : Node(master_port) {}
    ~MasterNode() override {}

    MasterNode(const MasterNode&) = delete;
    MasterNode& operator=(const MasterNode&) = delete;
    MasterNode(MasterNode&&) = delete;
    MasterNode& operator=(MasterNode&&) = delete;

    // The clients for master
    struct Peer {
        std::string addr_str;
        struct in_addr addr;
        std::uint16_t port;
        FileDescriptor fd;
        CancelableThread thread{};
        Peer(
            const std::string& addr_str_,
            const struct in_addr& addr_,
            const std::uint16_t port_,
            FileDescriptor&& fd_) : addr_str(addr_str_), addr(addr_), port(port_), fd(std::move(fd_)) {}
    };
    std::atomic_uint64_t number_of_peers{0};
    std::map<std::uint64_t, std::unique_ptr<Peer>> peers{};
    mutable std::mutex peers_mutex{};

    inline decltype(auto) add_peer(const std::uint64_t peer_id, const std::string& addr_str, const struct in_addr& addr, const std::uint16_t port, FileDescriptor&& fd) {
        std::scoped_lock<std::mutex> lk{this->peers_mutex};
        // Add to the list of clients/peers
        auto& ref = peers.emplace(peer_id, std::make_unique<Peer>(
            addr_str,
            addr,
            port,
            std::move(fd)
        )).first->second;
        return ref;
    }

    void listener() override {
        const auto epollfd = Epoll(this->listener_fd, this->listener_thread.evtfd);
        while (!this->listener_thread.stopped.load(std::memory_order_acquire)) {
            spdlog::trace("Master waiting for peers to connect");

            // Wait for event
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || !epollfd.check_fd_in_result(events, this->listener_fd)) {
                continue;
            }

            struct sockaddr_in incomming_peer{};
            socklen_t incomming_peer_size = sizeof(incomming_peer);
            auto client_fd = FileDescriptor{::accept(this->listener_fd.get(), reinterpret_cast<struct sockaddr*>(&incomming_peer), &incomming_peer_size)};
            SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(client_fd.get() < 0, "Failed to accept a connection") {
                continue;
            }
            constexpr auto enabled = 1;
            setsockopt(client_fd.get(), SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled));

            const auto addr_str = addr_to_string(incomming_peer.sin_addr);
            const auto peer_id = number_of_peers.fetch_add(1);
            spdlog::trace("Accepting new peer : {}, ID: {}", addr_str, peer_id);

            // Get port
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                Packet::send(client_fd, Packet::AskPortPacket{ .peer_id = peer_id }),
                "Failed send a AskPort packet to peer {}, ID {}",
                addr_str, peer_id
            );
            const auto response = Packet::recv<Packet::ReportPortPacket>(client_fd);

            if (!response.has_value()) {
                spdlog::error("Failed to acquire the port of : {}", addr_str);
                (void)Packet::send(client_fd, Packet::DisconnectPacket{});
                continue;
            }

            spdlog::trace("New peer {} at {}:{} connected!", peer_id, addr_str, response.value().port);

            {
                std::scoped_lock<std::mutex> lk{this->peers_mutex};
                for (const auto& [id, other_peer] : this->peers) {
                    if (!other_peer->thread.stopped.load(std::memory_order_acquire)) {
                        if(Packet::send(other_peer->fd, Packet::RegisterPeerPacket{
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
};

class PeerNode : public Node {
 protected:
    class Peers {
         public:
            template<typename... Ts>
            auto inline add(FileDescriptor&& fd, Ts&&... ts) {
                std::scoped_lock<std::mutex> lk{this->mutex};
                this->epollfd.add_fd(fd);
                const auto fd_value = fd.get();
                this->peers.emplace(fd_value, Peer{std::move(fd), std::forward<Ts>(ts)...});
            }

            auto inline del(FileDescriptor& fd) {
                std::scoped_lock<std::mutex> lk{this->mutex};
                if (fd.get() >= 0) {
                    Packet::send(fd, Packet::DisconnectPacket{});
                }
                this->epollfd.delete_fd(fd);
                this->peers.erase(fd.get());
                fd.release();
            }

            auto inline clear() {
                std::scoped_lock<std::mutex> lk{this->mutex};
                for (auto& [fd, peer] : this->peers) {
                    if (peer.fd.get() >= 0) {
                        Packet::send(peer.fd, Packet::DisconnectPacket{});
                    }
                    this->epollfd.delete_fd(peer.fd);
                }
                this->peers.clear();
            }

            const Epoll& get_epoll_fd() const {
                return this->epollfd;
            }

            struct Peer {
                FileDescriptor fd;
                std::uint64_t id;
                std::string addr_str;
                std::uint16_t port;
                Peer(FileDescriptor&& fd_, const std::uint64_t id_, const std::string& addr_str_, const std::uint16_t port_)
                    : fd(std::move(fd_)), id(id_), addr_str(addr_str_), port(port_) {}
            };
            std::unordered_map<int, Peer> peers{};
            Epoll epollfd{};
            mutable std::mutex mutex{};

            std::optional<Peer*> operator[](const int fd_to_find) {
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
                    SPDLOG_ASSERT_DUMP_IF_ERROR(
                        Packet::send(peer.fd, packet),
                        "Cannot send packet, type = {}, during broadcast", packet.hdr.type);
                }
            }
    };

    Peers peers{};
    std::uint64_t my_id{};

    PeerNode(const std::string master_ip_, const std::uint16_t my_port_) : Node(my_port_), master_ip(master_ip_), my_port(my_port_), master_fd(socket(AF_INET, SOCK_STREAM, 0)) {
        SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
            master_fd.get() < 0,
            "Failed to create the socket for master"
        );

        spdlog::info("Connecting to master {}:{}", master_ip, master_port);

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(master_port);
        SPDLOG_ASSERT_DUMP_IF_ERROR(
            !inet_aton(master_ip.c_str(), &addr.sin_addr),
            "Failed convert {} to a inet address",
            master_ip
        );
        SPDLOG_ASSERT_DUMP_IF_ERROR(
            connect(master_fd.get(), reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)),
            "Failed to connect to master at {}:{}",
            master_ip, master_port
        );
        this->master_communication_thread = std::thread{[this]() {
            this->handler(this->master_ip, master_port, this->master_fd, this->master_communication_thread);
        }};
    }

    ~PeerNode() override {}

    PeerNode(const PeerNode&) = delete;
    PeerNode& operator=(const PeerNode&) = delete;
    PeerNode(PeerNode&&) = delete;
    PeerNode& operator=(PeerNode&&) = delete;

 private:
    const std::string master_ip;
    const std::uint16_t my_port;

    // master <-> client
    FileDescriptor master_fd;
    CancelableThread master_communication_thread{};

    void listener() override {
        const auto epollfd = Epoll(this->listener_fd, this->listener_thread.evtfd);
        while (!this->listener_thread.stopped.load(std::memory_order_acquire)) {
            spdlog::trace("Waiting for peers to connect");

            // Wait for event
            const auto [count, events] = epollfd.wait();
            if ((count <= 0) || !epollfd.check_fd_in_result(events, this->listener_fd)) {
                continue;
            }

            struct sockaddr_in incomming_peer{};
            socklen_t incomming_peer_size = sizeof(incomming_peer);
            auto peer_fd = FileDescriptor{::accept(this->listener_fd.get(), reinterpret_cast<struct sockaddr*>(&incomming_peer), &incomming_peer_size)};
            SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
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
            const auto response = Packet::recv<Packet::MyIDPacket>(peer_fd);
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

    bool handle_register_peer(const FileDescriptor& fd, const Packet::RegisterPeerPacket& msg) final {
        if (msg.peer_id > my_id) {  // New peer connected into the federation, connect and make p2p channel
            struct sockaddr_in peer_addr{};
            peer_addr.sin_family = AF_INET;
            peer_addr.sin_addr.s_addr = msg.addr;
            peer_addr.sin_port = htons(msg.port);

            auto peer_fd = FileDescriptor{socket(AF_INET, SOCK_STREAM, 0)};
            SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(
                peer_fd.get() < 0,
                "Failed to create the socket for peer"
            );

            const auto addr_str = addr_to_string(peer_addr.sin_addr);
            spdlog::trace("Registering and connecting to a new peer : {}:{}", addr_str, msg.port);

            // Connect to the remote peer
            SPDLOG_ASSERT_DUMP_IF_ERROR(
                connect(peer_fd.get(), reinterpret_cast<struct sockaddr*>(&peer_addr), sizeof(peer_addr)),
                "Failed to connect to peer at {}:{}, ID: {}",
                addr_str, msg.port, msg.peer_id
            );

            // Tell them our ID
            if (Packet::send(peer_fd, Packet::MyIDPacket{ .peer_id = my_id })) {
                spdlog::error("Fail to transmit peer ID : {}", addr_str);
                return true;
            }

            spdlog::trace("New peer {} at {}:{} connected!", msg.peer_id, addr_str, msg.port);

            // Add to list of peers
            this->peers.add(std::move(peer_fd), msg.peer_id, addr_str, msg.port);

            spdlog::info("Register a peer {}:{}, ID: {}", addr_str, msg.port, msg.peer_id);
        }
        return Packet::send(fd, Packet::AckPacket{});
    }

    bool handle_ask_port(const FileDescriptor& fd, const Packet::AskPortPacket& msg) final {
        this->my_id = msg.peer_id;
        spdlog::trace("Report my port is {}", this->my_port);
        spdlog::trace("My ID is assigned as {}", this->my_id);
        return Packet::send(fd, Packet::ReportPortPacket{ .port = my_port });
    }
};
