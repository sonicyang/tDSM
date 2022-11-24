#pragma once
#include <sys/socket.h>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <spdlog/spdlog.h>

#include "fd.hpp"

namespace Packet {
    enum PacketType : uint32_t {
        PING,
        PONG,
        ASK_PORT,
        REPORT_PORT,
        DISCONNECT,
        ACK,
        REGISTER_PEER,
        UNREGISTER_PEER,
        MYID,
        DONE_INIT,
        NUM_PACKET_TYPE
    };

    struct PacketHeader {
        PacketType type;
    } __attribute((packed));

    struct PingPacket {
        PacketHeader hdr = { .type = PacketType::PING };
        uint32_t magic = 0;
    } __attribute((packed));

    struct PongPacket {
        PacketHeader hdr = { .type = PacketType::PONG };
        uint32_t magic = 0;
    } __attribute((packed));

    struct DisconnectPacket {
        PacketHeader hdr = { .type = PacketType::DISCONNECT };
    } __attribute((packed));

    struct AckPacket {
        PacketHeader hdr = { .type = PacketType::ACK };
    } __attribute((packed));

    struct AskPortPacket {
        PacketHeader hdr = { .type = PacketType::ASK_PORT };
        std::uint64_t peer_id = 0x0;
    } __attribute((packed));

    struct ReportPortPacket {
        PacketHeader hdr = { .type = PacketType::REPORT_PORT };
        std::uint16_t port = 0;
    } __attribute((packed));

    struct RegisterPeerPacket {
        PacketHeader hdr = { .type = PacketType::REGISTER_PEER };
        std::uint64_t peer_id = 0x0;
        std::uint32_t addr = 0;
        std::uint16_t port = 0;
    } __attribute((packed));

    struct UnregisterPeerPacket {
        PacketHeader hdr = { .type = PacketType::UNREGISTER_PEER };
        std::uint32_t addr = 0;
        std::uint16_t port = 0;
    } __attribute((packed));

    struct MyIDPacket {
        PacketHeader hdr = { .type = PacketType::MYID };
        std::uint64_t peer_id = 0x0;
    } __attribute((packed));

    // XXX: Add traits as protection
    template<typename T>
    static inline auto send(const FileDescriptor& fd, const T& packet) {
        auto remain = sizeof(T);
        while (remain) {
            const auto ret = ::send(fd.get(), reinterpret_cast<const std::uint8_t*>(&packet) + (sizeof(T) - remain), remain, 0);
            if (ret <= 0) {
                spdlog::error("Failed to send a packet: {}", strerror(errno));
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }
        return false;
    }

    template<typename T>
    static inline std::optional<T> recv(const FileDescriptor& fd) {
        T packet;
        auto remain = sizeof(T);
        while (remain) {
            const auto ret = ::recv(fd.get(), reinterpret_cast<std::uint8_t*>(&packet) + (sizeof(T) - remain), remain, 0);
            if (ret <= 0) {
                spdlog::error("Failed to recv a packet: {}", strerror(errno));
                return {};
            }
            remain -= static_cast<decltype(remain)>(ret);
        }

        if (packet.hdr.type != T{}.hdr.type) {
            return {};
        }
        return packet;
    }

    static inline std::optional<PacketType> peek_packet_type(const FileDescriptor& fd) {
        PacketHeader hdr;
        if (recv(fd.get(), &hdr, sizeof(hdr), MSG_PEEK) != sizeof(hdr)) {
            return {};
        }
        return static_cast<PacketType>(hdr.type);
    }
}  // Packet
