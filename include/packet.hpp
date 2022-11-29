#pragma once
#include <sys/socket.h>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <spdlog/spdlog.h>

#include "logging.hpp"
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
        ASK_PAGE,
        SEND_PAGE,
        MY_PAGE,
        YOUR_PAGE,
        NUM_PACKET_TYPE
    };

    static auto inline packet_type_to_string(const PacketType type) {
#define CASE(name) case name: return #name
        switch(type) {
            CASE(PING);
            CASE(PONG);
            CASE(ASK_PORT);
            CASE(REPORT_PORT);
            CASE(DISCONNECT);
            CASE(ACK);
            CASE(REGISTER_PEER);
            CASE(UNREGISTER_PEER);
            CASE(MYID);
            CASE(ASK_PAGE);
            CASE(SEND_PAGE);
            CASE(MY_PAGE);
            CASE(YOUR_PAGE);
            CASE(NUM_PACKET_TYPE);
        }
        return "";
#undef CASE
    }

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

    struct AskPagePacket {
        PacketHeader hdr = { .type = PacketType::ASK_PAGE };
        std::size_t frame_id = 0x0;
    } __attribute((packed));

    struct SendPagePacketHdr {
        PacketHeader hdr = { .type = PacketType::SEND_PAGE };
        std::size_t frame_id = 0x0;
    } __attribute((packed));

    struct MyPagePacket {
        PacketHeader hdr = { .type = PacketType::MY_PAGE };
        std::size_t frame_id = 0x0;
    } __attribute((packed));

    struct YourPagePacket {
        PacketHeader hdr = { .type = PacketType::YOUR_PAGE };
        std::size_t frame_id = 0x0;
    } __attribute((packed));

    // XXX: Add traits as protection
    template<typename T>
    static inline auto send(const FileDescriptor& fd, const T& packet, const void* const data = nullptr, const std::size_t length = 0) {
        auto remain = sizeof(T);
        while (remain) {
            const auto ret = ::send(fd.get(), reinterpret_cast<const std::uint8_t*>(&packet) + (sizeof(T) - remain), remain, 0);
            SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(ret <= 0, "Failed to send a packet") {
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }

        remain = length;
        while (data != nullptr && remain) {
            const auto ret = ::send(fd.get(), reinterpret_cast<const std::uint8_t*>(data) + (length - remain), remain, 0);
            SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(ret <= 0, "Failed to send a packet") {
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }
        return false;
    }

    static inline std::optional<PacketType> peek_packet_type(const FileDescriptor& fd) {
        PacketHeader hdr;
        if (recv(fd.get(), &hdr, sizeof(hdr), MSG_PEEK) != sizeof(hdr)) {
            return {};
        }
        return static_cast<PacketType>(hdr.type);
    }

    static inline bool recv(const FileDescriptor& fd, void* const data, const std::size_t length) {
        auto remain = length;
        while (data != nullptr && remain) {
            const auto ret = ::recv(fd.get(), reinterpret_cast<std::uint8_t*>(data) + (length - remain), remain, 0);
            SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(ret <= 0, "Failed to recv a packet: {}") {
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }
        return false;
    }

    template<typename T>
    static inline std::optional<T> recv(const FileDescriptor& fd) {
        T packet;
        if (recv(fd, &packet, sizeof(T))) {
            return {};
        }

        if (packet.hdr.type != T{}.hdr.type) {
            return {};
        }

        return packet;
    }
}  // Packet
