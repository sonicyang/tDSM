#pragma once
#include <sys/socket.h>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <spdlog/spdlog.h>

#include "configs.hpp"
#include "sys/fd.hpp"
#include "utils/compiler.hpp"
#include "utils/better_enum.hpp"
#include "utils/logging.hpp"

#define DEFINE_PACKET(name, ...) \
    struct name##_packet { \
        packet_header hdr = { .type = packet_type::name }; \
        __VA_ARGS__ \
    } tDSM_PACKED_STRUCT

namespace tDSM::packet {
    tDSM_BETTER_ENUM(packet_type, std::uint32_t,
        ping,
        pong,
        ask_port,
        report_port,
        disconnect,
        ack,
        register_peer,
        unregister_peer,
        my_id,
        ask_page,
        send_page,
        my_page,
        your_page,
        lock,
        no_lock,
        unlock,
        no_unlock
    );

    struct packet_header {
        packet_type type;
    } tDSM_PACKED_STRUCT;

    DEFINE_PACKET(ping,
        std::uint32_t magic = 0;
    );

    DEFINE_PACKET(pong,
        std::uint32_t magic = 0;
    );

    DEFINE_PACKET(disconnect);

    DEFINE_PACKET(ack);

    DEFINE_PACKET(ask_port,
        std::uint64_t peer_id = 0x0;
    );

    DEFINE_PACKET(report_port,
        std::uint16_t port = 0;
    );

    DEFINE_PACKET(register_peer,
        std::uint64_t peer_id = 0x0;
        std::uint32_t addr = 0;
        std::uint16_t port = 0;
    );

    DEFINE_PACKET(unregister_peer,
        std::uint32_t addr = 0;
        std::uint16_t port = 0;
    );

    DEFINE_PACKET(my_id,
        std::uint64_t peer_id = 0x0;
    );

    DEFINE_PACKET(ask_page,
        std::size_t frame_id = 0x0;
    );

    DEFINE_PACKET(send_page,
        std::size_t frame_id = 0x0;
        std::size_t size = page_size;
    );

    DEFINE_PACKET(my_page,
        std::size_t frame_id = 0x0;
    );

    DEFINE_PACKET(your_page,
        std::size_t frame_id = 0x0;
    );

    DEFINE_PACKET(lock,
        std::uintptr_t address = 0x0;
        std::size_t size = 0x0;
    );

    DEFINE_PACKET(no_lock,
        std::uintptr_t address = 0x0;
        std::size_t size = 0x0;
    );

    DEFINE_PACKET(unlock,
        std::uintptr_t address = 0x0;
        std::size_t size = 0x0;
    );

    DEFINE_PACKET(no_unlock,
        std::uintptr_t address = 0x0;
        std::size_t size = 0x0;
    );

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

    static inline std::optional<packet_type> peek_packet_type(const FileDescriptor& fd) {
        packet_header hdr;
        if (::recv(fd.get(), &hdr, sizeof(hdr), MSG_PEEK) != sizeof(hdr)) {
            return {};
        }
        return static_cast<packet_type>(hdr.type);
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
}  // tDSM::packet

#undef DEFINE_PACKET
