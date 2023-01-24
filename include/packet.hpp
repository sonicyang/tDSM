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
        sem_get,
        sem_put
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
        std::uint32_t use_compression = 0x0;
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

    DEFINE_PACKET(sem_get,
        std::uintptr_t address = 0x0;
    );

    DEFINE_PACKET(sem_put,
        std::uintptr_t address = 0x0;
    );

    // XXX: Add traits as protection
    template<typename T>
    static inline auto send(const sys::file_descriptor& fd, const T& packet, const void* const data = nullptr, const std::size_t length = 0) {
        auto remain = sizeof(T);
        while (remain) {
            const auto ret = ::send(fd.get(), reinterpret_cast<const std::uint8_t*>(&packet) + (sizeof(T) - remain), remain, 0);
            tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(ret <= 0, "Failed to send a packet") {
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }

        remain = length;
        while (data != nullptr && remain) {
            const auto ret = ::send(fd.get(), reinterpret_cast<const std::uint8_t*>(data) + (length - remain), remain, 0);
            tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(ret <= 0, "Failed to send a packet") {
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }
        return false;
    }

    static inline std::optional<packet_type> peek_packet_type(const sys::file_descriptor& fd) {
        packet_header hdr;
        if (::recv(fd.get(), &hdr, sizeof(hdr), MSG_PEEK) != sizeof(hdr)) {
            return {};
        }
        return static_cast<packet_type>(hdr.type);
    }

    static inline bool recv(const sys::file_descriptor& fd, void* const data, const std::size_t length) {
        auto remain = length;
        while (data != nullptr && remain) {
            const auto ret = ::recv(fd.get(), reinterpret_cast<std::uint8_t*>(data) + (length - remain), remain, 0);
            tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(ret <= 0, "Failed to recv a packet: {}") {
                return true;
            }
            remain -= static_cast<decltype(remain)>(ret);
        }
        return false;
    }

    template<typename T>
    static inline std::optional<T> recv(const sys::file_descriptor& fd) {
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
