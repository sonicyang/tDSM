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

#include "spdlog/spdlog.h"
#include "zmq.hpp"

#include "configs.hpp"
#include "sys/fd.hpp"
#include "utils/compiler.hpp"
#include "utils/better_enum.hpp"
#include "utils/logging.hpp"

#define DEFINE_PACKET(name, ...) \
    struct name##_packet { \
        packet_header hdr = { .from = my_id, .type = packet_type::name }; \
        __VA_ARGS__ \
        auto to(const std::size_t to_id) { \
            this->hdr.to = to_id; \
            return *this; \
        } \
        operator zmq::const_buffer() const { return zmq::buffer(this, sizeof(name##_packet)); } \
    } tDSM_PACKED_STRUCT

namespace tDSM::packet {
    // dirity hack to get around the cyclic dependency problem
    extern std::size_t my_id;

    tDSM_BETTER_ENUM(packet_type, std::uint32_t,
        noop,
        connect,
        configure,
        disconnect,
        register_peer,
        my_id,
        ask_page,
        send_page,
        my_page,
        your_page,
        sem_get,
        sem_put,
        call,
        call_ack,
        ret,
        ret_ack,
        max_types
    );

    struct packet_header {
        std::size_t to{};
        std::size_t from{};
        packet_type type;
    } tDSM_PACKED_STRUCT;

    DEFINE_PACKET(noop);

    DEFINE_PACKET(disconnect);

    DEFINE_PACKET(connect,
        char addr[16];
        std::uint16_t port = 0;
    );

    DEFINE_PACKET(configure,
        std::uint64_t peer_id = 0x0;
        std::uint32_t use_compression = 0x0;
    );

    DEFINE_PACKET(register_peer,
        std::uint64_t peer_id = 0x0;
        char addr[16];
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

    DEFINE_PACKET(call,
        std::size_t call_id = 0x0;
        std::size_t action = 0x0;
        std::size_t size = 0x0;
    );

    DEFINE_PACKET(call_ack,
        std::size_t call_id = 0x0;
    );

    DEFINE_PACKET(ret,
        std::size_t call_id = 0x0;
        std::size_t size = 0x0;
    );

    DEFINE_PACKET(ret_ack,
        std::size_t call_id = 0x0;
    );

    static inline auto subscribe_to_id(auto& sock, const std::size_t id) {
        std::uint8_t id_char[sizeof(id)];
        std::memcpy(id_char, &id, sizeof(id));
        sock.set(zmq::sockopt::subscribe, zmq::const_buffer(id_char, sizeof(id_char)));
    }
}  // tDSM::packet

#undef DEFINE_PACKET
