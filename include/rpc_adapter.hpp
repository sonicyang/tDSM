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

#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "zmq.hpp"

#include "packet.hpp"
#include "swapper.hpp"

namespace tDSM::rpc {

zmq::message_t dispatch_rpc(const std::size_t action, const zmq::message_t& args);

struct rpc_adapter_base {
    static inline std::unordered_map<std::size_t, std::function<zmq::message_t(const zmq::message_t&)>> rpc_list;

    static inline auto& register_rpc(const auto hash, const auto& func) {
        return rpc_list[hash] = func;
    }

    std::size_t remote{};

    template<typename return_type>
    inline return_type common_stub(const auto hash, const auto... args) {
        assert(remote != 0);
        constexpr auto args_size = (sizeof(args) + ...);
        const auto action = hash;

        zmq::message_t send_buffer(args_size);

        auto encoder = [ptr = static_cast<std::uint8_t*>(send_buffer.data())](const auto arg) mutable {
            std::memcpy(ptr, &arg, sizeof(arg));
            ptr += sizeof(arg);
        };
        (encoder(args), ...);

        const auto ret_msg = swapper::get().call(action, this->remote, std::move(send_buffer));

        if constexpr (!std::is_void_v<return_type>) {
            return_type ret;
            std::memcpy(&ret, ret_msg.data(), sizeof(ret));
            return ret;
        } else {
            (void)ret_msg;
        }
    }

    template<typename return_type, auto func, typename... arg_types>
    static zmq::message_t common_proxy(const zmq::message_t& args) {
        // marshalling
        auto arg_decoder = [ptr = static_cast<const std::uint8_t*>(args.data())]<typename T>() mutable {
            T ret;
            std::memcpy(&ret, ptr, sizeof(ret));
            ptr += sizeof(ret);
            return ret;
        };

        if constexpr (std::is_void_v<return_type>) {
            func(arg_decoder.template operator()<arg_types>()...);
            return zmq::message_t{};
        } else {
            const auto ret = func(arg_decoder.template operator()<arg_types>()...);

            zmq::message_t encoded_ret(sizeof(ret));
            std::memcpy(encoded_ret.data(), &ret, sizeof(ret));

            return encoded_ret;
        }
    }
};

template<auto func, typename return_type, typename... arg_types>
struct static_rpc_adapter : rpc_adapter_base {

    // Currently only handle copyable POD types of reference to objects
    static_assert(((std::is_trivially_copyable_v<arg_types> || std::is_reference_v<arg_types>) && ...));

    template<typename T>
    using justify_arg_type = std::conditional_t<std::is_reference_v<T>, std::add_pointer_t<std::remove_reference_t<T>>, T>;

    template<typename OriginalType>
    static inline constexpr auto justify_arg(OriginalType arg) {
        if constexpr (std::is_reference_v<decltype(arg)>) {
            return &arg;
        } else {
            return arg;
        }
    }

    template<typename OriginalType>
    static inline constexpr OriginalType unjustify_arg(auto arg) {
        if constexpr (std::is_reference_v<OriginalType>) {
            return *arg;
        } else {
            return arg;
        }
    }

    static inline return_type proxy(arg_types... args) {
        // Calls the actual function
        return func(args...);
    }

    static inline return_type proxy_no_ref(justify_arg_type<arg_types>... args) {
        // Convert the reference types back from pointer types, otherwise there will be a signature mismatch with original function
        return proxy(unjustify_arg<arg_types>(args)...);
    }

    static inline auto common_proxy_alias = common_proxy<return_type, proxy_no_ref, justify_arg_type<arg_types>...>;
    static inline auto common_proxy_alias_address = reinterpret_cast<std::uintptr_t>(&common_proxy_alias);

    inline return_type stub_no_ref(justify_arg_type<arg_types>... args) {
        // Call the RPC
        return common_stub<return_type>(common_proxy_alias_address, args...);
    }

    inline return_type stub(arg_types... args) {
        // Convert the reference types to pointer types
        return stub_no_ref(justify_arg<arg_types>(args)...);
    }

    inline decltype(auto) operator()(arg_types... args) {
        // Convert the reference types to pointer types
        return stub(args...);
    }

    static inline auto registered = register_rpc(common_proxy_alias_address, common_proxy_alias);

    static_rpc_adapter() {
        (void)registered;  // force the static variable to initialize
    }
};

namespace detail {

template<auto func, typename T>
struct simple_rpc_adapter;

template<auto func, typename return_type, typename... arg_types>
struct simple_rpc_adapter<func, return_type(*)(arg_types...)> : static_rpc_adapter<func, return_type, arg_types...> {};

}  // detail

template<auto func>
struct simple_rpc_adapter : detail::simple_rpc_adapter<func, decltype(func)> {};

}  // rpc_core
