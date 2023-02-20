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

#include <cstddef>
#include <string>
#include <vector>

#define tDSM_EXPAND_ENUM_TO_STR_CASE(e) case e: return #e

#define tDSM_EXPAND_ENUM_TO_STR(e, ...) \
    tDSM_EXPAND_ENUM_TO_STR_CASE(e)__VA_OPT__(;) \
    tDSM_EXPAND_ENUM_TO_STR(__VA_ARGS__)

#define tDSM_BETTER_ENUM(type_name, base_type, ...) \
    enum class type_name : base_type { \
        __VA_ARGS__ \
    }; \
    \
    static auto inline type_name##_to_string(const type_name e) { \
        static auto str = [] { \
            static auto src = std::string{#__VA_ARGS__}; \
            std::vector<std::string> ret; \
            std::size_t pos; \
            while ((pos = src.find(',')) != std::string::npos) { \
                ret.emplace_back(src.substr(0, pos)); \
                src.erase(0, pos + 1); \
            } \
            ret.emplace_back(src); \
            return ret; \
        }(); \
        return str[static_cast<base_type>(e)]; \
    }
