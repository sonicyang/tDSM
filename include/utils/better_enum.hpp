#pragma once

#include <cstddef>
#include <string>
#include <vector>

#define tDSM_EXPAND_ENUM_TO_STR_CASE(e) case e: return #e

#define tDSM_EXPAND_ENUM_TO_STR(e, ...) \
    tDSM_EXPAND_ENUM_TO_STR_CASE(e)__VA_OPT__(;) \
    tDSM_EXPAND_ENUM_TO_STR(__VA_ARGS__)

#define tDSM_BETTER_ENUM(type_name, base_type, ...) \
    enum type_name : base_type { \
        __VA_ARGS__ \
    }; \
    \
    static auto inline type_name##_to_string(const type_name e) { \
        static auto str = [] { \
            static auto src = std::string{#__VA_ARGS__}; \
            std::vector<const char*> ret; \
            for (std::size_t pos = 0; pos != src.npos; pos = src.find(',')) { \
                ret.emplace_back(src.c_str() + pos); \
            } \
            return ret; \
        }(); \
        return str[static_cast<base_type>(e)]; \
    }
