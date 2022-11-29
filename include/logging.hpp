#pragma once
#include <cerrno>
#include <spdlog/spdlog.h>
#include <string>
#include <utility>

#define CONCAT(a, b) CONCAT_INNER(a, b)
#define CONCAT_INNER(a, b) a ## b

#define UNIQUE_NAME(base) CONCAT(base, __COUNTER__)

#define SPDLOG_PERROR(...) do { \
    spdlog::error(__VA_ARGS__); \
    spdlog::error("Errno : {}", strerror(errno)); \
} while(false)

#define SPDLOG_DUMP_IF_ERROR(...) SPDLOG_DUMP_IF_ERROR_(UNIQUE_NAME(_error), __VA_ARGS__)
#define SPDLOG_DUMP_IF_ERROR_(vname, statement, ...) if([&] { \
    bool vname{}; \
    vname = (statement); \
    if (vname) { \
        spdlog::error(__VA_ARGS__); \
    }\
    return vname; \
}())

#define SPDLOG_ASSERT_DUMP_IF_ERROR(...) SPDLOG_ASSERT_DUMP_IF_ERROR_(UNIQUE_NAME(_error), __VA_ARGS__)
#define SPDLOG_ASSERT_DUMP_IF_ERROR_(vname, statement, ...) do { \
    bool vname{}; \
    vname = (statement); \
    SPDLOG_DUMP_IF_ERROR(vname, __VA_ARGS__) { \
        assert(!vname); \
    } \
} while(false)

#define SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(...) SPDLOG_DUMP_IF_ERROR_WITH_ERRNO_(UNIQUE_NAME(_error), __VA_ARGS__)
#define SPDLOG_DUMP_IF_ERROR_WITH_ERRNO_(vname, statement, ...) if([&] { \
    bool vname{}; \
    vname = (statement); \
    if (vname) { \
        SPDLOG_PERROR(__VA_ARGS__); \
    }\
    return vname; \
}())

#define SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(...) SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO_(UNIQUE_NAME(_error), __VA_ARGS__)
#define SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO_(vname, statement, ...) do { \
    bool vname{}; \
    vname = (statement); \
    SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(vname, __VA_ARGS__) { \
        assert(!vname); \
    } \
} while(false)
