#pragma once
#include <cerrno>
#include <spdlog/spdlog.h>
#include <string>
#include <utility>

#define tDSM_CONCAT(a, b) tDSM_CONCAT_INNER(a, b)
#define tDSM_CONCAT_INNER(a, b) a ## b

#define tDSM_UNIQUE_NAME(base) tDSM_CONCAT(base, __COUNTER__)

#define tDSM_SPDLOG_PERROR(...) do { \
    spdlog::error(__VA_ARGS__); \
    spdlog::error("Errno : {}", strerror(errno)); \
} while(false)

#define tDSM_SPDLOG_DUMP_IF_ERROR(...) tDSM_SPDLOG_DUMP_IF_ERROR_(tDSM_UNIQUE_NAME(_error), __VA_ARGS__)
#define tDSM_SPDLOG_DUMP_IF_ERROR_(vname, statement, ...) if([&] { \
    bool vname{}; \
    vname = (statement); \
    if (vname) { \
        spdlog::error(__VA_ARGS__); \
    }\
    return vname; \
}())

#define tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR(...) tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_(tDSM_UNIQUE_NAME(_error), __VA_ARGS__)
#define tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_(vname, statement, ...) do { \
    bool vname{}; \
    vname = (statement); \
    tDSM_SPDLOG_DUMP_IF_ERROR(vname, __VA_ARGS__) { \
        assert(!vname); \
    } \
} while(false)

#define tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(...) tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO_(tDSM_UNIQUE_NAME(_error), __VA_ARGS__)
#define tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO_(vname, statement, ...) if([&] { \
    bool vname{}; \
    vname = (statement); \
    if (vname) { \
        tDSM_SPDLOG_PERROR(__VA_ARGS__); \
    }\
    return vname; \
}())

#define tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO(...) tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO_(tDSM_UNIQUE_NAME(_error), __VA_ARGS__)
#define tDSM_SPDLOG_ASSERT_DUMP_IF_ERROR_WITH_ERRNO_(vname, statement, ...) do { \
    bool vname{}; \
    vname = (statement); \
    tDSM_SPDLOG_DUMP_IF_ERROR_WITH_ERRNO(vname, __VA_ARGS__) { \
        assert(!vname); \
    } \
} while(false)
