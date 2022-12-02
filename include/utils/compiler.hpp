#pragma once

#if defined(__GNUC__) || defined(__clang__)

#define tDSM_PACKED_STRUCT __attribute__((packed))

#else

#error "Compiler not supported"

#endif
