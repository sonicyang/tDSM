# Copyright © 2023 Yang ChungFan <sonic.tw.tp@gmail.com>
# This work is free. You can redistribute it and/or modify it under the
# terms of the Do What The Fuck You Want To Public License, Version 2,
# as published by Sam Hocevar. See the COPYING file for more details.
# 
# This program is free software. It comes without any warranty, to
# the extent permitted by applicable law. You can redistribute it
# and/or modify it under the terms of the Do What The Fuck You Want
# To Public License, Version 2, as published by Sam Hocevar. See
# http://www.wtfpl.net/ for more details.

cmake_minimum_required(VERSION 3.13.0)
project(rdma)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_CXX_STANDARD 20)

set(ZMQ_BUILD_TESTS OFF)
set(ENABLE_DRAFTS ON)
option(ENABLE_DRAFTS ON)
option(CPPZMQ_BUILD_TESTS "" OFF)

add_subdirectory(3rdparty/libzmq)
add_subdirectory(3rdparty/cppzmq)
add_subdirectory(3rdparty/lz4/build/cmake)
add_subdirectory(3rdparty/fmt)
add_subdirectory(3rdparty/spdlog)
add_subdirectory(3rdparty/abseil-cpp)

add_compile_definitions(ZMQ_BUILD_DRAFT_API)

function(add_rdma_executable NAME)
    add_executable(${NAME} ${ARGN})

    target_include_directories(${NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include)

    target_link_libraries(${NAME} PRIVATE 
        fmt
        spdlog::spdlog
        absl::flags
        absl::flags_parse
        absl::flags_usage
        lz4_static
        cppzmq
    )
    target_link_options(${NAME} PRIVATE "-no-pie")

    set(${NAME}_LINKER_SCRIPT ${CMAKE_SOURCE_DIR}/linker_script/rdma.ld)
    set_target_properties(${NAME} PROPERTIES LINK_DEPENDS ${${NAME}_LINKER_SCRIPT})
    target_link_options(${NAME} PRIVATE "${${NAME}_LINKER_SCRIPT}")

    target_compile_options(${NAME} PUBLIC -Wall)
    target_compile_options(${NAME} PUBLIC -Wextra)
    target_compile_options(${NAME} PUBLIC -Werror)
    #target_compile_options(${NAME} PUBLIC -Wfatal-errors)
    target_compile_options(${NAME} PUBLIC -Wno-deprecated-declarations)
    target_compile_options(${NAME} PUBLIC -pedantic)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        target_compile_options(${NAME} PUBLIC -Weverything)
        # No don't care about anything below C++20
        target_compile_options(${NAME} PUBLIC -Wno-c++98-compat)
        target_compile_options(${NAME} PUBLIC -Wno-c++98-compat-pedantic)
        target_compile_options(${NAME} PUBLIC -Wno-c++20-compat)
        target_compile_options(${NAME} PUBLIC -Wno-exit-time-destructors)
        # Auto padding by compiler is allowed for performance
        target_compile_options(${NAME} PUBLIC -Wno-padded)
        # We want to explicit write whether a struct is packed or not
        target_compile_options(${NAME} PUBLIC -Wno-packed)
        # We need this
        target_compile_options(${NAME} PUBLIC -Wno-global-constructors)
        # STFU
        target_compile_options(${NAME} PUBLIC -Wno-weak-vtables)

        # ABSL need This
        target_compile_options(${NAME} PUBLIC -Wno-reserved-identifier)
        # lz4 need this
        target_compile_options(${NAME} PUBLIC -Wno-old-style-cast)
        # fmt need this
        target_compile_options(${NAME} PUBLIC -Wno-undefined-func-template)
        target_compile_options(${NAME} PUBLIC -Wno-switch-enum)
        target_compile_options(${NAME} PUBLIC -Wno-float-equal)
        target_compile_options(${NAME} PUBLIC -Wno-documentation-unknown-command)
        # zmq need this
        target_compile_options(${NAME} PUBLIC -Wno-reserved-macro-identifier)
        target_compile_options(${NAME} PUBLIC -Wno-missing-noreturn)
    endif()
endfunction()

#file(GLOB_RECURSE SOURCES CONFIGURE_DEPENDS ${ARGN})
#add_rdma_executable(rdma src/demo.cpp src/initialize.cpp src/swapper.cpp src/logging.cpp src/rpc_adapter.cpp)
add_rdma_executable(memtest src/memtest.cpp src/initialize.cpp src/swapper.cpp src/logging.cpp src/rpc_adapter.cpp)
