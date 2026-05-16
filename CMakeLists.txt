cmake_minimum_required(VERSION 3.16)
project(delta CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif()

add_compile_options(-Wall -Wno-unused-parameter -Wno-unused-variable -Wno-sign-compare)

# -----------------------------------------------------------------------------
# DELTA_TLS — opt-in OpenSSL link for native TLS termination.
#
# When ON, cpp-httplib's SSLServer is enabled via CPPHTTPLIB_OPENSSL_SUPPORT,
# OpenSSL is linked into delta_core, and HttpServer::enable_tls() becomes a
# real method instead of returning false. Default is OFF so the standard
# binary stays libssl-free for distros without OpenSSL dev headers.
#
# Build with TLS:
#   cmake -S . -B build -DDELTA_TLS=ON
#   cmake --build build -j8
#
# After build the binary serves HTTPS on the configured --tls-cert / --tls-key.
# -----------------------------------------------------------------------------
option(DELTA_TLS "Build with native TLS support (requires OpenSSL)" OFF)
if(DELTA_TLS)
  find_package(OpenSSL REQUIRED)
  add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT=1)
  message(STATUS "Delta: TLS enabled, linking OpenSSL ${OPENSSL_VERSION}")
endif()

include_directories(${CMAKE_SOURCE_DIR}/third_party ${CMAKE_SOURCE_DIR}/src)

find_package(Threads REQUIRED)

file(GLOB_RECURSE DELTA_SOURCES
  src/core/*.cpp
  src/auth/*.cpp
  src/database/*.cpp
  src/storage/*.cpp
  src/cache/*.cpp
  src/vector/*.cpp
  src/index/*.cpp
  src/network/*.cpp
  src/cluster/*.cpp
)

add_library(delta_core STATIC ${DELTA_SOURCES})
target_link_libraries(delta_core PUBLIC Threads::Threads)
if(DELTA_TLS)
  target_link_libraries(delta_core PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

add_executable(delta_server src/server/main.cpp src/server/config.cpp)
target_link_libraries(delta_server PRIVATE delta_core)

enable_testing()
file(GLOB TEST_SOURCES tests/unit/*.cpp)
foreach(test_src ${TEST_SOURCES})
  get_filename_component(test_name ${test_src} NAME_WE)
  add_executable(${test_name} ${test_src})
  target_link_libraries(${test_name} PRIVATE delta_core)
  add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

# -----------------------------------------------------------------------------
# Optional multi-process chaos test (Round 2 part 3).
#
# Drives three real `delta_server` processes wired with --enable-raft and
# exercises leader election, replication, kill -9 failover, restart catch-
# up, and live membership change. Skipped automatically when Python 3 is
# not on PATH so the default ctest run stays cross-platform.
# -----------------------------------------------------------------------------
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  add_test(
    NAME test_raft_chaos
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/tests/integration/raft_chaos.py
            --binary $<TARGET_FILE:delta_server>
  )
  set_tests_properties(test_raft_chaos PROPERTIES TIMEOUT 120)
  add_test(
    NAME test_sharding_chaos
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/tests/integration/sharding_chaos.py
            --binary $<TARGET_FILE:delta_server>
  )
  set_tests_properties(test_sharding_chaos PROPERTIES TIMEOUT 120)
  add_test(
    NAME test_multi_master_chaos
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/tests/integration/multi_master_chaos.py
            --binary $<TARGET_FILE:delta_server>
  )
  set_tests_properties(test_multi_master_chaos PROPERTIES TIMEOUT 120)
endif()
