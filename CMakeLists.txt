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
)

add_library(delta_core STATIC ${DELTA_SOURCES})
target_link_libraries(delta_core PUBLIC Threads::Threads)

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
