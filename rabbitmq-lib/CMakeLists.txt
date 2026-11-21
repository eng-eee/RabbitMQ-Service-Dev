cmake_minimum_required(VERSION 3.16)
project(rmq VERSION 0.1.0 LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(RMQ_BUILD_EXAMPLES      "Build example programs"            ON)
option(RMQ_BUILD_TESTS        "Build the broker integration test" ON)
option(RMQ_WARNINGS_AS_ERRORS  "Treat compiler warnings as errors" OFF)

# ---------------------------------------------------------------------------
# Dependencies
#   AMQP-CPP is fetched and built as a static library without its own TCP
#   module: rmq owns the socket and the IO thread, AMQP-CPP is used purely as
#   the protocol codec through AMQP::ConnectionHandler.
#   Offline / vendored build: -DFETCHCONTENT_SOURCE_DIR_AMQPCPP=/path/to/AMQP-CPP
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)

include(FetchContent)
set(AMQP-CPP_BUILD_SHARED   OFF CACHE BOOL "" FORCE)
set(AMQP-CPP_LINUX_TCP      OFF CACHE BOOL "" FORCE)
set(AMQP-CPP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    amqpcpp
    GIT_REPOSITORY https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
    GIT_TAG        v4.3.27
    GIT_SHALLOW    TRUE
)

# AMQP-CPP declares cmake_minimum_required(VERSION < 3.5), which CMake >= 4 rejects.
# Scoped to the dependency only.
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
FetchContent_MakeAvailable(amqpcpp)
unset(CMAKE_POLICY_VERSION_MINIMUM)

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(rmq
    src/error.cpp
    src/connection.cpp
    src/detail/connection_core.cpp
    src/detail/amqp_util.cpp
    src/detail/managed_channel.cpp
    src/detail/consumer.cpp
    src/publisher.cpp
    src/subscriber.cpp
    src/rpc_client.cpp
    src/rpc_server.cpp
)
add_library(rmq::rmq ALIAS rmq)

target_compile_features(rmq PUBLIC cxx_std_17)
set_target_properties(rmq PROPERTIES CXX_EXTENSIONS OFF)

target_include_directories(rmq
    PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# AMQP-CPP never leaks into the public headers, so it stays a PRIVATE dependency.
target_link_libraries(rmq
    PUBLIC  Threads::Threads
    PRIVATE amqpcpp
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(rmq PRIVATE -Wall -Wextra -Wpedantic)
    if(RMQ_WARNINGS_AS_ERRORS)
        target_compile_options(rmq PRIVATE -Werror)
    endif()
endif()

# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------
if(RMQ_BUILD_EXAMPLES)
    foreach(demo connect_demo pubsub_demo rpc_demo)
        add_executable(rmq_${demo} examples/${demo}.cpp)
        target_link_libraries(rmq_${demo} PRIVATE rmq::rmq)
    endforeach()
endif()

# ---------------------------------------------------------------------------
# Integration test (needs a running broker; run it manually)
# ---------------------------------------------------------------------------
if(RMQ_BUILD_TESTS)
    add_executable(rmq_integration_test tests/integration_test.cpp)
    target_link_libraries(rmq_integration_test PRIVATE rmq::rmq)
endif()
