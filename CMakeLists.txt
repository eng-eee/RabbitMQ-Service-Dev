cmake_minimum_required(VERSION 3.16)

project(OrderProcessingSystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

message(STATUS "CMake version: ${CMAKE_VERSION}")
message(STATUS "Build type   : ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard : ${CMAKE_CXX_STANDARD}")

find_package(Threads REQUIRED)
find_package(log4cxx REQUIRED)

# The rmq library is part of this repository. Keep its demos/tests out of the
# application build; the services below are the real consumers.
set(RMQ_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RMQ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(rabbitmq-lib)

# ---------------------------------------------------------------------------
# Shared application layer: JSON, configuration, event envelope, logging and
# the RabbitMQ service base used by every executable.
# ---------------------------------------------------------------------------
add_library(app_common STATIC
    src/common/json.cc
    src/common/config.cc
    src/common/event.cc
    src/common/logging.cc
    src/common/service.cc
)

target_include_directories(app_common
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/rabbitmq-lib/include
        ${log4cxx_INCLUDE_DIRS}
)

target_link_libraries(app_common
    PUBLIC
        log4cxx
        rmq::rmq
        Threads::Threads
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(app_common PRIVATE -Wall -Wextra -Wpedantic)
endif()

# ---------------------------------------------------------------------------
# One executable per service (SYS-003).
# ---------------------------------------------------------------------------
set(APP_EXECUTABLES
    order_service
    inventory_service
    payment_service
    notification_service
    order_monitor_service
    order_cli
)

foreach(executable ${APP_EXECUTABLES})
    add_executable(${executable} src/services/${executable}/main.cc)
    target_link_libraries(${executable} PRIVATE app_common)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${executable} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endforeach()
