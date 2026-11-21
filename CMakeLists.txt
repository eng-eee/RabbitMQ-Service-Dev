cmake_minimum_required(VERSION 3.16)

project(CppRabbitMQ LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# By default, set the build type to Debug if not specified
set(CMAKE_BUILD_TYPE Debug)

# Messaging the STATUS of the build process
message(STATUS "CMake version: ${CMAKE_VERSION}")
message(STATUS "Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "CPP Version : ${CMAKE_CXX_STANDARD}")


find_package(log4cxx REQUIRED)

# Collect application sources (excluding main.cc)
file(GLOB_RECURSE APP_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/src/*.cc"
)

# Exclude main.cc from the application sources to avoid duplicate compilation
# and linking the gtest main function easily
list(FILTER APP_SOURCES EXCLUDE REGEX "/main\\.cc$")

# Application object library
add_library(app_objects OBJECT
    ${APP_SOURCES}
)

target_include_directories(app_objects
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${log4cxx_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/rabbitmq-lib/include
)


# External libraries for the application object library
target_link_libraries(app_objects
    PUBLIC
        log4cxx
)

# Creating the main executable
add_executable(my_app
    src/main.cc
)

# Submodule for RabbitMQ library
add_subdirectory(rabbitmq-lib)

# Linking external libraries to the main executable
target_link_libraries(my_app
    PRIVATE
        app_objects
        log4cxx
        rmq::rmq
)