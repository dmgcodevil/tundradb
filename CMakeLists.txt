cmake_minimum_required(VERSION 3.30)
project(tundradb)

set(CMAKE_CXX_STANDARD 20)

# Add Homebrew prefix to CMAKE_PREFIX_PATH
list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
list(APPEND CMAKE_PREFIX_PATH "/usr/local")

# Find required packages
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(benchmark REQUIRED)
find_package(GTest REQUIRED)

# Include directories
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Core library
add_library(core src/core.cpp)
target_include_directories(core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${ARROW_INCLUDE_DIR}
)

# Link against Arrow (using imported targets)
#target_link_libraries(core
#    PRIVATE
#        Arrow::arrow_shared
#)


# Main executable
add_executable(tundradb src/main.cpp)
target_link_libraries(tundradb
    PRIVATE
        core
        Arrow::arrow_shared
)

# Add the tests subdirectory
add_subdirectory(tests)

