cmake_minimum_required(VERSION 3.30)
project(tundradb)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add Homebrew prefix to CMAKE_PREFIX_PATH
list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
list(APPEND CMAKE_PREFIX_PATH "/usr/local")

# Find required packages
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(benchmark REQUIRED)
find_package(GTest REQUIRED)

# Explicitly set UUID_LIBRARY for Linux
if(APPLE)
  # On macOS, we need to use find_library as it's part of the system
  find_library(UUID_LIBRARY System)
elseif(UNIX)
  # On Linux, directly link to uuid
  set(UUID_LIBRARY uuid)
endif()

# Check if nlohmann_json target already exists
if(NOT TARGET nlohmann_json::nlohmann_json)
  # Include FetchContent for downloading dependencies
  include(FetchContent)

  # Add nlohmann/json
  FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.2
  )
  FetchContent_MakeAvailable(json)
else()
  message(STATUS "nlohmann_json target already exists, skipping FetchContent")
endif()

# Include directories
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Core library
add_library(core
        src/core.cpp
        src/storage.cpp
        src/metadata.cpp
        src/file_utils.cpp
)

target_include_directories(core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${ARROW_INCLUDE_DIR}
)

# Explicitly link core against UUID
#target_link_libraries(core
#    PUBLIC  # Changed from PRIVATE to PUBLIC so dependents can use it
#        uuid  # Directly use the library name
#)

# Main executable
add_executable(tundradb src/main.cpp)
target_link_libraries(tundradb
    PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        nlohmann_json::nlohmann_json
        ${UUID_LIBRARY}
)

# Add the tests subdirectory
add_subdirectory(tests)

