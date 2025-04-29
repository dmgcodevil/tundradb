cmake_minimum_required(VERSION 3.30)
project(tundradb)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable Address Sanitizer
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

#set(CMAKE_C_COMPILER "/opt/homebrew/opt/llvm/bin/clang")
#set(CMAKE_CXX_COMPILER "/opt/homebrew/opt/llvm/bin/clang++")
# Add Homebrew prefix to CMAKE_PREFIX_PATH
list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
list(APPEND CMAKE_PREFIX_PATH "/usr/local")

# Find required packages
find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)
find_package(benchmark REQUIRED)
find_package(GTest REQUIRED)
find_package(TBB REQUIRED)

message(STATUS "ArrowDataset libs: ${ARROW_DATASET_LIBRARIES}")

# Find CDS library
if(APPLE)
    # On macOS, try both debug and release versions
    find_library(CDS_LIB NAMES cds_d cds PATHS /usr/local/lib /usr/local/lib64)
else()
    # On Linux, try both debug and release versions
    find_library(CDS_LIB NAMES cds_d cds PATHS /usr/local/lib /usr/local/lib64)
endif()

if(NOT CDS_LIB)
    message(FATAL_ERROR "CDS library not found. Please install libcds.")
endif()

include_directories(/usr/local/include)

# Try to find spdlog first, if not found we'll use FetchContent
find_package(spdlog QUIET)

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
# Check if we already have json in a local path
#set(JSON_SOURCE_DIR "${CMAKE_SOURCE_DIR}/external/json" CACHE PATH "Path to json source")
#
#if(NOT EXISTS "${JSON_SOURCE_DIR}/CMakeLists.txt")
#    # Clone it only if it doesn't exist
#    execute_process(
#            COMMAND git clone https://github.com/nlohmann/json.git ${JSON_SOURCE_DIR}
#            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/external
#    )
#endif()
#add_subdirectory(${JSON_SOURCE_DIR})

# Add spdlog if not found with find_package
if(NOT spdlog_FOUND AND NOT TARGET spdlog::spdlog)
  include(FetchContent)
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.12.0
  )
  FetchContent_MakeAvailable(spdlog)
  message(STATUS "Using FetchContent to provide spdlog")
else()
  message(STATUS "Using installed spdlog")
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
        src/snapshot.cpp
        src/edge_store.cpp
        src/table_info.cpp
        include/schema.hpp
)

target_include_directories(core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${ARROW_INCLUDE_DIR}
        /usr/local/include
)

# Link dependencies
target_link_libraries(core
    PUBLIC
        Arrow::arrow_shared
        ArrowDataset::arrow_dataset_shared
        Parquet::parquet_shared
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        ${UUID_LIBRARY}
        ${CDS_LIB}
        TBB::tbb
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

# Enable testing
enable_testing()

# Add tests directory
add_subdirectory(tests)

