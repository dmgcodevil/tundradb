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

# Find UUID library
if(APPLE)
  # On macOS, we need to use find_library as it's part of the system
  find_library(UUID_LIBRARY System)
elseif(UNIX)
  # On Linux, we can use pkg-config or directly link to uuid
  find_package(PkgConfig QUIET)
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(UUID uuid)
  endif()
  
  if(NOT UUID_FOUND)
    # Fallback to direct linking
    set(UUID_LIBRARY uuid)
  endif()
endif()

# Include FetchContent for downloading dependencies
include(FetchContent)

# Add nlohmann/json
FetchContent_Declare(
  json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.2
)
FetchContent_MakeAvailable(json)

# Include directories
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Core library
add_library(core 
    src/core.cpp src/storage.cpp
)


target_include_directories(core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${ARROW_INCLUDE_DIR}
)


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

