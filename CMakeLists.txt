cmake_minimum_required(VERSION 3.30)
project(tundradb VERSION 1.0.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Option: build shared or static library (default: static)
option(BUILD_SHARED_LIBS "Build TundraDB as a shared library" OFF)
option(TUNDRADB_BUILD_SHELL "Build the tundra_shell interactive CLI" ON)
option(TUNDRADB_BUILD_TESTS "Build tests" ON)
option(TUNDRADB_BUILD_BENCHMARKS "Build benchmarks" ON)

# Configure compile-time logging levels
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(TUNDRA_LOG_LEVEL_DEBUG)
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_definitions(TUNDRA_LOG_LEVEL_INFO)
elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    add_compile_definitions(TUNDRA_LOG_LEVEL_INFO)
else()
    add_compile_definitions(TUNDRA_LOG_LEVEL_DEBUG)
endif()

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
find_package(Parquet REQUIRED)

# Arrow targets are available after find_package(Arrow)

# ArrowDataset depends on Arrow, ArrowCompute, ArrowAcero, and Parquet
# Try to find ArrowCompute and ArrowAcero first
find_package(ArrowCompute QUIET)
find_package(ArrowAcero QUIET)

if(TUNDRADB_BUILD_BENCHMARKS)
    add_executable(tundra_bench_runner bench/tundra_runner.cpp)
    target_link_libraries(tundra_bench_runner
            PRIVATE
            core
            Arrow::arrow_shared
            ${ARROW_DATASET_LIB}
            Parquet::parquet_shared
            ${UUID_LIBRARY}
            ${ANTLR4_RUNTIME}
            LLVMSupport
            LLVMCore
            TBB::tbb
    )
endif()


# Find CDS library
if(APPLE)
    find_package(LLVM REQUIRED CONFIG PATHS /opt/homebrew/opt/llvm/lib/cmake/llvm)
else()
    find_package(LLVM REQUIRED)
endif()

include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

# Add specific paths for ArrowDataset
list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/cmake/ArrowDataset")
list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/cmake")

# Now try to find ArrowDataset with explicit CONFIG mode
find_package(ArrowDataset QUIET CONFIG)
if(NOT ArrowDataset_FOUND)
    # Try without CONFIG mode
    find_package(ArrowDataset QUIET)
endif()

# Manual inclusion for ArrowAcero if not found
if(NOT ArrowAcero_FOUND)
    message(STATUS "Attempting manual ArrowAcero inclusion...")
    if(EXISTS "/usr/lib/x86_64-linux-gnu/cmake/ArrowAcero/ArrowAceroTargets.cmake")
        include("/usr/lib/x86_64-linux-gnu/cmake/ArrowAcero/ArrowAceroTargets.cmake")
        set(ArrowAcero_FOUND TRUE)
        message(STATUS "Manually included ArrowAcero targets")
    endif()
endif()

# Manual inclusion for ArrowCompute if not found
if(NOT ArrowCompute_FOUND)
    message(STATUS "Attempting manual ArrowCompute inclusion...")
    if(EXISTS "/usr/lib/x86_64-linux-gnu/cmake/ArrowCompute/ArrowComputeTargets.cmake")
        include("/usr/lib/x86_64-linux-gnu/cmake/ArrowCompute/ArrowComputeTargets.cmake")
        set(ArrowCompute_FOUND TRUE)
        message(STATUS "Manually included ArrowCompute targets")
    else()
        # ArrowCompute is likely part of the main Arrow library on Linux
        # Create an alias from ArrowCompute::arrow_compute_shared to Arrow::arrow_shared
        if(TARGET Arrow::arrow_shared AND NOT TARGET ArrowCompute::arrow_compute_shared)
            add_library(ArrowCompute::arrow_compute_shared ALIAS Arrow::arrow_shared)
            message(STATUS "Created ArrowCompute::arrow_compute_shared alias to Arrow::arrow_shared")
            set(ArrowCompute_FOUND TRUE)
        endif()
    endif()
endif()

# If ArrowDataset still not found, try manual inclusion
if(NOT ArrowDataset_FOUND)
    message(STATUS "Attempting manual ArrowDataset inclusion...")
    # Try to manually include the ArrowDataset targets
    if(EXISTS "/usr/lib/x86_64-linux-gnu/cmake/ArrowDataset/ArrowDatasetTargets.cmake")
        include("/usr/lib/x86_64-linux-gnu/cmake/ArrowDataset/ArrowDatasetTargets.cmake")
        set(ArrowDataset_FOUND TRUE)
        message(STATUS "Manually included ArrowDataset targets")
    endif()
endif()

# ArrowDataset detection complete

if(ArrowDataset_FOUND AND TARGET ArrowDataset::arrow_dataset_shared)
    set(ARROW_DATASET_LIB ArrowDataset::arrow_dataset_shared)
    message(STATUS "Using ArrowDataset::arrow_dataset_shared")
elseif(TARGET Arrow::arrow_dataset_shared)
    set(ARROW_DATASET_LIB Arrow::arrow_dataset_shared)
    message(STATUS "Using Arrow::arrow_dataset_shared")
else()
    # Fallback: assume dataset functionality is in the main arrow library
    set(ARROW_DATASET_LIB Arrow::arrow_shared)
    message(STATUS "Using Arrow::arrow_shared (assuming dataset functionality included)")
endif()

find_package(benchmark REQUIRED)
find_package(GTest REQUIRED)
find_package(TBB REQUIRED)

include_directories(libs/linenoise)
include_directories(libs/json)

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
#if(NOT TARGET nlohmann_json::nlohmann_json)
#  # Include FetchContent for downloading dependencies
#  include(FetchContent)
#
#  # Add nlohmann/json
#  FetchContent_Declare(
#    json
#    GIT_REPOSITORY https://github.com/nlohmann/json.git
#    GIT_TAG v3.11.2
#  )
#  FetchContent_MakeAvailable(json)
#else()
#  message(STATUS "nlohmann_json target already exists, skipping FetchContent")
#endif()
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
        src/query.cpp
        src/query_execution.cpp
        src/join.cpp
        src/row.cpp
        src/storage.cpp
        src/metadata.cpp
        src/file_utils.cpp
        src/snapshot.cpp
        src/edge_store.cpp
        src/table_info.cpp
        src/utils.cpp
        src/schema.cpp
        src/arrow_utils.cpp
        src/types.cpp
        src/clock.cpp
)

target_include_directories(core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/libs/json>
        $<INSTALL_INTERFACE:include/tundradb>
    PRIVATE
        ${ARROW_INCLUDE_DIR}
        /usr/local/include
)

# Link dependencies
target_link_libraries(core
    PUBLIC
        Arrow::arrow_shared
        ${ARROW_DATASET_LIB}
        Parquet::parquet_shared
        spdlog::spdlog
        ${UUID_LIBRARY}
        ${CDS_LIB}
        TBB::tbb
)

# Add Arrow Compute library for Linux (Arrow 21.0.0+)
if(TARGET Arrow::arrow_compute_shared)
    target_link_libraries(core PUBLIC Arrow::arrow_compute_shared)
    message(STATUS "Linking Arrow Compute library")
elseif(EXISTS "/usr/lib/x86_64-linux-gnu/libarrow_compute.so")
    target_link_libraries(core PUBLIC /usr/lib/x86_64-linux-gnu/libarrow_compute.so)
    message(STATUS "Linking Arrow Compute library (direct path)")
endif()

# Explicitly link core against UUID
#target_link_libraries(core
#    PUBLIC  # Changed from PRIVATE to PUBLIC so dependents can use it
#        uuid  # Directly use the library name
#)

# ---------------------------------------------------------------------------
# ANTLR Integration (needed by shell)
# ---------------------------------------------------------------------------
if(TUNDRADB_BUILD_SHELL)
    # Find Java for running ANTLR generator
    find_package(Java REQUIRED)

    # Define ANTLR jar location
    set(ANTLR_JAR_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/antlr/lib/antlr-4.13.2-complete.jar")

    # Set up the output directory
    set(ANTLR_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/antlr_generated")
    file(MAKE_DIRECTORY ${ANTLR_OUTPUT_DIR})

    # Define generated files
    set(TUNDRAQL_GENERATED_SRC
        ${ANTLR_OUTPUT_DIR}/TundraQLLexer.cpp
        ${ANTLR_OUTPUT_DIR}/TundraQLParser.cpp
        ${ANTLR_OUTPUT_DIR}/TundraQLBaseListener.cpp
        ${ANTLR_OUTPUT_DIR}/TundraQLListener.cpp
        ${ANTLR_OUTPUT_DIR}/TundraQLBaseVisitor.cpp
        ${ANTLR_OUTPUT_DIR}/TundraQLVisitor.cpp
    )

    # Mark files as generated
    foreach(src_file ${TUNDRAQL_GENERATED_SRC})
        set_source_files_properties(
            ${src_file}
            PROPERTIES
            GENERATED TRUE
        )
    endforeach()

    # Custom target for ANTLR parser generation
    add_custom_target(GenerateTundraQLParser DEPENDS ${TUNDRAQL_GENERATED_SRC})

    # Custom command to generate ANTLR files
    add_custom_command(
        OUTPUT ${TUNDRAQL_GENERATED_SRC}
        COMMAND
        ${Java_JAVA_EXECUTABLE} -jar ${ANTLR_JAR_LOCATION} -Werror -Dlanguage=Cpp -listener -visitor -o ${ANTLR_OUTPUT_DIR} -package tundraql ${CMAKE_CURRENT_SOURCE_DIR}/antlr/TundraQL.g4
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/antlr/TundraQL.g4
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        COMMENT "Generating ANTLR4 parser"
    )

    # Add ANTLR runtime library
    if(APPLE)
        find_library(ANTLR4_RUNTIME antlr4-runtime PATHS /opt/homebrew/lib /usr/local/lib)
        include_directories(/opt/homebrew/include/antlr4-runtime /usr/local/include/antlr4-runtime)
    else()
        find_library(ANTLR4_RUNTIME antlr4-runtime PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu)
        find_path(ANTLR4_INCLUDE_DIR antlr4-runtime.h PATHS
            /usr/local/include/antlr4-runtime
            /usr/include/antlr4-runtime
            /usr/local/include
            /usr/include
        )
        if(ANTLR4_INCLUDE_DIR)
            include_directories(${ANTLR4_INCLUDE_DIR})
            message(STATUS "Found ANTLR4 headers at: ${ANTLR4_INCLUDE_DIR}")
        else()
            include_directories(/usr/local/include)
            message(WARNING "ANTLR4 headers not found in expected paths, using /usr/local/include as fallback")
        endif()
    endif()

    if(NOT ANTLR4_RUNTIME)
        message(WARNING "ANTLR4 runtime library not found. Please install it via Homebrew or from source.")
    else()
        message(STATUS "Found ANTLR4 runtime library: ${ANTLR4_RUNTIME}")
    endif()

    # Create a library for the ANTLR parser
    add_library(tundraql_parser ${TUNDRAQL_GENERATED_SRC})
    add_dependencies(tundraql_parser GenerateTundraQLParser)

    target_include_directories(tundraql_parser
        PUBLIC
            ${ANTLR_OUTPUT_DIR}
        PRIVATE
            /usr/local/include
            /usr/local/include/antlr4-runtime
            /usr/include
            /usr/include/antlr4-runtime
    )

    if(ANTLR4_RUNTIME)
        target_link_libraries(tundraql_parser
            PRIVATE
                ${ANTLR4_RUNTIME}
        )
    endif()

    # Interactive shell executable
    add_executable(tundra_shell src/tundra_shell.cpp libs/linenoise/linenoise.c)

    target_include_directories(tundra_shell
        PRIVATE
            ${ANTLR_OUTPUT_DIR}
    )

    add_dependencies(tundra_shell GenerateTundraQLParser)

    target_link_libraries(tundra_shell
        PRIVATE
            core
            tundraql_parser
            Arrow::arrow_shared
            ${ARROW_DATASET_LIB}
            Parquet::parquet_shared
            ${UUID_LIBRARY}
            ${ANTLR4_RUNTIME}
            LLVMSupport
            LLVMCore
    )
endif()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(TUNDRADB_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Install rules  –  library + headers + shell + CMake package config
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Install the core library
install(TARGETS core
    EXPORT TundraDBTargets
    LIBRARY  DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE  DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME  DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tundradb
)

# Install public headers
install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tundradb
    FILES_MATCHING PATTERN "*.hpp"
)

# Install the bundled nlohmann json header
install(FILES libs/json/json.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tundradb
)

# Install the shell executable
if(TUNDRADB_BUILD_SHELL)
    install(TARGETS tundra_shell
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()

# Generate the version file
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/TundraDBConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Export the targets
install(EXPORT TundraDBTargets
    FILE TundraDBTargets.cmake
    NAMESPACE TundraDB::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TundraDB
)

# Generate and install the config file
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TundraDBConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/TundraDBConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TundraDB
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/TundraDBConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/TundraDBConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TundraDB
)

# ---------------------------------------------------------------------------
# CPack — generate distributable packages (.tar.gz, .deb, .rpm, .dmg)
# ---------------------------------------------------------------------------
set(CPACK_PACKAGE_NAME "TundraDB")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VENDOR "TundraDB")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Graph database with temporal versioning, per-schema IDs, and Apache Arrow")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/yourusername/tundradb")
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
    set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
endif()
set(CPACK_PACKAGE_FILE_NAME "TundraDB-${PROJECT_VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")

# Archive generators (works everywhere)
set(CPACK_GENERATOR "TGZ")

# macOS: also generate .dmg
if(APPLE)
    list(APPEND CPACK_GENERATOR "DragNDrop")
endif()

# Linux: generate .deb and .rpm
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Debian/Ubuntu .deb
    list(APPEND CPACK_GENERATOR "DEB")
    set(CPACK_DEBIAN_PACKAGE_MAINTAINER "TundraDB Maintainers")
    set(CPACK_DEBIAN_PACKAGE_DEPENDS "libarrow-dev, libparquet-dev, libtbb-dev, libspdlog-dev")
    set(CPACK_DEBIAN_PACKAGE_SECTION "database")

    # Fedora/RHEL .rpm
    list(APPEND CPACK_GENERATOR "RPM")
    set(CPACK_RPM_PACKAGE_LICENSE "MIT")
    set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
    set(CPACK_RPM_PACKAGE_REQUIRES "arrow-devel, parquet-devel, tbb-devel, spdlog-devel")
endif()

# Source package
set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_SOURCE_IGNORE_FILES
    "/build/"
    "/build_release/"
    "/build_tsan/"
    "/dist/"
    "/\\\\.git/"
    "/test_db_"
    "/tundra_profile"
    "\\\\.trace/"
)

include(CPack)

