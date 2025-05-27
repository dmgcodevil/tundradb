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

# ArrowDataset might be part of Arrow or a separate package
# Try to find it as a separate package first
find_package(ArrowDataset QUIET)
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

find_package(Parquet REQUIRED)
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
        src/storage.cpp
        src/metadata.cpp
        src/file_utils.cpp
        src/snapshot.cpp
        src/edge_store.cpp
        src/table_info.cpp
        src/utils.cpp
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
        ${ARROW_DATASET_LIB}
        Parquet::parquet_shared
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
        ${UUID_LIBRARY}
)

# Interactive shell executable
add_executable(tundra_shell src/tundra_shell.cpp libs/linenoise/linenoise.c)

target_link_libraries(tundra_shell
    PRIVATE
        core
        tundraql_parser
        Arrow::arrow_shared
        Parquet::parquet_shared
        ${UUID_LIBRARY}
        ${ANTLR4_RUNTIME}
)

# ANTLR Integration
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
# On macOS with Homebrew, you may need to adjust these paths
if(APPLE)
    find_library(ANTLR4_RUNTIME antlr4-runtime PATHS /opt/homebrew/lib /usr/local/lib)
    include_directories(/opt/homebrew/include/antlr4-runtime /usr/local/include/antlr4-runtime)
else()
    # Linux - look in standard locations and also check common install paths
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
        message(WARNING "ANTLR4 headers not found")
    endif()
endif()

if(NOT ANTLR4_RUNTIME)
    message(WARNING "ANTLR4 runtime library not found. Please install it via Homebrew or from source.")
else()
    message(STATUS "Found ANTLR4 runtime library: ${ANTLR4_RUNTIME}")
endif()

# Create a library for the ANTLR parser
add_library(tundraql_parser
    ${TUNDRAQL_GENERATED_SRC}
)

# Make sure the parser is generated first
add_dependencies(tundraql_parser GenerateTundraQLParser)

# Include the generated header files
target_include_directories(tundraql_parser
    PUBLIC
        ${ANTLR_OUTPUT_DIR}
)

# Link ANTLR runtime to the parser library
if(ANTLR4_RUNTIME)
    target_link_libraries(tundraql_parser
        PRIVATE
            ${ANTLR4_RUNTIME}
    )
endif()

# Link the parser library to the main executable
target_link_libraries(tundradb
    PRIVATE
        tundraql_parser
)

# Enable testing
enable_testing()

# Add tests directory
add_subdirectory(tests)

