# Enable testing
enable_testing()

# Include Google Test
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Add options for sanitizer support
option(ENABLE_SANITIZERS "Enable sanitizers in tests" OFF)
set(SANITIZER_TYPE "address" CACHE STRING "Sanitizer type (address, thread, undefined, memory)")

# Sanitizer flags
if(ENABLE_SANITIZERS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        message(STATUS "Enabling ${SANITIZER_TYPE} sanitizer for tests")
        set(SANITIZER_COMPILE_FLAGS "")
        set(SANITIZER_LINK_FLAGS "")
        
        if(SANITIZER_TYPE STREQUAL "address")
            list(APPEND SANITIZER_COMPILE_FLAGS "-fsanitize=address")
            list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=address")
        elseif(SANITIZER_TYPE STREQUAL "thread")
            list(APPEND SANITIZER_COMPILE_FLAGS "-fsanitize=thread")
            list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=thread")
            
            # Add TSan suppression file
            set(TSAN_SUPPRESSIONS_FILE "${CMAKE_CURRENT_SOURCE_DIR}/tsan_suppressions.txt")
            if(EXISTS ${TSAN_SUPPRESSIONS_FILE})
                message(STATUS "Using TSan suppressions file: ${TSAN_SUPPRESSIONS_FILE}")
                # Create environment variables for running tests
                set(TSAN_OPTIONS_VALUE "suppressions=${TSAN_SUPPRESSIONS_FILE}")
                if(APPLE)
                    # On macOS, we need to set this globally for the process
                    add_compile_definitions(TSAN_OPTIONS="${TSAN_OPTIONS_VALUE}")
                endif()
                
                # For GCC we can use this
                if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
                    add_compile_options("-fno-sanitize-recover=all")
                endif()
            else()
                message(WARNING "TSan suppressions file not found: ${TSAN_SUPPRESSIONS_FILE}")
            endif()
        elseif(SANITIZER_TYPE STREQUAL "undefined")
            list(APPEND SANITIZER_COMPILE_FLAGS "-fsanitize=undefined")
            list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=undefined")
        elseif(SANITIZER_TYPE STREQUAL "memory")
            list(APPEND SANITIZER_COMPILE_FLAGS "-fsanitize=memory")
            list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=memory")
        else()
            message(WARNING "Unknown sanitizer type: ${SANITIZER_TYPE}")
        endif()
        
        # Add additional common compiler flags
        list(APPEND SANITIZER_COMPILE_FLAGS "-fno-omit-frame-pointer")
        
    else()
        message(WARNING "Sanitizers requested but not supported with this compiler")
    endif()
endif()

# Add sharding test
add_executable(sharding_test
    test_sharding.cpp
)

# Add snapshot test
add_executable(snapshot_test
    snapshot_test.cpp
)

# Add new concurrency test
add_executable(concurrency_test
    concurrency_test.cpp
)

add_executable(
        concurrent_set_stress_test
        concurrent_set_stress_test.cpp
)

# Add edge store test
add_executable(edge_store_test
    edge_store_test.cpp
)

# Add table info test
add_executable(table_info_test
    table_info_test.cpp
)

# Add schema utils test
add_executable(schema_utils_test
    schema_utils_test.cpp
)

# Add database test
add_executable(database_test
    database_test.cpp
)


add_executable(join_test
        join_test.cpp
)

# Add benchmark test
add_executable(benchmark_test
    benchmark_test.cpp
)

add_executable(where_pushdown_join_test
        where_pushdown_join_test.cpp)

# Add WHERE expression test
add_executable(where_expression_test
        where_expression_test.cpp)

add_executable(memory_arena_test
        memory_arena_test.cpp)

add_executable(free_list_arena_test
        free_list_arena_test.cpp)

# Link against Arrow and GTest
target_link_libraries(sharding_test
    PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        GTest::GTest
        GTest::Main
        pthread
        ${UUID_LIBRARY}
)

target_link_libraries(snapshot_test
    PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        GTest::GTest
        GTest::Main
        pthread
)

# Configure the concurrency test
target_link_libraries(concurrency_test
    PRIVATE
        core
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

target_link_libraries(concurrent_set_stress_test
        PRIVATE
        core
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

# Link against required libraries
target_link_libraries(edge_store_test
    PRIVATE
        core
        Arrow::arrow_shared
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

# Link against required libraries
target_link_libraries(table_info_test
    PRIVATE
        core
        Arrow::arrow_shared
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

# Link against required libraries
target_link_libraries(schema_utils_test
    PRIVATE
        core
        Arrow::arrow_shared
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

# Link against required libraries
target_link_libraries(database_test
    PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

target_link_libraries(join_test
        PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

# Link benchmark test with Google Benchmark and other dependencies
target_link_libraries(benchmark_test
    PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        benchmark::benchmark
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

target_link_libraries(where_pushdown_join_test
        PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        benchmark::benchmark
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

target_link_libraries(where_expression_test
        PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        GTest::GTest
        GTest::Main
        pthread
        TBB::tbb
)

target_link_libraries(memory_arena_test
        PRIVATE
        GTest::GTest
        GTest::Main
)

target_link_libraries(free_list_arena_test
        PRIVATE
        GTest::GTest
        GTest::Main
        spdlog::spdlog
)

# Apply sanitizer flags to all test targets if enabled
if(ENABLE_SANITIZERS)
    target_compile_options(sharding_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(sharding_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(snapshot_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(snapshot_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(concurrency_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(concurrency_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(edge_store_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(edge_store_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(table_info_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(table_info_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(schema_utils_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(schema_utils_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(database_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(database_test PRIVATE ${SANITIZER_LINK_FLAGS})

    target_compile_options(join_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(join_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(where_expression_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(where_expression_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(free_list_arena_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(free_list_arena_test PRIVATE ${SANITIZER_LINK_FLAGS})
endif()

# Simple test registration
add_test(NAME ShardingTest COMMAND sharding_test)
add_test(NAME SnapshotTest COMMAND snapshot_test)
add_test(NAME ConcurrencyTest COMMAND concurrency_test)
add_test(NAME ConcurrentSetStressTest COMMAND concurrent_set_stress_test)
add_test(NAME EdgeStoreTest COMMAND edge_store_test)
add_test(NAME TableInfoTest COMMAND table_info_test)
add_test(NAME SchemaUtilsTest COMMAND schema_utils_test)
add_test(NAME DatabaseTest COMMAND database_test)
add_test(NAME JoinTest COMMAND join_test)
add_test(NAME WhereExpressionTest COMMAND where_expression_test)
add_test(NAME MemoryArenaTest COMMAND memory_arena_test)
add_test(NAME FreeListArenaTest COMMAND free_list_arena_test)

# Set TSan options for tests after they've been registered
if(ENABLE_SANITIZERS AND SANITIZER_TYPE STREQUAL "thread" AND EXISTS ${TSAN_SUPPRESSIONS_FILE})
    foreach(test_target ShardingTest SnapshotTest ConcurrencyTest ConcurrentSetStressTest EdgeStoreTest TableInfoTest SchemaUtilsTest DatabaseTest)
        set_property(
            TEST ${test_target}
            PROPERTY ENVIRONMENT "TSAN_OPTIONS=${TSAN_OPTIONS_VALUE}"
        )
    endforeach()
endif()

set_tests_properties(
        ShardingTest
        SnapshotTest
        ConcurrencyTest
        ConcurrentSetStressTest
        EdgeStoreTest
        TableInfoTest
        SchemaUtilsTest
        DatabaseTest
        JoinTest
        WhereExpressionTest
        FreeListArenaTest
        PROPERTIES
        ISOLATED TRUE  # This ensures tests run in isolation
)