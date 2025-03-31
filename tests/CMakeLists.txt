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

# Link against Arrow and GTest
target_link_libraries(sharding_test
    PRIVATE
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        nlohmann_json::nlohmann_json
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
        nlohmann_json::nlohmann_json
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

# Apply sanitizer flags to all test targets if enabled
if(ENABLE_SANITIZERS)
    target_compile_options(sharding_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(sharding_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(snapshot_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(snapshot_test PRIVATE ${SANITIZER_LINK_FLAGS})
    
    target_compile_options(concurrency_test PRIVATE ${SANITIZER_COMPILE_FLAGS})
    target_link_options(concurrency_test PRIVATE ${SANITIZER_LINK_FLAGS})
endif()

# Simple test registration
add_test(NAME ShardingTest COMMAND sharding_test)
add_test(NAME SnapshotTest COMMAND snapshot_test)
add_test(NAME ConcurrencyTest COMMAND concurrency_test)
