# Enable testing
enable_testing()

# Include Google Test
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Add sharding test
add_executable(sharding_test
    test_sharding.cpp
)

# Link against Arrow and GTest
target_link_libraries(sharding_test
    PRIVATE
    Arrow::arrow_shared
    GTest::GTest
    GTest::Main
    pthread
)

# Simple test registration
add_test(NAME ShardingTest COMMAND sharding_test) 