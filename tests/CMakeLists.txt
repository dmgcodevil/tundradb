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
        core
        Arrow::arrow_shared
        Parquet::parquet_shared
        nlohmann_json::nlohmann_json
        GTest::GTest
        GTest::Main
        pthread
        ${UUID_LIBRARY}
)



# Simple test registration
add_test(NAME ShardingTest COMMAND sharding_test)
