#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>
#include <memory>
#include <string>
#include "../include/core.hpp"
#include "../include/logger.hpp"

using namespace std::string_literals;
using namespace tundradb;

// Helper function to generate a unique test directory name
std::string get_test_dir_name() {
    return "./testdb_" + std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count());
}

// Helper function to create a user node with given name and age
std::shared_ptr<Node> create_user_node(Database& db, const std::string& name, int64_t age) {
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> fields = {
        {"name", create_str_array(name).ValueOrDie()},
        {"age", create_int64_array(age).ValueOrDie()}
    };

    return db.create_node("users", fields).ValueOrDie();
}

// Helper to create a schema for users
void create_user_schema(Database& db) {
    auto name_field = arrow::field("name", arrow::utf8());
    auto age_field = arrow::field("age", arrow::int64());
    auto schema = arrow::schema({name_field, age_field});
    
    db.get_schema_registry()->add("users", schema).ValueOrDie();
}

// Test fixture to handle setup and teardown
class DatabaseSnapshotTest : public ::testing::Test {
protected:
    std::string test_dir;
    
    void SetUp() override {
        test_dir = get_test_dir_name();
        // Create the directory
        std::filesystem::create_directories(test_dir);
    }
    
    void TearDown() override {
        // Clean up the test directory
        std::filesystem::remove_all(test_dir);
    }
    
    // Helper to create database with test settings
    std::shared_ptr<Database> create_test_database() {
        auto config = make_config()
            .with_db_path(test_dir)
            .with_shard_capacity(4)  // Small capacity for testing
            .with_chunk_size(2)      // Small chunk size for testing
            .with_persistence_enabled(true)
            .build();
            
        auto db = std::make_shared<Database>(config);
        return db;
    }
    
    // Helper to create test data (8 users)
    void create_test_users(Database& db, int from, int until)   {
        for (int i = from; i < until; ++i) {
            create_user_node(db, "User " + std::to_string(i), 20 + i);
        }
    }
    
    // Helper to verify data in the table
    void verify_user_table(const std::shared_ptr<arrow::Table>& table, int expected_count) {
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(table->num_rows(), expected_count);
        ASSERT_EQ(table->num_columns(), 3); // id, name, age
        
        // Verify the schema
        auto schema = table->schema();
        ASSERT_EQ(schema->field(0)->name(), "id");
        ASSERT_EQ(schema->field(1)->name(), "name");
        ASSERT_EQ(schema->field(2)->name(), "age");
    }
};


// Test Scenario 1: Create DB, add nodes, snapshot, recreate, verify
TEST_F(DatabaseSnapshotTest, BasicSnapshotAndReload) {
    // Step 1: Create and initialize DB
    auto db = create_test_database();
    ASSERT_TRUE(db->initialize().ValueOrDie());
    
    // Verify data and metadata folders are created
    ASSERT_TRUE(std::filesystem::exists(test_dir + "/data"));
    ASSERT_TRUE(std::filesystem::exists(test_dir + "/metadata"));
    
    // Step 2: Create schema and add users
    create_user_schema(*db);
    create_test_users(*db, 0, 8);
    
    // Step 3: Create snapshot
    auto snapshot = db->create_snapshot().ValueOrDie();
    ASSERT_TRUE(snapshot != nullptr);
    
    // Verify snapshot was created and has correct structure
    ASSERT_TRUE(!snapshot->manifest_location.empty());
    ASSERT_TRUE(std::filesystem::exists(snapshot->manifest_location));
    
    // Step 4: Create a new DB instance (simulating restart)
    db.reset();  // Destroy first instance
    auto new_db = create_test_database();
    create_user_schema(*new_db);
    ASSERT_TRUE(new_db->initialize().ValueOrDie());
    
    // Step 5: Get table and verify data
    std::cout << "Step 5: Get table and verify data" << std::endl;
    auto table = new_db->get_table("users").ValueOrDie();
    verify_user_table(table, 8);
}


// Test Scenario 2: Create DB, add nodes, snapshot, create another snapshot without changes, recreate, verify
TEST_F(DatabaseSnapshotTest, SnapshotWithoutChanges) {
    // Step 1: Create and initialize DB
    auto db = create_test_database();
    ASSERT_TRUE(db->initialize().ValueOrDie());
    
    // Step 2: Create schema and add users
    create_user_schema(*db);
    create_test_users(*db, 0, 8);
    
    // Step 3: Create first snapshot
    auto snapshot1 = db->create_snapshot().ValueOrDie();
    ASSERT_TRUE(snapshot1 != nullptr);
    std::string first_manifest_location = snapshot1->manifest_location;


    // Step 4: Create second snapshot without changes
    auto snapshot2 = db->create_snapshot().ValueOrDie();
    ASSERT_TRUE(snapshot2 != nullptr);
    std::string second_manifest_location = snapshot2->manifest_location;
    
    // Different snapshot IDs and files
    ASSERT_NE(snapshot1->id, snapshot2->id);
    ASSERT_NE(first_manifest_location, second_manifest_location);
    
    // Get manifest content to compare
    auto manifest1 = read_json_file<Manifest>(first_manifest_location).ValueOrDie();
    auto manifest2 = read_json_file<Manifest>(second_manifest_location).ValueOrDie();
    
    // Different manifest IDs
    ASSERT_NE(manifest1.id, manifest2.id);
    
    // Same number of shards
    ASSERT_EQ(manifest1.shards.size(), manifest2.shards.size());
    
    // Same data files references
    for (size_t i = 0; i < manifest1.shards.size(); ++i) {
        const auto& shard1 = manifest1.shards[i];
        
        // Find matching shard in manifest2
        bool found = false;
        for (const auto& shard2 : manifest2.shards) {
            if (shard1.id == shard2.id && shard1.schema_name == shard2.schema_name) {
                ASSERT_EQ(shard1.data_file, shard2.data_file);  // Same data file reference
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found);
    }
    
    // Step 5: Create a new DB instance (simulating restart)
    db.reset();  // Destroy first instance
    auto new_db = create_test_database();
    create_user_schema(*new_db);
    ASSERT_TRUE(new_db->initialize().ValueOrDie());
    
    // Step 6: Get table and verify data
    auto table = new_db->get_table("users").ValueOrDie();
    verify_user_table(table, 8);
}


// Test Scenario 3: Create DB, add nodes, snapshot, add more nodes, snapshot again, recreate, verify
TEST_F(DatabaseSnapshotTest, SnapshotWithAdditionalData) {
    // Step 1: Create and initialize DB
    auto db = create_test_database();
    ASSERT_TRUE(db->initialize().ValueOrDie());
    
    // Step 2: Create schema and add users
    create_user_schema(*db);
    create_test_users(*db, 0, 8);  // Create 4 users initially
    
    // Step 3: Create first snapshot
    auto snapshot1 = db->create_snapshot().ValueOrDie();
    ASSERT_TRUE(snapshot1 != nullptr);
    
    // Step 4: Add 4 more users
    create_test_users(*db, 8, 16);  // Create 4 more users
    
    // Step 5: Create second snapshot with additional data
    auto snapshot2 = db->create_snapshot().ValueOrDie();
    ASSERT_TRUE(snapshot2 != nullptr);
    
    // Different snapshot IDs
    ASSERT_NE(snapshot1->id, snapshot2->id);
    
    // Step 6: Create a new DB instance (simulating restart)
    db.reset();  // Destroy first instance
    auto new_db = create_test_database();
    create_user_schema(*new_db);
    ASSERT_TRUE(new_db->initialize().ValueOrDie());
    
    // Step 7: Get table and verify data - should have all 8 users
    auto table = new_db->get_table("users").ValueOrDie();
    verify_user_table(table, 16);
}


int main(int argc, char** argv) {
    // Initialize logger
    tundradb::Logger::getInstance().setLevel(tundradb::LogLevel::INFO);
    
    // Optional: set log to file for test output
    // tundradb::Logger::getInstance().setLogToFile("snapshot_test.log");
    
    tundradb::log_info("Starting snapshot tests");
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 