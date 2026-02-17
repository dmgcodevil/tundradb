#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/json/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include <filesystem>

#include "core.hpp"
#include "logger.hpp"

using namespace tundradb;

class DatabaseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a unique test directory
    test_db_path = "test_db_" + std::to_string(now_millis());

    // Ensure the test directory doesn't exist
    std::filesystem::remove_all(test_db_path);

    // Set up logger
    // Logger::get_instance().set_level(LogLevel::DEBUG);
  }

  void TearDown() override {
    // Clean up after tests
    // std::filesystem::remove_all(test_db_path);
  }

  // Helper method to create a database with persistence enabled
  std::shared_ptr<Database> createDatabase(size_t shard_capacity = 1000) {
    auto config = make_config()
                      .with_db_path(test_db_path)
                      .with_shard_capacity(shard_capacity)
                      .with_chunk_size(1000)
                      .build();

    auto db = std::make_shared<Database>(config);
    return db;
  }

  // Helper to create a simple user schema
  void setupUserSchema(Database& db) {
    auto name_field = arrow::field("name", arrow::utf8());
    auto age_field = arrow::field("age", arrow::int64());
    auto schema = arrow::schema({name_field, age_field});
    db.get_schema_registry()->create("users", schema).ValueOrDie();
  }

  // Helper to create user nodes
  std::vector<std::shared_ptr<Node>> createUsers(Database& db, int count) {
    std::vector<std::shared_ptr<Node>> nodes;
    for (int i = 0; i < count; i++) {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"user" + std::to_string(i)}},
          {"age", Value{int64_t(20 + i)}}};

      auto node = db.create_node("users", data).ValueOrDie();
      nodes.push_back(node);
    }
    return nodes;
  }

  // Helper to verify counters match expected values
  void verifyCounters(
      Database& db, int64_t expected_node_id, int64_t expected_shard_id,
      int64_t expected_edge_id,
      std::unordered_map<std::string, int64_t> expected_index_counters) {
    // Need to access private members, so this is just for illustration of what
    // we'd want to test Would need to add accessor methods or make these
    // members accessible for testing

    // Check node counter
    auto node_manager_id = expected_node_id;  // Would access private field
    EXPECT_EQ(node_manager_id, expected_node_id);

    // Check shard counter
    auto shard_manager_id = expected_shard_id;  // Would access private field
    EXPECT_EQ(shard_manager_id, expected_shard_id);

    // Check edge counter
    auto edge_store_id = expected_edge_id;  // Would access private field
    EXPECT_EQ(edge_store_id, expected_edge_id);

    // Check index counters
    for (const auto& [schema, counter] : expected_index_counters) {
      auto index_counter = counter;  // Would access private field
      EXPECT_EQ(index_counter, counter);
    }
  }

  // Helper to construct a valid manifest path
  std::string getManifestPath(const std::string& test_db_path,
                              const std::string& manifest_location) {
    // Check if manifest_location already contains the test_db_path
    if (manifest_location.find(test_db_path) == 0) {
      // It already has the path, use it directly
      return manifest_location;
    } else {
      // It's a relative path, so prepend the test_db_path
      return test_db_path + "/" + manifest_location;
    }
  }

  // Helper function to read parquet file and return table
  arrow::Result<std::shared_ptr<arrow::Table>> readParquetFile(
      const std::string& file_path) {
    // Open the parquet file
    ARROW_ASSIGN_OR_RAISE(auto input_file,
                          arrow::io::ReadableFile::Open(file_path));

    // Create a parquet reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_ASSIGN_OR_RAISE(
        reader,
        parquet::arrow::OpenFile(input_file, arrow::default_memory_pool()));

    // Read the table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

    return table;
  }

  // Helper function to validate data file contents
  void validateDataFile(const std::string& manifest_path,
                        const std::string& expected_schema_name,
                        int expected_record_count,
                        const std::vector<int64_t>& expected_user_ids) {
    // 1. Read manifest
    auto manifest_json =
        read_json_file<nlohmann::json>(manifest_path).ValueOrDie();

    // 2. Find the shard for the expected schema
    std::string data_file_path;
    bool found_shard = false;

    for (const auto& shard : manifest_json["shards"]) {
      std::string schema_name = shard["schema_name"];
      if (schema_name == expected_schema_name) {
        data_file_path = shard["data_file"];
        found_shard = true;

        // Validate record count in manifest
        int64_t record_count = shard["record_count"];
        EXPECT_EQ(record_count, expected_record_count)
            << "Record count mismatch in manifest for " << expected_schema_name;
        break;
      }
    }

    ASSERT_TRUE(found_shard)
        << "Could not find shard for schema " << expected_schema_name;
    ASSERT_FALSE(data_file_path.empty()) << "Data file path is empty";

    // 3. Handle data file path correctly
    std::string full_data_file_path = data_file_path;

    // If the path is not absolute, check if it's already relative to current
    // directory
    if (!std::filesystem::path(data_file_path).is_absolute()) {
      // First, try the path as-is (might be relative to current working
      // directory)
      if (!std::filesystem::exists(data_file_path)) {
        // If that doesn't work, try prepending test_db_path
        full_data_file_path = test_db_path + "/" + data_file_path;
      }
    }

    // 4. Verify file exists
    ASSERT_TRUE(std::filesystem::exists(full_data_file_path))
        << "Data file does not exist: " << full_data_file_path;

    // 5. Read parquet file and validate contents
    auto table_result = readParquetFile(full_data_file_path);
    ASSERT_TRUE(table_result.ok())
        << "Failed to read parquet file: " << table_result.status().ToString();

    auto table = table_result.ValueOrDie();

    // 6. Validate number of rows
    EXPECT_EQ(table->num_rows(), expected_record_count)
        << "Actual row count in parquet file doesn't match expected";

    // 7. Validate specific user IDs if provided
    if (!expected_user_ids.empty()) {
      auto id_column = table->GetColumnByName("id");
      ASSERT_NE(id_column, nullptr) << "Could not find 'id' column in table";

      std::set<int64_t> actual_ids;
      for (int chunk_idx = 0; chunk_idx < id_column->num_chunks();
           chunk_idx++) {
        auto chunk = std::static_pointer_cast<arrow::Int64Array>(
            id_column->chunk(chunk_idx));
        for (int64_t i = 0; i < chunk->length(); i++) {
          actual_ids.insert(chunk->Value(i));
        }
      }

      std::set<int64_t> expected_ids_set(expected_user_ids.begin(),
                                         expected_user_ids.end());
      EXPECT_EQ(actual_ids, expected_ids_set)
          << "User IDs in parquet file don't match expected IDs";
    }
  }

  std::string test_db_path;
};

// Test 1: Create a new DB, create a snapshot, verify file structure and
// counters
TEST_F(DatabaseTest, CreateDbAndSnapshot) {
  // Create a new database
  auto db = createDatabase();
  db->initialize().ValueOrDie();

  // Create a snapshot
  auto snapshot = db->create_snapshot().ValueOrDie();

  // Verify directory structure exists
  EXPECT_TRUE(std::filesystem::exists(test_db_path));
  EXPECT_TRUE(std::filesystem::exists(test_db_path + "/data"));
  EXPECT_TRUE(std::filesystem::exists(test_db_path + "/db_info.json"));

  // Verify manifest file exists and has expected structure
  std::string manifest_path =
      getManifestPath(test_db_path, snapshot.manifest_location);
  EXPECT_TRUE(std::filesystem::exists(manifest_path));

  // Read manifest file and verify content
  auto manifest_json =
      read_json_file<nlohmann::json>(manifest_path).ValueOrDie();
  EXPECT_EQ(manifest_json["edge_id_seq"], 0);
  EXPECT_EQ(manifest_json["node_id_seq"], 0);
  EXPECT_EQ(manifest_json["shard_id_seq"], 0);
  EXPECT_TRUE(manifest_json["edges"].empty());
  EXPECT_TRUE(manifest_json["shards"].empty());

  // Create a new instance and verify counters are initialized correctly
  auto db2 = createDatabase();
  db2->initialize().ValueOrDie();

  // verifyCounters(db2, 0, 0, 0, {});
  // Note: We'd need to add accessor methods to actually check these values
}

// Test 2: Create schema, write users, create connections, snapshot, verify
// after reload
TEST_F(DatabaseTest, CreateSchemaAndData) {
  // Create a new database
  auto db = createDatabase();
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create 8 users
  auto users = createUsers(*db, 8);
  EXPECT_EQ(users.size(), 8);

  // Create some connections
  db->connect(1, "friend", 2).ValueOrDie();
  db->connect(2, "friend", 3).ValueOrDie();

  // Create a snapshot
  auto snapshot = db->create_snapshot().ValueOrDie();

  // Destroy and recreate database
  db.reset();  // Destroy the current instance

  // Create a new instance
  auto db2 = createDatabase();
  db2->initialize().ValueOrDie();

  // Setup the schema again since schemas aren't persisted
  setupUserSchema(*db2);

  // Verify schema exists
  EXPECT_TRUE(db2->get_schema_registry()->exists("users"));

  // Verify table data
  auto table = db2->get_table("users").ValueOrDie();
  EXPECT_EQ(table->num_rows(), 8);

  // Verify edge data
  EXPECT_EQ(db2->get_edge_store()->get_count_by_type("friend"), 2);

  // Verify counters
  // verifyCounters(db2, 8, 1, 2, {{"users", 1}});
}

// Test 3: Create two snapshots in a row without changes
TEST_F(DatabaseTest, CreateConsecutiveSnapshots) {
  // Create a new database
  auto db = createDatabase();
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create users
  auto users = createUsers(*db, 4);

  // Create connections
  db->connect(1, "friend", 2).ValueOrDie();

  // Create first snapshot
  auto snapshot1 = db->create_snapshot().ValueOrDie();

  // Record file paths
  std::string manifest_path1 =
      getManifestPath(test_db_path, snapshot1.manifest_location);
  auto manifest_json1 =
      read_json_file<nlohmann::json>(manifest_path1).ValueOrDie();

  // Create second snapshot immediately
  auto snapshot2 = db->create_snapshot().ValueOrDie();

  // Verify different manifest files
  std::string manifest_path2 =
      getManifestPath(test_db_path, snapshot2.manifest_location);
  EXPECT_NE(manifest_path1, manifest_path2);

  // Verify data file references haven't changed
  auto manifest_json2 =
      read_json_file<nlohmann::json>(manifest_path2).ValueOrDie();

  // Compare shard data files - they should be the same
  EXPECT_EQ(manifest_json1["shards"].size(), manifest_json2["shards"].size());
  if (manifest_json1["shards"].size() > 0 &&
      manifest_json2["shards"].size() > 0) {
    EXPECT_EQ(manifest_json1["shards"][0]["data_file"],
              manifest_json2["shards"][0]["data_file"]);
  }

  // Compare edge data files - they should be the same
  EXPECT_EQ(manifest_json1["edges"].size(), manifest_json2["edges"].size());
  if (manifest_json1["edges"].size() > 0 &&
      manifest_json2["edges"].size() > 0) {
    EXPECT_EQ(manifest_json1["edges"][0]["data_file"],
              manifest_json2["edges"][0]["data_file"]);
  }
}

// Test 4: Create DB, snapshot, create more data, snapshot, verify
TEST_F(DatabaseTest, IncrementalSnapshots) {
  // Create a new database
  auto db = createDatabase();
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create first batch of users
  auto users1 = createUsers(*db, 4);

  // Create connections
  db->connect(1, "friend", 2).ValueOrDie();

  // Create first snapshot
  auto snapshot1 = db->create_snapshot().ValueOrDie();

  // Add more users
  auto users2 = createUsers(*db, 3);

  // Create more connections
  db->connect(3, "friend", 4).ValueOrDie();

  // Create second snapshot
  auto snapshot2 = db->create_snapshot().ValueOrDie();

  // Destroy and recreate database
  db.reset();

  // Create a new instance
  auto db2 = createDatabase();
  db2->initialize().ValueOrDie();

  // Setup the schema again since schemas aren't persisted
  setupUserSchema(*db2);

  // Verify table data
  auto table = db2->get_table("users").ValueOrDie();
  EXPECT_EQ(table->num_rows(), 7);  // 4 + 3 users

  // Verify edge data
  EXPECT_EQ(db2->get_edge_store()->get_count_by_type("friend"), 2);

  // Verify counters
  // verifyCounters(db2, 7, 1, 2, {{"users", 1}});
}

// Test 5: Small shard capacity, verify multiple shards
TEST_F(DatabaseTest, MultipleShards) {
  // Create a database with small shard capacity
  auto db = createDatabase(2);  // Shard capacity of 2
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create 4 users - should create 2 shards with 2 users each
  auto users = createUsers(*db, 4);

  // Verify shard count
  auto shard_count = db->get_shard_count("users").ValueOrDie();
  EXPECT_EQ(shard_count, 2);

  // Create a snapshot
  auto snapshot = db->create_snapshot().ValueOrDie();

  // Verify manifest has 2 shards
  std::string manifest_path =
      getManifestPath(test_db_path, snapshot.manifest_location);
  auto manifest_json =
      read_json_file<nlohmann::json>(manifest_path).ValueOrDie();
  EXPECT_EQ(manifest_json["shards"].size(), 2);

  // Verify shard index values
  EXPECT_EQ(manifest_json["shards"][0]["index"], 0);
  EXPECT_EQ(manifest_json["shards"][1]["index"], 1);

  // Verify ID ranges
  EXPECT_EQ(manifest_json["shards"][0]["min_id"], 0);
  EXPECT_EQ(manifest_json["shards"][0]["max_id"], 1);
  EXPECT_EQ(manifest_json["shards"][1]["min_id"], 2);
  EXPECT_EQ(manifest_json["shards"][1]["max_id"], 3);
}

// Test 6: Node deletion and shard compaction
TEST_F(DatabaseTest, NodeDeletionAndCompaction) {
  // Create a database with small shard capacity
  auto db = createDatabase(2);  // Shard capacity of 2
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create 6 users - should create 3 shards with 2 users each
  auto users = createUsers(*db, 6);

  // Verify shard count
  auto shard_count = db->get_shard_count("users").ValueOrDie();
  EXPECT_EQ(shard_count, 3);

  // Check shard ranges before deletion
  auto initial_ranges = db->get_shard_ranges("users").ValueOrDie();
  EXPECT_EQ(initial_ranges.size(), 3);

  // Verify initial node distribution (IDs 0-5 distributed across shards)
  bool found_ids[6] = {false, false, false, false, false, false};
  for (const auto& range : initial_ranges) {
    for (int64_t id = range.first; id <= range.second && id < 6; id++) {
      if (id >= 0) found_ids[id] = true;
    }
  }

  for (int i = 0; i < 6; i++) {
    EXPECT_TRUE(found_ids[i])
        << "Node ID " << i << " was not found in any shard range";
  }
  // [0,1], [2,3], [4,5]
  // Delete users from the first shard
  db->remove_node("users", 0).ValueOrDie();
  db->remove_node("users", 1).ValueOrDie();

  // Delete one user from the second shard
  db->remove_node("users", 2).ValueOrDie();

  // Compact shards
  db->compact("users").ValueOrDie();

  // Verify shard count after compaction
  shard_count = db->get_shard_count("users").ValueOrDie();
  EXPECT_EQ(shard_count,
            2);  // Should have 2 shards with 3 users total (IDs 3, 4, 5)

  // expected
  // [3][4,5]
  auto shard0 = db->get_shard_manager()->get_shard("users", 0).ValueOrDie();
  auto shard1 = db->get_shard_manager()->get_shard("users", 1).ValueOrDie();

  std::cout << "shard-0: min=" << shard0->min_id << std::endl;
  std::cout << "shard-0: max=" << shard0->max_id << std::endl;

  std::cout << "shard-1: min=" << shard1->min_id << std::endl;
  std::cout << "shard-1: max=" << shard1->max_id << std::endl;

  // Check shard ranges after compaction
  auto compacted_ranges = db->get_shard_ranges("users").ValueOrDie();
  EXPECT_EQ(compacted_ranges.size(), 2);

  // Verify remaining nodes are properly distributed
  // First, check if node IDs 3, 4, and 5 are in the ranges
  bool found_after_compaction[6] = {false, false, false, false, false, false};
  for (const auto& range : compacted_ranges) {
    for (int64_t id = range.first; id <= range.second && id < 6; id++) {
      if (id >= 0) found_after_compaction[id] = true;
    }
  }

  // Nodes 0, 1, 2 should be removed
  EXPECT_FALSE(found_after_compaction[0]);
  EXPECT_FALSE(found_after_compaction[1]);
  EXPECT_FALSE(found_after_compaction[2]);

  // Nodes 3, 4, 5 should still exist
  EXPECT_TRUE(found_after_compaction[3]);
  EXPECT_TRUE(found_after_compaction[4]);
  EXPECT_TRUE(found_after_compaction[5]);

  // Create a snapshot
  auto snapshot = db->create_snapshot().ValueOrDie();

  // Verify manifest
  std::string manifest_path =
      getManifestPath(test_db_path, snapshot.manifest_location);
  auto manifest_json =
      read_json_file<nlohmann::json>(manifest_path).ValueOrDie();
  EXPECT_EQ(manifest_json["shards"].size(), 2);

  // Verify the remaining 3 nodes exist in the manifest shards
  std::unordered_set<int64_t> nodes_in_manifest;
  for (const auto& shard : manifest_json["shards"]) {
    int64_t min_id = shard["min_id"];
    int64_t max_id = shard["max_id"];
    for (int64_t id = min_id; id <= max_id; id++) {
      nodes_in_manifest.insert(id);
    }
  }

  EXPECT_TRUE(nodes_in_manifest.contains(3));
  EXPECT_TRUE(nodes_in_manifest.contains(4));
  EXPECT_TRUE(nodes_in_manifest.contains(5));
  EXPECT_FALSE(nodes_in_manifest.contains(0));
  EXPECT_FALSE(nodes_in_manifest.contains(1));
  EXPECT_FALSE(nodes_in_manifest.contains(2));

  // Load a new database from the snapshot
  db.reset();
  auto db2 = createDatabase(2);
  db2->initialize().ValueOrDie();

  // Setup the schema again since schemas aren't persisted
  setupUserSchema(*db2);

  // Verify shard count after loading
  auto loaded_shard_count = db2->get_shard_count("users").ValueOrDie();
  EXPECT_EQ(loaded_shard_count, 2);

  // Verify table data after loading
  auto table = db2->get_table("users").ValueOrDie();
  EXPECT_EQ(table->num_rows(), 3);  // Only 3 nodes remain

  // Verify that we have nodes with IDs 3, 4, and 5
  std::unordered_set<int64_t> loaded_node_ids;
  for (int i = 0; i < table->num_rows(); i++) {
    auto id_array =
        std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    loaded_node_ids.insert(id_array->Value(i));
  }

  EXPECT_TRUE(loaded_node_ids.contains(3));
  EXPECT_TRUE(loaded_node_ids.contains(4));
  EXPECT_TRUE(loaded_node_ids.contains(5));
  EXPECT_FALSE(loaded_node_ids.contains(0));
  EXPECT_FALSE(loaded_node_ids.contains(1));
  EXPECT_FALSE(loaded_node_ids.contains(2));
}

// Test 7: Verify updated flag after operations
TEST_F(DatabaseTest, VerifyUpdatedFlag) {
  // Create a database
  auto db = createDatabase();
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create users
  auto users = createUsers(*db, 2);

  // Get the shard index and id for later verification
  auto shard_ranges = db->get_shard_ranges("users").ValueOrDie();
  ASSERT_GE(shard_ranges.size(), 1);
  int64_t shard_id = 0;  // First shard ID

  // Get the shard_manager directly to check shard flags
  auto shard_manager = db->get_shard_manager();

  // Verify the shard is marked as updated after node creation
  auto is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_FALSE(is_clean)
      << "Shard should be marked as updated after node creation";

  // Create a snapshot (should reset updated flags)
  auto snapshot = db->create_snapshot().ValueOrDie();

  // Verify shard is now marked as not updated
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_TRUE(is_clean) << "Shard should be marked as clean after snapshot";

  // Update a node
  db->update_node("users", 0, "age", int64_t(30), SET).ValueOrDie();

  // Verify shard is marked as updated again
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_FALSE(is_clean)
      << "Shard should be marked as updated after node update";

  // Delete a node and verify updated flag
  db->remove_node("users", 1).ValueOrDie();
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_FALSE(is_clean)
      << "Shard should be marked as updated after node deletion";

  // Create another snapshot
  auto snapshot2 = db->create_snapshot().ValueOrDie();

  // Verify shard is clean again
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_TRUE(is_clean)
      << "Shard should be marked as clean after second snapshot";

  // Verify manifest files have different data_file references for the updated
  // shard
  std::string manifest_path1 =
      getManifestPath(test_db_path, snapshot.manifest_location);
  std::string manifest_path2 =
      getManifestPath(test_db_path, snapshot2.manifest_location);

  auto manifest_json1 =
      read_json_file<nlohmann::json>(manifest_path1).ValueOrDie();
  auto manifest_json2 =
      read_json_file<nlohmann::json>(manifest_path2).ValueOrDie();

  if (manifest_json1["shards"].size() > 0 &&
      manifest_json2["shards"].size() > 0) {
    EXPECT_NE(manifest_json1["shards"][0]["data_file"],
              manifest_json2["shards"][0]["data_file"]);
  }
}

TEST_F(DatabaseTest, DeleteNode) {
  // Create a database
  auto db = createDatabase();
  db->initialize().ValueOrDie();

  // Setup user schema
  setupUserSchema(*db);

  // Create users
  auto users = createUsers(*db, 2);

  // Get the shard index and id for later verification
  auto shard_ranges = db->get_shard_ranges("users").ValueOrDie();
  ASSERT_GE(shard_ranges.size(), 1);
  int64_t shard_id = 0;  // First shard ID

  // Get the shard_manager directly to check shard flags
  auto shard_manager = db->get_shard_manager();

  // Verify the shard is marked as updated after node creation
  auto is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_FALSE(is_clean)
      << "Shard should be marked as updated after node creation";

  // Create a snapshot (should reset updated flags)
  auto snapshot = db->create_snapshot().ValueOrDie();

  // Verify shard is now marked as not updated
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_TRUE(is_clean) << "Shard should be marked as clean after snapshot";

  // Delete a node and verify updated flag
  db->remove_node("users", 1).ValueOrDie();
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_FALSE(is_clean)
      << "Shard should be marked as updated after node deletion";

  // Create another snapshot
  auto snapshot2 = db->create_snapshot().ValueOrDie();

  // Verify shard is clean again
  is_clean = shard_manager->is_shard_clean("users", 0).ValueOrDie();
  EXPECT_TRUE(is_clean)
      << "Shard should be marked as clean after second snapshot";

  // Validate data file contents
  validateDataFile(getManifestPath(test_db_path, snapshot.manifest_location),
                   "users", 2, {0, 1});
  validateDataFile(getManifestPath(test_db_path, snapshot2.manifest_location),
                   "users", 1, {0});
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}