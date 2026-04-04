#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "../include/arrow_map_union_types.hpp"
#include "../include/core.hpp"
#include "../include/field_update.hpp"
#include "common/logger.hpp"

using namespace std::string_literals;
using namespace tundradb;

// Helper function to generate a unique test directory name
std::string get_test_dir_name() {
  return "./testdb_" +
         std::to_string(
             std::chrono::system_clock::now().time_since_epoch().count());
}

// Helper function to create a user node with given name and age
std::shared_ptr<Node> create_user_node(Database& db, const std::string& name,
                                       int64_t age) {
  std::unordered_map<std::string, Value> fields = {{"name", Value{name}},
                                                   {"age", Value{age}}};

  return db.create_node("users", fields).ValueOrDie();
}

// Helper to create a schema for users
void create_user_schema(Database& db) {
  auto name_field = arrow::field("name", arrow::utf8());
  auto age_field = arrow::field("age", arrow::int64());
  auto schema = arrow::schema({name_field, age_field});

  db.get_schema_registry()->create("users", schema).ValueOrDie();
}

void create_user_map_schema(Database& db) {
  auto map_value_type = map_union_value_type();
  auto name_field = arrow::field("name", arrow::utf8());
  auto props_field =
      arrow::field("props", arrow::map(arrow::utf8(), map_value_type));
  auto schema = arrow::schema({name_field, props_field});
  db.get_schema_registry()->create("users_map", schema).ValueOrDie();
}

void create_user_array_schema(Database& db) {
  auto name_field = arrow::field("name", arrow::utf8());
  auto tags_field =
      arrow::field("tags", arrow::list(arrow::field("item", arrow::utf8())));
  auto schema = arrow::schema({name_field, tags_field});
  db.get_schema_registry()->create("users_array", schema).ValueOrDie();
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
    // std::filesystem::remove_all(test_dir);
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
  void create_test_users(Database& db, int from, int until) {
    for (int i = from; i < until; ++i) {
      create_user_node(db, "User " + std::to_string(i), 20 + i);
    }
  }

  // Helper to verify data in the table
  void verify_user_table(const std::shared_ptr<arrow::Table>& table,
                         int expected_count) {
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(table->num_rows(), expected_count);
    ASSERT_EQ(table->num_columns(), 3);  // id, name, age

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
  ASSERT_TRUE(snapshot.id != 0);
  std::string manifest_location = snapshot.manifest_location;

  // Step 4: Create a new DB instance (simulating restart)
  db.reset();  // Destroy first instance
  auto new_db = create_test_database();
  create_user_schema(*new_db);
  ASSERT_TRUE(new_db->initialize().ValueOrDie());

  // Step 5: Get table and verify data
  auto table = new_db->get_table("users").ValueOrDie();
  verify_user_table(table, 8);
}

// Test Scenario 2: Create DB, add nodes, snapshot, create another snapshot
// without changes, recreate, verify
TEST_F(DatabaseSnapshotTest, SnapshotWithoutChanges) {
  // Step 1: Create and initialize DB
  auto db = create_test_database();
  ASSERT_TRUE(db->initialize().ValueOrDie());

  // Step 2: Create schema and add users
  create_user_schema(*db);
  create_test_users(*db, 0, 8);

  // Step 3: Create first snapshot
  auto snapshot1 = db->create_snapshot().ValueOrDie();
  ASSERT_TRUE(snapshot1.id != 0);
  std::string first_manifest_location = snapshot1.manifest_location;
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // Step 4: Create second snapshot without changes
  auto snapshot2 = db->create_snapshot().ValueOrDie();
  ASSERT_TRUE(snapshot2.id != 0);
  std::string second_manifest_location = snapshot2.manifest_location;

  // Different snapshot IDs and files
  ASSERT_NE(snapshot1.id, snapshot2.id);
  ASSERT_NE(first_manifest_location, second_manifest_location);

  // Get manifest content to compare
  auto manifest1 =
      read_json_file<Manifest>(first_manifest_location).ValueOrDie();
  auto manifest2 =
      read_json_file<Manifest>(second_manifest_location).ValueOrDie();

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
        ASSERT_EQ(shard1.data_file,
                  shard2.data_file);  // Same data file reference
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

// Test Scenario 3: Create DB, add nodes, snapshot, add more nodes, snapshot
// again, recreate, verify
TEST_F(DatabaseSnapshotTest, SnapshotWithAdditionalData) {
  // Step 1: Create and initialize DB
  auto db = create_test_database();
  ASSERT_TRUE(db->initialize().ValueOrDie());

  // Step 2: Create schema and add initial users
  create_user_schema(*db);
  create_test_users(*db, 0, 4);

  // Step 3: Create first snapshot
  auto snapshot1 = db->create_snapshot().ValueOrDie();
  ASSERT_TRUE(snapshot1.id != 0);
  std::string first_manifest_location = snapshot1.manifest_location;

  // Step 4: Add more users
  create_test_users(*db, 4, 8);

  // Step 5: Create second snapshot
  auto snapshot2 = db->create_snapshot().ValueOrDie();
  ASSERT_TRUE(snapshot2.id != 0);
  std::string second_manifest_location = snapshot2.manifest_location;

  // Different snapshot IDs
  ASSERT_NE(snapshot1.id, snapshot2.id);

  // Step 6: Create a new DB instance (simulating restart)
  db.reset();  // Destroy first instance
  auto new_db = create_test_database();
  create_user_schema(*new_db);
  ASSERT_TRUE(new_db->initialize().ValueOrDie());

  // Step 7: Get table and verify data - should have all 8 users
  auto table = new_db->get_table("users").ValueOrDie();
  verify_user_table(table, 8);
}

TEST_F(DatabaseSnapshotTest, SnapshotReloadPreservesMapValues) {
  auto db = create_test_database();
  ASSERT_TRUE(db->initialize().ValueOrDie());
  create_user_map_schema(*db);

  auto node =
      db->create_node("users_map", {{"name", Value{"Alice"}}}).ValueOrDie();
  auto props = db->get_schema_registry()
                   ->get("users_map")
                   .ValueOrDie()
                   ->get_field("props");
  ASSERT_NE(props, nullptr);
  auto update_res = node->update_fields(
      {FieldUpdate{props, Value{int32_t(42)}, UpdateType::SET, {"score"}},
       FieldUpdate{props, Value{"admin"}, UpdateType::SET, {"role"}}});
  ASSERT_TRUE(update_res.ok()) << update_res.status().ToString();

  auto snapshot = db->create_snapshot().ValueOrDie();
  ASSERT_TRUE(snapshot.id != 0);

  db.reset();
  auto new_db = create_test_database();
  create_user_map_schema(*new_db);
  ASSERT_TRUE(new_db->initialize().ValueOrDie());

  auto restored = new_db->get_node_manager()->get_node("users_map", 0);
  ASSERT_TRUE(restored.ok()) << restored.status().ToString();
  auto restored_node = restored.ValueOrDie();
  auto props_value =
      restored_node->get_value(restored_node->get_schema()->get_field("props"));
  ASSERT_TRUE(props_value.ok()) << props_value.status().ToString();
  ASSERT_EQ(props_value.ValueOrDie().type(), ValueType::MAP);

  const auto map = props_value.ValueOrDie().as_map_ref();
  EXPECT_EQ(map.get_value("score").as_int32(), 42);
  EXPECT_EQ(map.get_value("role").as_string(), "admin");
}

TEST_F(DatabaseSnapshotTest, SnapshotReloadPreservesArrayValues) {
  auto db = create_test_database();
  ASSERT_TRUE(db->initialize().ValueOrDie());
  create_user_array_schema(*db);

  auto node =
      db->create_node("users_array", {{"name", Value{"Alice"}}}).ValueOrDie();
  auto tags = db->get_schema_registry()
                  ->get("users_array")
                  .ValueOrDie()
                  ->get_field("tags");
  ASSERT_NE(tags, nullptr);
  auto update_res = node->update(
      tags, Value(std::vector<Value>{Value{"engineer"}, Value{"graph"}}));
  ASSERT_TRUE(update_res.ok()) << update_res.status().ToString();

  auto snapshot = db->create_snapshot().ValueOrDie();
  ASSERT_TRUE(snapshot.id != 0);

  db.reset();
  auto new_db = create_test_database();
  create_user_array_schema(*new_db);
  ASSERT_TRUE(new_db->initialize().ValueOrDie());

  auto restored = new_db->get_node_manager()->get_node("users_array", 0);
  ASSERT_TRUE(restored.ok()) << restored.status().ToString();
  auto restored_node = restored.ValueOrDie();
  auto tags_value =
      restored_node->get_value(restored_node->get_schema()->get_field("tags"));
  ASSERT_TRUE(tags_value.ok()) << tags_value.status().ToString();
  ASSERT_EQ(tags_value.ValueOrDie().type(), ValueType::ARRAY);
  ASSERT_TRUE(tags_value.ValueOrDie().holds_array_ref());

  const auto arr = tags_value.ValueOrDie().as_array_ref();
  ASSERT_EQ(arr.length(), 2u);
  EXPECT_EQ(Value::read_value_from_memory(arr.element_ptr(0), arr.elem_type())
                .as_string(),
            "engineer");
  EXPECT_EQ(Value::read_value_from_memory(arr.element_ptr(1), arr.elem_type())
                .as_string(),
            "graph");
}

TEST_F(DatabaseSnapshotTest, SnapshotReloadPreservesEdgeSchemaProperties) {
  auto db = create_test_database();
  ASSERT_TRUE(db->initialize().ValueOrDie());

  db->get_schema_registry()
      ->create("User", arrow::schema({arrow::field("name", arrow::utf8())}))
      .ValueOrDie();
  db->get_schema_registry()
      ->create("Company", arrow::schema({arrow::field("name", arrow::utf8())}))
      .ValueOrDie();
  ASSERT_TRUE(
      db->register_edge_schema(
            "WORKS_AT", {std::make_shared<Field>("since", ValueType::INT64),
                         std::make_shared<Field>("role", ValueType::STRING)})
          .ok());

  db->create_node("User", {{"name", Value{"Alice"}}}).ValueOrDie();
  db->create_node("Company", {{"name", Value{"Acme"}}}).ValueOrDie();
  ASSERT_TRUE(db->connect(0, "WORKS_AT", 0,
                          {{"since", Value{int64_t(2025)}},
                           {"role", Value{"engineer"}}})
                  .ok());
  ASSERT_TRUE(db->create_snapshot().ok());

  db.reset();
  auto new_db = create_test_database();
  ASSERT_TRUE(new_db->initialize().ValueOrDie());

  auto q = Query::from("u:User")
               .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner, "e")
               .select({"u.name", "e.since", "e.role"})
               .build();
  auto qr = new_db->query(q);
  ASSERT_TRUE(qr.ok()) << qr.status().ToString();
  auto table = qr.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  auto since_col = table->GetColumnByName("e.since");
  auto role_col = table->GetColumnByName("e.role");
  ASSERT_NE(since_col, nullptr);
  ASSERT_NE(role_col, nullptr);

  auto since_arr =
      std::static_pointer_cast<arrow::Int64Array>(since_col->chunk(0));
  auto role_arr =
      std::static_pointer_cast<arrow::StringArray>(role_col->chunk(0));
  ASSERT_FALSE(since_arr->IsNull(0));
  ASSERT_FALSE(role_arr->IsNull(0));
  EXPECT_EQ(since_arr->Value(0), 2025);
  EXPECT_EQ(role_arr->GetString(0), "engineer");
}

int main(int argc, char** argv) {
  // Initialize logger
  tundradb::Logger::get_instance().set_level(tundradb::LogLevel::INFO);

  // Optional: set log to file for test output
  // tundradb::Logger::getInstance().setLogToFile("snapshot_test.log");

  tundradb::log_info("Starting snapshot tests");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}