#include <gtest/gtest.h>

#include "../include/core.hpp"

using namespace tundradb;

class ShardingTest : public ::testing::Test {
 protected:
  // Small shard size for testing
  const size_t SHARD_SIZE = 2;
  const size_t CHUNK_SIZE = 2;

  // Set up the test
  void SetUp() override {
    // Initialize database with small shard size
    db = std::make_unique<Database>(SHARD_SIZE, CHUNK_SIZE);

    // Create a test schema
    auto name_field = arrow::field("name", arrow::utf8());
    auto count_field = arrow::field("count", arrow::int64());
    auto schema = arrow::schema({name_field, count_field});

    auto result = db->register_schema("test-schema", schema);
    ASSERT_TRUE(result.ok())
        << "Failed to register schema: " << result.status().ToString();
  }

  // Create a test node with specified name and count
  std::shared_ptr<Node> create_test_node(const std::string& name,
                                         int64_t count) {
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> fields = {
        {"name", create_str_array(name).ValueOrDie()},
        {"count", create_int64_array(count).ValueOrDie()}};

    auto result = db->create_node("test-schema", fields);
    EXPECT_TRUE(result.ok())
        << "Failed to create node: " << result.status().ToString();
    return result.ValueOrDie();
  }

  std::unique_ptr<Database> db;
};

// Test adding and retrieving nodes
TEST_F(ShardingTest, AddAndRetrieveNodes) {
  // Create 5 nodes
  std::vector<std::shared_ptr<Node>> nodes;
  for (int i = 0; i < 5; i++) {
    auto node = create_test_node("Node" + std::to_string(i), i);
    nodes.push_back(node);
  }

  // Get the table
  auto table_result = db->get_table("test-schema");
  ASSERT_TRUE(table_result.ok())
      << "Failed to get table: " << table_result.status().ToString();
  auto table = table_result.ValueOrDie();

  // Verify table has correct number of rows
  EXPECT_EQ(table->num_rows(), 5);

  // Verify the first node's ID is 0 (first node created)
  auto id_array =
      std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  EXPECT_EQ(id_array->Value(0), 0);

  // Verify the last node's ID is 4 (last node created)
  EXPECT_EQ(id_array->Value(4), 4);

  // Verify the name of the first node
  auto name_array =
      std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
  EXPECT_EQ(name_array->GetString(0), "Node0");

  // Verify the name of the last node
  EXPECT_EQ(name_array->GetString(4), "Node4");
}

// Test updating nodes
TEST_F(ShardingTest, UpdateNodes) {
  // Create a node
  auto node = create_test_node("Original", 0);

  // Update the node
  auto update =
      std::make_shared<SetOperation>(node->id, std::vector<std::string>{"name"},
                                     create_str_array("Updated").ValueOrDie());

  auto update_result = db->update_node(update);
  ASSERT_TRUE(update_result.ok())
      << "Failed to update node: " << update_result.status().ToString();

  // Get the table
  auto table_result = db->get_table("test-schema");
  ASSERT_TRUE(table_result.ok());
  auto table = table_result.ValueOrDie();

  // Verify the update was applied
  auto name_array =
      std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
  EXPECT_EQ(name_array->GetString(0), "Updated");
}

// Test compaction
TEST_F(ShardingTest, Compaction) {
  // Create nodes to fill multiple shards (with our small shard size)
  for (int i = 0; i < 6; i++) {
    create_test_node("Node" + std::to_string(i), i);
  }

  // Now remove some nodes to create gaps
  auto update =
      std::make_shared<SetOperation>(1,  // Remove node with ID 1
                                     std::vector<std::string>{"name"},
                                     create_str_array("Removed").ValueOrDie());

  auto update_result = db->update_node(update);
  ASSERT_TRUE(update_result.ok());

  // Compact the schema
  auto compact_result = db->compact("test-schema");
  ASSERT_TRUE(compact_result.ok())
      << "Failed to compact: " << compact_result.status().ToString();

  // Get the table and verify all nodes are still present
  auto table_result = db->get_table("test-schema");
  ASSERT_TRUE(table_result.ok());
  auto table = table_result.ValueOrDie();

  // We should still have 6 nodes
  EXPECT_EQ(table->num_rows(), 6);

  // Run compact_all and ensure it works
  auto compact_all_result = db->compact_all();
  ASSERT_TRUE(compact_all_result.ok())
      << "Failed to compact all schemas: "
      << compact_all_result.status().ToString();
}

// Test ordering of nodes in the table
TEST_F(ShardingTest, NodeOrdering) {
  // Create nodes in reverse order to test sorting
  for (int i = 10; i >= 0; i--) {
    create_test_node("Node" + std::to_string(i), i);
  }

  // Get the table
  auto table_result = db->get_table("test-schema");
  ASSERT_TRUE(table_result.ok());
  auto table = table_result.ValueOrDie();

  // Verify nodes are ordered by ID, regardless of insertion order
  auto id_array =
      std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));

  // The first row should have the smallest ID
  EXPECT_EQ(id_array->Value(0), 0);

  // IDs should be in ascending order
  for (int i = 1; i < table->num_rows(); i++) {
    EXPECT_LT(id_array->Value(i - 1), id_array->Value(i));
  }

  // The last row should have the largest ID
  EXPECT_EQ(id_array->Value(table->num_rows() - 1), 10);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}