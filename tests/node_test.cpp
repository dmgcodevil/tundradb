#include "../include/node.hpp"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <unordered_map>

#include "../include/field_update.hpp"
#include "common/logger.hpp"
#include "../include/schema.hpp"
#include "common/types.hpp"

using namespace tundradb;

class NodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize logger for debugging
    // Logger::get_instance().set_level(LogLevel::DEBUG);

    // Create schema registry
    schema_registry_ = std::make_shared<SchemaRegistry>();

    // Create a test schema
    auto fields = std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("name", arrow::utf8(), false),
        arrow::field("age", arrow::int32(), true),
        arrow::field("email", arrow::utf8(), true),
        arrow::field("score", arrow::float64(), false)};
    test_schema_ = arrow::schema(fields);

    // Register the schema
    auto result = schema_registry_->create("User", test_schema_);
    ASSERT_TRUE(result.ok());

    // Register a schema with array fields for array tests
    auto array_fields = std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("name", arrow::utf8(), false),
        arrow::field("tags", arrow::list(arrow::field("item", arrow::utf8())),
                     true),
        arrow::field("scores",
                     arrow::list(arrow::field("item", arrow::int32())), true)};
    auto array_schema = arrow::schema(array_fields);
    ASSERT_TRUE(schema_registry_->create("UserWithArrays", array_schema).ok());

    // Create node manager
    node_manager_ = std::make_unique<NodeManager>(schema_registry_);
  }

  void TearDown() override {
    // Clean up
    node_manager_.reset();
    schema_registry_.reset();
  }

  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::shared_ptr<arrow::Schema> test_schema_;
  std::unique_ptr<NodeManager> node_manager_;
};

// Test NodeManager create_node functionality
TEST_F(NodeTest, NodeManagerCreateNode) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"John Doe"}},
      {"age", Value{static_cast<int32_t>(30)}},
      {"email", Value{"john@example.com"}},
      {"score", Value{85.5}}};

  // Create node using NodeManager
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok())
      << "Failed to create node: " << node_result.status().ToString();

  auto node = node_result.ValueOrDie();
  ASSERT_NE(node, nullptr);

  // Check node properties
  EXPECT_EQ(node->schema_name, "User");
  EXPECT_GE(node->id, 0);  // Auto-generated ID should be >= 0

  log_debug("Created node with ID: {}", node->id);
}

// Test NodeManager node retrieval
TEST_F(NodeTest, NodeManagerGetNode) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Jane Smith"}},
      {"age", Value{static_cast<int32_t>(25)}},
      {"email", Value{"jane@example.com"}},
      {"score", Value{92.0}}};

  // Create and add node
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());

  auto original_node = node_result.ValueOrDie();
  int64_t node_id = original_node->id;

  // Retrieve node
  auto retrieved_result = node_manager_->get_node("User", node_id);
  ASSERT_TRUE(retrieved_result.ok());

  auto retrieved_node = retrieved_result.ValueOrDie();
  ASSERT_NE(retrieved_node, nullptr);
  EXPECT_EQ(retrieved_node->id, node_id);
  EXPECT_EQ(retrieved_node->schema_name, "User");
}

// Test NodeManager remove_node functionality
TEST_F(NodeTest, NodeManagerRemoveNode) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Bob Johnson"}},
      {"age", Value{static_cast<int32_t>(35)}},
      {"score", Value{78.5}}};

  // Create node
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());

  auto node = node_result.ValueOrDie();
  int64_t node_id = node->id;

  // Verify node exists
  auto get_result = node_manager_->get_node("User", node_id);
  ASSERT_TRUE(get_result.ok());

  // Remove node
  bool removed = node_manager_->remove_node("User", node_id);
  EXPECT_TRUE(removed);

  // Verify node no longer exists
  auto get_result_after = node_manager_->get_node("User", node_id);
  // EXPECT_FALSE(get_result_after.ok()) << "Node should not exist after
  // removal";
}

// Test Node get_value functionality with arena backend
TEST_F(NodeTest, NodeGetValue) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Alice Cooper"}},
      {"age", Value{static_cast<int32_t>(28)}},
      {"email", Value{"alice@example.com"}},
      {"score", Value{88.7}}};

  // Create node
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());
  Logger::get_instance().debug("node created");

  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();
  Logger::get_instance().debug("Test getting values");
  // Test getting values
  auto name_result = node->get_value(schema->get_field("name"));
  ASSERT_TRUE(name_result.ok())
      << "Failed to get name: " << name_result.status().ToString();
  EXPECT_EQ(name_result.ValueOrDie().to_string(), "Alice Cooper");

  auto age_result = node->get_value(schema->get_field("age"));
  ASSERT_TRUE(age_result.ok())
      << "Failed to get age: " << age_result.status().ToString();
  EXPECT_EQ(age_result.ValueOrDie().as_int32(), 28);

  auto email_result = node->get_value(schema->get_field("email"));
  ASSERT_TRUE(email_result.ok())
      << "Failed to get email: " << email_result.status().ToString();
  EXPECT_EQ(email_result.ValueOrDie().to_string(), "alice@example.com");

  auto score_result = node->get_value(schema->get_field("score"));
  ASSERT_TRUE(score_result.ok())
      << "Failed to get score: " << score_result.status().ToString();
  EXPECT_DOUBLE_EQ(score_result.ValueOrDie().as_double(), 88.7);

  // Test auto-generated ID
  auto id_result = node->get_value(schema->get_field("id"));
  ASSERT_TRUE(id_result.ok())
      << "Failed to get id: " << id_result.status().ToString();
  EXPECT_EQ(id_result.ValueOrDie().as_int64(), node->id);

  log_debug("Successfully retrieved all field values from node");
}

// Test Node set_value functionality with arena backend
TEST_F(NodeTest, NodeSetValue) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Charlie Brown"}},
      {"age", Value{static_cast<int32_t>(22)}},
      {"score", Value{75.0}}};

  // Create node
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());

  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  Logger::get_instance().debug("Update values");

  // Update values
  auto set_name_result =
      node->set_value(schema->get_field("name"), Value{"Charlie Updated"});
  Logger::get_instance().debug("done");
  Logger::get_instance().debug("set_name_result: {}", set_name_result.ok());
  ASSERT_TRUE(set_name_result.ok())
      << "Failed to set name: " << set_name_result.status().ToString();

  auto name_result = node->get_value(schema->get_field("name"));
  ASSERT_TRUE(name_result.ok());
  EXPECT_EQ(name_result.ValueOrDie().to_string(), "Charlie Updated");

  auto set_age_result = node->set_value(schema->get_field("age"),
                                        Value{static_cast<int32_t>(23)});
  ASSERT_TRUE(set_age_result.ok())
      << "Failed to set age: " << set_age_result.status().ToString();

  auto age_result = node->get_value(schema->get_field("age"));
  ASSERT_TRUE(age_result.ok());
  EXPECT_EQ(age_result.ValueOrDie().as_int32(), 23);

  auto set_email_result = node->set_value(schema->get_field("email"),
                                          Value{"charlie.updated@example.com"});
  ASSERT_TRUE(set_email_result.ok())
      << "Failed to set email: " << set_email_result.status().ToString();

  auto email_result = node->get_value(schema->get_field("email"));
  ASSERT_TRUE(email_result.ok());
  EXPECT_EQ(email_result.ValueOrDie().to_string(),
            "charlie.updated@example.com");

  auto set_score_result =
      node->set_value(schema->get_field("score"), Value{82.5});
  ASSERT_TRUE(set_score_result.ok())
      << "Failed to set score: " << set_score_result.status().ToString();

  auto score_result = node->get_value(schema->get_field("score"));
  ASSERT_TRUE(score_result.ok());
  EXPECT_DOUBLE_EQ(score_result.ValueOrDie().as_double(), 82.5);

  log_debug("Successfully updated all field values in node");
}

// Test Node with nullable fields
TEST_F(NodeTest, NodeNullableFields) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"David Wilson"}}, {"score", Value{90.0}}
      // age and email are nullable and not provided
  };

  // Create node
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());

  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  // Test required fields
  auto name_result = node->get_value(schema->get_field("name"));
  ASSERT_TRUE(name_result.ok());
  EXPECT_EQ(name_result.ValueOrDie().to_string(), "David Wilson");

  auto score_result = node->get_value(schema->get_field("score"));
  ASSERT_TRUE(score_result.ok());
  EXPECT_DOUBLE_EQ(score_result.ValueOrDie().as_double(), 90.0);

  // Test nullable fields (should be null)
  auto age_result = node->get_value(schema->get_field("age"));
  ASSERT_TRUE(age_result.ok());

  Logger::get_instance().debug("age_result type: {}",
                               to_string(age_result.ValueOrDie().type()));

  EXPECT_TRUE(age_result.ValueOrDie().is_null())
      << "Age should be null when not provided";

  auto email_result = node->get_value(schema->get_field("email"));
  ASSERT_TRUE(email_result.ok());
  Logger::get_instance().debug("email_result type: {}",
                               to_string(email_result.ValueOrDie().type()));
  EXPECT_TRUE(email_result.ValueOrDie().is_null())
      << "Email should be null when not provided";

  log_debug("Successfully handled nullable fields");
}

// Test deprecated data() method for backward compatibility
// TEST_F(NodeTest, DeprecatedDataMethod) {
//   std::unordered_map<std::string, Value> node_data = {
//       {"name", Value{"Eva Martinez"}},
//       {"age", Value{static_cast<int32_t>(32)}},
//       {"email", Value{"eva@example.com"}},
//       {"score", Value{95.5}}};
//
//   // Create node
//   auto node_result =
//       node_manager_->create_node("User", node_data, schema_registry_);
//   ASSERT_TRUE(node_result.ok());
//
//   auto node = node_result.ValueOrDie();
//
//   // We need to set the schema_ field for the deprecated data() method to
//   work
//   // This is a bit of a hack since schema_ is private, but it's needed for
//   the
//   // deprecated method In a real scenario, the schema would be set during
//   node
//   // creation
//
//   // Test the deprecated data() method
//   // Note: This method reads from the arena, so it should work with our
//   // arena-based implementation
//   try {
//     auto data_map = node->data();
//
//     // Check that we get all the schema fields
//     EXPECT_TRUE(data_map.contains("id"));
//     EXPECT_TRUE(data_map.contains("name"));
//     EXPECT_TRUE(data_map.contains("age"));
//     EXPECT_TRUE(data_map.contains("email"));
//     EXPECT_TRUE(data_map.contains("score"));
//
//     // Verify values match what we expect
//     EXPECT_EQ(data_map.at("id").as_int64(), node->id);
//     EXPECT_EQ(data_map.at("name").to_string(), "Eva Martinez");
//     EXPECT_EQ(data_map.at("age").as_int32(), 32);
//     EXPECT_EQ(data_map.at("email").to_string(), "eva@example.com");
//     EXPECT_DOUBLE_EQ(data_map.at("score").as_double(), 95.5);
//
//     log_debug("Deprecated data() method returned {} fields",
//     data_map.size());
//
//   } catch (const std::exception& e) {
//     // The deprecated data() method might fail if schema_ is not properly set
//     // This is expected behavior since it's a deprecated method
//     log_debug("Deprecated data() method failed as expected: {}", e.what());
//     GTEST_SKIP() << "Deprecated data() method requires schema_ to be set, "
//                     "which is not done in current implementation";
//   }
// }

// Test error handling - invalid field name
TEST_F(NodeTest, DISABLED_ErrorHandlingInvalidField) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Frank Thompson"}}, {"score", Value{77.0}}};

  // Create node
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());

  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  // Try to get non-existent field
  auto invalid_result = node->get_value(schema->get_field("nonexistent_field"));
  EXPECT_FALSE(invalid_result.ok()) << "Should fail for non-existent field";

  // Try to set non-existent field
  auto invalid_set_result =
      node->set_value(schema->get_field("nonexistent_field"), Value{"test"});
  EXPECT_FALSE(invalid_set_result.ok())
      << "Should fail when setting non-existent field";

  log_debug("Error handling for invalid fields works correctly");
}

// Test NodeManager validation - required fields
TEST_F(NodeTest, DISABLED_NodeManagerValidationRequiredFields) {
  std::unordered_map<std::string, Value> incomplete_data = {
      {"age", Value{static_cast<int32_t>(25)}}
      // Missing required "name" and "score" fields
  };

  // Try to create node with missing required fields
  auto node_result = node_manager_->create_node("User", incomplete_data);
  EXPECT_FALSE(node_result.ok())
      << "Should fail when required fields are missing";

  log_debug("Validation correctly rejected node with missing required fields");
}

// Test NodeManager validation - type mismatch
TEST_F(NodeTest, DISABLED_NodeManagerValidationTypeMismatch) {
  std::unordered_map<std::string, Value> invalid_data = {
      {"name", Value{"Grace Lee"}},
      {"age", Value{"not_a_number"}},  // Should be int32, but providing string
      {"score", Value{85.0}}};

  // Try to create node with type mismatch
  auto node_result = node_manager_->create_node("User", invalid_data);
  EXPECT_FALSE(node_result.ok())
      << "Should fail when field types don't match schema";

  log_debug("Validation correctly rejected node with type mismatch");
}

// Test NodeManager validation - auto-generated ID
TEST_F(NodeTest, DISABLED_NodeManagerValidationAutoGeneratedId) {
  std::unordered_map<std::string, Value> data_with_id = {
      {"id", Value{static_cast<int64_t>(999)}},  // Should not be allowed
      {"name", Value{"Henry Davis"}},
      {"score", Value{80.0}}};

  // Try to create node with manually provided ID
  auto node_result = node_manager_->create_node("User", data_with_id);
  EXPECT_FALSE(node_result.ok()) << "Should fail when ID is manually provided";

  log_debug("Validation correctly rejected node with manual ID");
}

// Test multiple nodes and ID counter
TEST_F(NodeTest, MultipleNodesAndIdCounter) {
  // Set initial ID counter
  node_manager_->set_id_counter("User", 100);
  EXPECT_EQ(node_manager_->get_id_counter("User"), 100);

  std::unordered_map<std::string, Value> node_data1 = {{"name", Value{"User1"}},
                                                       {"score", Value{80.0}}};

  std::unordered_map<std::string, Value> node_data2 = {{"name", Value{"User2"}},
                                                       {"score", Value{85.0}}};

  // Create first node
  auto node1_result = node_manager_->create_node("User", node_data1);
  ASSERT_TRUE(node1_result.ok());
  auto node1 = node1_result.ValueOrDie();
  EXPECT_EQ(node1->id, 100);

  // Create second node
  auto node2_result = node_manager_->create_node("User", node_data2);
  ASSERT_TRUE(node2_result.ok());
  auto node2 = node2_result.ValueOrDie();
  EXPECT_EQ(node2->id, 101);

  // Verify ID counter advanced
  EXPECT_EQ(node_manager_->get_id_counter("User"), 102);

  // Verify both nodes exist and are different
  auto get1_result = node_manager_->get_node("User", 100);
  auto get2_result = node_manager_->get_node("User", 101);

  ASSERT_TRUE(get1_result.ok());
  ASSERT_TRUE(get2_result.ok());

  EXPECT_NE(get1_result.ValueOrDie(), get2_result.ValueOrDie());

  log_debug("Successfully created multiple nodes with sequential IDs");
}

// Performance test - create and access many nodes
TEST_F(NodeTest, PerformanceTest) {
  const int num_nodes = 1000;
  std::vector<int64_t> node_ids;
  node_ids.reserve(num_nodes);

  // Create many nodes
  auto start_time = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_nodes; ++i) {
    std::unordered_map<std::string, Value> node_data = {
        {"name", Value{std::string("User_") + std::to_string(i)}},
        {"age", Value{static_cast<int32_t>(20 + (i % 50))}},
        {"score", Value{static_cast<double>(50.0 + (i % 50))}}};

    auto node_result = node_manager_->create_node("User", node_data);
    ASSERT_TRUE(node_result.ok()) << "Failed to create node " << i;

    node_ids.push_back(node_result.ValueOrDie()->id);
  }

  auto create_time = std::chrono::high_resolution_clock::now();

  // Access all nodes and verify data
  for (int i = 0; i < num_nodes; ++i) {
    auto node_result = node_manager_->get_node("User", node_ids[i]);
    ASSERT_TRUE(node_result.ok()) << "Failed to get node " << i;

    auto node = node_result.ValueOrDie();
    auto name_result = node->get_value(node->get_schema()->get_field("name"));
    ASSERT_TRUE(name_result.ok());

    std::string expected_name = std::string("User_") + std::to_string(i);
    EXPECT_EQ(name_result.ValueOrDie().to_string(), expected_name);
  }

  auto end_time = std::chrono::high_resolution_clock::now();

  auto create_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      create_time - start_time);
  auto access_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - create_time);

  log_debug("Performance test - Created {} nodes in {}ms, accessed all in {}ms",
            num_nodes, create_duration.count(), access_duration.count());

  // Reasonable performance expectations (these may need adjustment based on
  // hardware)
  EXPECT_LT(create_duration.count(), 1000)
      << "Node creation should be reasonably fast";
  EXPECT_LT(access_duration.count(), 500)
      << "Node access should be reasonably fast";
}

// Test NodeArena marks strings for deletion when node field is updated
TEST_F(NodeTest, NodeArenaMarkForDeletion) {
  Logger::get_instance().debug(
      "=== Starting NodeArenaMarkForDeletion Test ===");

  // Create a node
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{std::string("Original Name")}},
      {"score", Value{static_cast<double>(85.5)}}};
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result).ValueOrDie();
  auto schema = node->get_schema();

  // Get original string ref count (should be > 0)
  auto value1_result = node->get_value(schema->get_field("name"));
  ASSERT_TRUE(value1_result.ok());
  auto value1 = value1_result.ValueOrDie();
  StringRef original_ref = value1.as_string_ref();
  int32_t original_ref_count = original_ref.get_ref_count();

  Logger::get_instance().debug("Original ref count: {}", original_ref_count);
  EXPECT_GT(original_ref_count, 0);
  EXPECT_FALSE(original_ref.is_marked_for_deletion());

  // Update the name field - this should mark the old string for deletion
  auto set_result = node->set_value(schema->get_field("name"),
                                    Value{std::string("Updated Name")});
  ASSERT_TRUE(set_result.ok());

  // The original StringRef should now be marked for deletion
  EXPECT_TRUE(original_ref.is_marked_for_deletion());

  // But still valid (ref count > 0 because we hold a copy)
  EXPECT_EQ(original_ref.view(), "Original Name");

  // Get the new value
  auto value2_result = node->get_value(schema->get_field("name"));
  ASSERT_TRUE(value2_result.ok());
  EXPECT_EQ(value2_result.ValueOrDie().to_string(), "Updated Name");

  Logger::get_instance().debug(
      "=== NodeArenaMarkForDeletion Test Complete ===");
}

// Test that NodeArena properly stores and retrieves strings
TEST_F(NodeTest, NodeArenaStringStorage) {
  Logger::get_instance().debug("=== Starting NodeArenaStringStorage Test ===");

  const std::string test_string = "Test String Value";

  // Create a node
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{test_string}},
      {"score", Value{static_cast<double>(88.0)}}};
  auto node_result = node_manager_->create_node("User", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = std::move(node_result).ValueOrDie();
  auto schema = node->get_schema();

  // Get the string value back
  auto value_result = node->get_value(schema->get_field("name"));
  ASSERT_TRUE(value_result.ok());
  auto value = value_result.ValueOrDie();

  // Verify the string content is correct
  EXPECT_EQ(value.to_string(), test_string);

  // Verify it's stored as StringRef
  EXPECT_TRUE(value.holds_string_ref());

  StringRef ref = value.as_string_ref();
  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.view(), test_string);
  EXPECT_GT(ref.get_ref_count(), 0);

  Logger::get_instance().debug("=== NodeArenaStringStorage Test Complete ===");
}

// Test that multiple nodes can safely access the same string value
TEST_F(NodeTest, MultipleNodesSharedString) {
  Logger::get_instance().debug(
      "=== Starting MultipleNodesSharedString Test ===");

  const std::string shared_string = "Shared Value";
  std::vector<std::shared_ptr<Node>> nodes;

  // Create 5 nodes with the same string value
  for (int i = 0; i < 5; i++) {
    std::unordered_map<std::string, Value> node_data = {
        {"name", Value{shared_string}},
        {"score", Value{static_cast<double>(80.0 + i)}}};
    auto node_result = node_manager_->create_node("User", node_data);
    ASSERT_TRUE(node_result.ok());
    nodes.push_back(std::move(node_result).ValueOrDie());
  }

  auto schema = nodes[0]->get_schema();

  // Verify all nodes can read the string
  for (int i = 0; i < 5; i++) {
    auto value_result = nodes[i]->get_value(schema->get_field("name"));
    ASSERT_TRUE(value_result.ok());
    EXPECT_EQ(value_result.ValueOrDie().to_string(), shared_string);
  }

  Logger::get_instance().debug("=== Destroying nodes one by one ===");

  // Destroy nodes one by one - remaining nodes should still work
  for (int i = 0; i < 4; i++) {
    Logger::get_instance().debug("Destroying node {}", i);
    nodes[i].reset();

    // Remaining nodes should still be able to access the string
    for (int j = i + 1; j < 5; j++) {
      auto value_result = nodes[j]->get_value(schema->get_field("name"));
      ASSERT_TRUE(value_result.ok());
      EXPECT_EQ(value_result.ValueOrDie().to_string(), shared_string);
    }
  }

  Logger::get_instance().debug("=== Destroying final node ===");
  nodes[4].reset();

  Logger::get_instance().debug(
      "=== MultipleNodesSharedString Test Complete ===");
}

// ---------------------------------------------------------------------------
// Array field tests (schema: UserWithArrays with name, tags[], scores[])
// ---------------------------------------------------------------------------

// Test creating a node with array fields (list of strings, list of int32)
TEST_F(NodeTest, NodeManagerCreateNodeWithArray) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Alice"}},
      {"tags", Value{std::vector<Value>{Value{"a"}, Value{"b"}, Value{"c"}}}},
      {"scores", Value{std::vector<Value>{Value{static_cast<int32_t>(10)},
                                          Value{static_cast<int32_t>(20)},
                                          Value{static_cast<int32_t>(30)}}}}};

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok())
      << "Failed to create node: " << node_result.status().ToString();

  auto node = node_result.ValueOrDie();
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->schema_name, "UserWithArrays");
  EXPECT_GE(node->id, 0);
}

// Test get_value for array field: storage returns arena-backed ArrayRef
TEST_F(NodeTest, NodeGetValueArray) {
  std::vector<Value> tags = {Value{"x"}, Value{"y"}, Value{"z"}};
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Bob"}},
      {"tags", Value{tags}},
  };

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  auto tags_result = node->get_value(schema->get_field("tags"));
  ASSERT_TRUE(tags_result.ok())
      << "Failed to get tags: " << tags_result.status().ToString();
  const Value& tags_value = tags_result.ValueOrDie();

  EXPECT_EQ(tags_value.type(), ValueType::ARRAY);
  EXPECT_TRUE(tags_value.holds_array_ref())
      << "Arena should store array as ArrayRef";
  const ArrayRef& arr = tags_value.as_array_ref();
  EXPECT_FALSE(arr.is_null());
  EXPECT_EQ(arr.length(), 3u);

  // to_string() formats as "[x, y, z]"
  std::string str = tags_value.to_string();
  EXPECT_EQ(str, "[x, y, z]");
}

// Test get_value for int32 array field
TEST_F(NodeTest, NodeGetValueInt32Array) {
  std::vector<Value> scores = {
      Value{static_cast<int32_t>(1)},
      Value{static_cast<int32_t>(2)},
      Value{static_cast<int32_t>(3)},
  };
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Carol"}},
      {"scores", Value{scores}},
  };

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  auto scores_result = node->get_value(schema->get_field("scores"));
  ASSERT_TRUE(scores_result.ok());
  const Value& scores_value = scores_result.ValueOrDie();

  EXPECT_EQ(scores_value.type(), ValueType::ARRAY);
  EXPECT_TRUE(scores_value.holds_array_ref());
  const ArrayRef& arr = scores_value.as_array_ref();
  EXPECT_EQ(arr.length(), 3u);
  EXPECT_EQ(scores_value.to_string(), "[1, 2, 3]");
}

// Test set_value for array field (replace entire array)
TEST_F(NodeTest, NodeSetValueArray) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Dave"}},
      {"tags", Value{std::vector<Value>{Value{"old1"}, Value{"old2"}}}},
  };

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  auto get_before = node->get_value(schema->get_field("tags"));
  ASSERT_TRUE(get_before.ok());
  EXPECT_EQ(get_before.ValueOrDie().to_string(), "[old1, old2]");

  std::vector<Value> new_tags = {Value{"new1"}, Value{"new2"}, Value{"new3"}};
  auto set_result = node->set_value(schema->get_field("tags"), Value{new_tags});
  ASSERT_TRUE(set_result.ok())
      << "Failed to set tags: " << set_result.status().ToString();

  auto get_after = node->get_value(schema->get_field("tags"));
  ASSERT_TRUE(get_after.ok());
  EXPECT_EQ(get_after.ValueOrDie().to_string(), "[new1, new2, new3]");
}

// Test node with empty array
TEST_F(NodeTest, NodeArrayEmpty) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Eve"}},
      {"tags", Value{std::vector<Value>{}}},
  };

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  auto tags_result = node->get_value(schema->get_field("tags"));
  ASSERT_TRUE(tags_result.ok());
  const Value& tags_value = tags_result.ValueOrDie();

  EXPECT_EQ(tags_value.type(), ValueType::ARRAY);
  EXPECT_TRUE(tags_value.holds_array_ref());
  EXPECT_TRUE(tags_value.as_array_ref().empty());
  EXPECT_EQ(tags_value.as_array_ref().length(), 0u);
  EXPECT_EQ(tags_value.to_string(), "[]");
}

// Test nullable array field omitted (null)
TEST_F(NodeTest, NodeArrayNullableOmitted) {
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Frank"}},
      // tags and scores not provided -> null
  };

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  auto tags_result = node->get_value(schema->get_field("tags"));
  ASSERT_TRUE(tags_result.ok());
  EXPECT_TRUE(tags_result.ValueOrDie().is_null());

  auto scores_result = node->get_value(schema->get_field("scores"));
  ASSERT_TRUE(scores_result.ok());
  EXPECT_TRUE(scores_result.ValueOrDie().is_null());
}

// Test array storage: verify ArrayRef ref count and element access
TEST_F(NodeTest, NodeArrayStorage) {
  std::vector<Value> tags = {Value{"one"}, Value{"two"}};
  std::unordered_map<std::string, Value> node_data = {
      {"name", Value{"Grace"}},
      {"tags", Value{tags}},
  };

  auto node_result = node_manager_->create_node("UserWithArrays", node_data);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto schema = node->get_schema();

  auto tags_result = node->get_value(schema->get_field("tags"));
  ASSERT_TRUE(tags_result.ok());
  const Value& v = tags_result.ValueOrDie();

  EXPECT_TRUE(v.holds_array_ref());
  ArrayRef ref = v.as_array_ref();
  EXPECT_GT(ref.get_ref_count(), 0);
  EXPECT_FALSE(ref.is_marked_for_deletion());
  EXPECT_EQ(ref.length(), 2u);
  EXPECT_EQ(ref.elem_type(), ValueType::STRING);

  // Read elements via Value::read_value_from_memory
  auto elem0 =
      Value::read_value_from_memory(ref.element_ptr(0), ref.elem_type());
  auto elem1 =
      Value::read_value_from_memory(ref.element_ptr(1), ref.elem_type());
  EXPECT_EQ(elem0.to_string(), "one");
  EXPECT_EQ(elem1.to_string(), "two");
}

// ---------------------------------------------------------------------------
// MAP field tests (schema: UserWithMap  —  name, age, props: MAP)
// ---------------------------------------------------------------------------

class NodeMapFieldTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = std::make_shared<SchemaRegistry>();

    auto fields = std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("name", arrow::utf8(), false),
        arrow::field("age", arrow::int32(), true),
        arrow::field("props", arrow::map(arrow::utf8(), arrow::binary()),
                     true)};
    ASSERT_TRUE(registry_->create("UserWithMap", arrow::schema(fields)).ok());

    mgr_ = std::make_unique<NodeManager>(registry_);
  }

  std::shared_ptr<SchemaRegistry> registry_;
  std::unique_ptr<NodeManager> mgr_;
};

TEST_F(NodeMapFieldTest, MapFieldIsNullWhenOmitted) {
  auto node =
      mgr_->create_node("UserWithMap", {{"name", Value{"Alice"}}}).ValueOrDie();
  auto schema = node->get_schema();

  auto val = node->get_value(schema->get_field("props"));
  ASSERT_TRUE(val.ok());
  EXPECT_TRUE(val.ValueOrDie().is_null());
}

TEST_F(NodeMapFieldTest, SetMapKeyViUpdateFields) {
  auto node =
      mgr_->create_node("UserWithMap", {{"name", Value{"Bob"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(42)}, UpdateType::SET,
                                   std::vector<std::string>{"answer"}}})
                  .ok());

  auto val = node->get_value(schema->get_field("props"));
  ASSERT_TRUE(val.ok());
  Value v = val.ValueOrDie();
  ASSERT_TRUE(v.holds_map_ref());
  EXPECT_EQ(v.as_map_ref().get_value("answer").as_int32(), 42);
}

TEST_F(NodeMapFieldTest, SetMultipleMapKeys) {
  auto node = mgr_->create_node("UserWithMap", {{"name", Value{"Carol"}},
                                                {"age", Value{int32_t(30)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{3.14}, UpdateType::SET,
                                       std::vector<std::string>{"score"}}})
          .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{true}, UpdateType::SET,
                                       std::vector<std::string>{"active"}}})
          .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{"admin"}, UpdateType::SET,
                                       std::vector<std::string>{"role"}}})
          .ok());

  auto m =
      node->get_value(schema->get_field("props")).ValueOrDie().as_map_ref();
  EXPECT_EQ(m.count(), 3u);
  EXPECT_DOUBLE_EQ(m.get_value("score").as_double(), 3.14);
  EXPECT_EQ(m.get_value("active").as_bool(), true);
  EXPECT_EQ(m.get_value("role").as_string(), "admin");
}

TEST_F(NodeMapFieldTest, OverwriteMapKey) {
  auto node =
      mgr_->create_node("UserWithMap", {{"name", Value{"Dave"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(1)}, UpdateType::SET,
                                   std::vector<std::string>{"x"}}})
                  .ok());
  EXPECT_EQ(node->get_value(schema->get_field("props"))
                .ValueOrDie()
                .as_map_ref()
                .get_value("x")
                .as_int32(),
            1);

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(99)}, UpdateType::SET,
                                   std::vector<std::string>{"x"}}})
                  .ok());
  EXPECT_EQ(node->get_value(schema->get_field("props"))
                .ValueOrDie()
                .as_map_ref()
                .get_value("x")
                .as_int32(),
            99);
}

TEST_F(NodeMapFieldTest, MixedScalarAndMapKeyUpdates) {
  auto node = mgr_->create_node("UserWithMap", {{"name", Value{"Frank"}},
                                                {"age", Value{int32_t(20)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age = schema->get_field("age");
  auto props = schema->get_field("props");

  // Update both a scalar field and a map key in one batch
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{age, Value{int32_t(21)}, UpdateType::SET, {}},
               FieldUpdate{props, Value{100.0}, UpdateType::SET,
                           std::vector<std::string>{"score"}}})
          .ok());

  EXPECT_EQ(node->get_value(schema->get_field("age")).ValueOrDie().as_int32(),
            21);
  EXPECT_DOUBLE_EQ(node->get_value(schema->get_field("props"))
                       .ValueOrDie()
                       .as_map_ref()
                       .get_value("score")
                       .as_double(),
                   100.0);
}

TEST_F(NodeMapFieldTest, GetValueMissingMapKeyReturnsNull) {
  auto node =
      mgr_->create_node("UserWithMap", {{"name", Value{"Grace"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(1)}, UpdateType::SET,
                                   std::vector<std::string>{"a"}}})
                  .ok());

  auto m =
      node->get_value(schema->get_field("props")).ValueOrDie().as_map_ref();
  EXPECT_TRUE(m.get_value("nonexistent").is_null());
}