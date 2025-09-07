#include "../include/node_arena.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "../include/schema_layout.hpp"
#include "../include/string_arena.hpp"
#include "../include/types.hpp"

using namespace tundradb;

class NodeArenaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = std::make_unique<LayoutRegistry>();

    // Create a comprehensive test schema with all ValueTypes
    auto layout = std::make_unique<SchemaLayout>("TestNode");
    layout->add_field("id", ValueType::INT64);
    layout->add_field("count", ValueType::INT32);
    layout->add_field("score", ValueType::DOUBLE);
    layout->add_field("active", ValueType::BOOL);
    layout->add_field("description", ValueType::STRING);  // Variable length
    layout->add_field("short_name", ValueType::FIXED_STRING16);   // ≤16 chars
    layout->add_field("medium_name", ValueType::FIXED_STRING32);  // ≤32 chars
    layout->add_field("long_name", ValueType::FIXED_STRING64);    // ≤64 chars
    layout->finalize();

    total_node_size_ = layout->get_total_size_with_bitset();
    registry_->register_layout(std::move(layout));

    // Create NodeArena with FreeListArena for individual deallocation
    node_arena_ = node_arena_factory::create_free_list_arena(registry_);
  }

  void TearDown() override {
    node_arena_.reset();
    registry_.reset();
  }

  std::shared_ptr<LayoutRegistry> registry_;
  std::shared_ptr<NodeArena> node_arena_;
  size_t total_node_size_;
};

TEST_F(NodeArenaTest, SchemaLayoutSize) {
  // Verify the layout calculates correct sizes
  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");
  ASSERT_NE(layout, nullptr);

  // Check individual field sizes
  EXPECT_EQ(layout->get_field_layout("id")->size, 8);      // Int64
  EXPECT_EQ(layout->get_field_layout("count")->size, 4);   // Int32
  EXPECT_EQ(layout->get_field_layout("score")->size, 8);   // Double
  EXPECT_EQ(layout->get_field_layout("active")->size, 1);  // Bool
  EXPECT_EQ(layout->get_field_layout("description")->size,
            sizeof(StringRef));  // String → StringRef
  EXPECT_EQ(layout->get_field_layout("short_name")->size,
            sizeof(StringRef));  // FixedString16 → StringRef
  EXPECT_EQ(layout->get_field_layout("medium_name")->size,
            sizeof(StringRef));  // FixedString32 → StringRef
  EXPECT_EQ(layout->get_field_layout("long_name")->size,
            sizeof(StringRef));  // FixedString64 → StringRef

  // Total size should be aligned
  size_t expected_min_size =
      8 + 4 + 8 + 1 + 4 * sizeof(StringRef);  // + padding
  EXPECT_GE(layout->get_total_size(), expected_min_size);

  std::cout << "expected_min_size: " << expected_min_size << std::endl;
  std::cout << "Schema 'TestNode' total size: " << layout->get_total_size()
            << " bytes\n";
}

TEST_F(NodeArenaTest, BasicNodeAllocation) {
  // Test basic node allocation
  NodeHandle node1 = node_arena_->allocate_node("TestNode");
  EXPECT_FALSE(node1.is_null());
  EXPECT_EQ(node1.size, total_node_size_);
  EXPECT_NE(node1.ptr, nullptr);

  // Test multiple allocations
  NodeHandle node2 = node_arena_->allocate_node("TestNode");
  EXPECT_FALSE(node2.is_null());
  EXPECT_NE(node1.ptr, node2.ptr);  // Different memory locations

  // Test invalid schema
  NodeHandle invalid = node_arena_->allocate_node("NonExistent");
  EXPECT_TRUE(invalid.is_null());

  // Clean up
  node_arena_->deallocate_node(node1);
  node_arena_->deallocate_node(node2);
}

TEST_F(NodeArenaTest, NumericFieldOperations) {
  NodeHandle node = node_arena_->allocate_node("TestNode");
  ASSERT_FALSE(node.is_null());

  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");

  // Test Int64 field
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "id",
                                           Value{static_cast<int64_t>(12345)}));
  Value id_val = node_arena_->get_field_value(node, layout, "id");
  EXPECT_EQ(id_val.type(), ValueType::INT64);
  EXPECT_EQ(id_val.as_int64(), 12345L);

  // Test Int32 field
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "count", Value{42}));
  Value count_val = node_arena_->get_field_value(node, layout, "count");
  EXPECT_EQ(count_val.type(), ValueType::INT32);
  EXPECT_EQ(count_val.as_int32(), 42);

  // Test Double field
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "score", Value{95.5}));
  Value score_val = node_arena_->get_field_value(node, layout, "score");
  EXPECT_EQ(score_val.type(), ValueType::DOUBLE);
  EXPECT_DOUBLE_EQ(score_val.as_double(), 95.5);

  // Test Bool field
  EXPECT_TRUE(
      node_arena_->set_field_value(node, layout, "active", Value{true}));
  Value active_val = node_arena_->get_field_value(node, layout, "active");
  EXPECT_EQ(active_val.type(), ValueType::BOOL);
  EXPECT_EQ(active_val.as_bool(), true);

  node_arena_->deallocate_node(node);
}

TEST_F(NodeArenaTest, StringFieldOperations) {
  NodeHandle node = node_arena_->allocate_node("TestNode");
  ASSERT_FALSE(node.is_null());

  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");

  // Test variable-length String field
  std::string description =
      "This is a variable length description that can be any size";
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "description",
                                           Value{description}));
  Value desc_val = node_arena_->get_field_value(node, layout, "description");
  EXPECT_EQ(desc_val.type(), ValueType::STRING);
  EXPECT_EQ(desc_val.to_string(), description);

  // Test FixedString16 field (≤16 chars)
  std::string short_name = "Alice";  // 5 chars - fits in FixedString16
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "short_name",
                                           Value{short_name}));
  Value short_val = node_arena_->get_field_value(node, layout, "short_name");
  EXPECT_EQ(short_val.type(), ValueType::FIXED_STRING16);
  EXPECT_EQ(short_val.to_string(), short_name);

  // Test FixedString32 field (≤32 chars)
  std::string medium_name =
      "Alice Johnson Developer";  // 23 chars - fits in FixedString32
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "medium_name",
                                           Value{medium_name}));
  Value medium_val = node_arena_->get_field_value(node, layout, "medium_name");
  EXPECT_EQ(medium_val.type(), ValueType::FIXED_STRING32);
  EXPECT_EQ(medium_val.to_string(), medium_name);

  // Test FixedString64 field (≤64 chars)
  std::string long_name =
      "Alice Johnson Senior Software Engineer at TechCorp Inc.";  // 55 chars -
                                                                  // fits in
                                                                  // FixedString64
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "long_name",
                                           Value{long_name}));
  Value long_val = node_arena_->get_field_value(node, layout, "long_name");
  EXPECT_EQ(long_val.type(), ValueType::FIXED_STRING64);
  EXPECT_EQ(long_val.to_string(), long_name);

  node_arena_->deallocate_node(node);
}

TEST_F(NodeArenaTest, StringArenaIntegration) {
  NodeHandle node = node_arena_->allocate_node("TestNode");
  ASSERT_FALSE(node.is_null());

  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");

  // Set strings of different sizes to test automatic pool selection
  std::string str1 = "short";  // 5 chars → FixedString16 pool
  std::string str2 =
      "medium length text here";  // 25 chars → FixedString32 pool
  std::string str3 =
      "this is a very long string that exceeds the 64 character limit and "
      "should go to unlimited pool";  // 100+ chars → String pool

  EXPECT_TRUE(
      node_arena_->set_field_value(node, layout, "short_name", Value{str1}));
  EXPECT_TRUE(
      node_arena_->set_field_value(node, layout, "medium_name", Value{str2}));
  EXPECT_TRUE(
      node_arena_->set_field_value(node, layout, "description", Value{str3}));

  // Verify they're stored correctly and can be retrieved
  EXPECT_EQ(
      node_arena_->get_field_value(node, layout, "short_name").to_string(),
      str1);
  EXPECT_EQ(
      node_arena_->get_field_value(node, layout, "medium_name").to_string(),
      str2);
  EXPECT_EQ(
      node_arena_->get_field_value(node, layout, "description").to_string(),
      str3);

  // Verify StringArena statistics
  StringArena* string_arena = node_arena_->get_string_arena();
  ASSERT_NE(string_arena, nullptr);

  // Check that different pools are being used
  StringPool* pool16 = string_arena->get_pool(ValueType::FIXED_STRING16);
  StringPool* pool32 = string_arena->get_pool(ValueType::FIXED_STRING32);
  StringPool* poolStr = string_arena->get_pool(ValueType::STRING);

  EXPECT_GT(pool16->get_total_allocated(), 0);   // str1 went here
  EXPECT_GT(pool32->get_total_allocated(), 0);   // str2 went here
  EXPECT_GT(poolStr->get_total_allocated(), 0);  // str3 went here

  node_arena_->deallocate_node(node);
}

TEST_F(NodeArenaTest, ErrorHandling) {
  NodeHandle node = node_arena_->allocate_node("TestNode");
  ASSERT_FALSE(node.is_null());

  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");

  // Test invalid field names
  EXPECT_FALSE(
      node_arena_->set_field_value(node, layout, "nonexistent", Value{42}));
  Value invalid_val = node_arena_->get_field_value(node, layout, "nonexistent");
  EXPECT_TRUE(invalid_val.is_null());

  // Test invalid schema names
  // EXPECT_FALSE(
  //     node_arena_->set_field_value(node, layout, "id", Value{42}));
  // Value invalid_schema_val =
  //     node_arena_->get_field_value(node, layout, "id");
  // EXPECT_TRUE(invalid_schema_val.is_null());

  // Test operations on null handles
  NodeHandle null_handle;
  EXPECT_FALSE(
      node_arena_->set_field_value(null_handle, layout, "id", Value{42}));
  Value null_handle_val =
      node_arena_->get_field_value(null_handle, layout, "id");
  EXPECT_TRUE(null_handle_val.is_null());

  node_arena_->deallocate_node(node);
}

TEST_F(NodeArenaTest, CompleteNodeLifecycle) {
  // Test complete lifecycle with all field types
  NodeHandle node = node_arena_->allocate_node("TestNode");
  ASSERT_FALSE(node.is_null());

  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");

  // Set all fields
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "id",
                                           Value{static_cast<int64_t>(1001)}));
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "count", Value{500}));
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "score", Value{88.7}));
  EXPECT_TRUE(
      node_arena_->set_field_value(node, layout, "active", Value{false}));
  EXPECT_TRUE(node_arena_->set_field_value(
      node, layout, "description",
      Value{"Complete test node with all field types"}));
  EXPECT_TRUE(
      node_arena_->set_field_value(node, layout, "short_name", Value{"Test"}));
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "medium_name",
                                           Value{"Test Node Medium"}));
  EXPECT_TRUE(node_arena_->set_field_value(
      node, layout, "long_name",
      Value{"Test Node with Long Name for Testing Purposes"}));

  // Verify all fields
  EXPECT_EQ(node_arena_->get_field_value(node, layout, "id").as_int64(), 1001L);
  EXPECT_EQ(node_arena_->get_field_value(node, layout, "count").as_int32(),
            500);
  EXPECT_DOUBLE_EQ(
      node_arena_->get_field_value(node, layout, "score").as_double(), 88.7);
  EXPECT_EQ(node_arena_->get_field_value(node, layout, "active").as_bool(),
            false);
  EXPECT_EQ(
      node_arena_->get_field_value(node, layout, "description").to_string(),
      "Complete test node with all field types");
  EXPECT_EQ(
      node_arena_->get_field_value(node, layout, "short_name").to_string(),
      "Test");
  EXPECT_EQ(
      node_arena_->get_field_value(node, layout, "medium_name").to_string(),
      "Test Node Medium");
  EXPECT_EQ(node_arena_->get_field_value(node, layout, "long_name").to_string(),
            "Test Node with Long Name for Testing Purposes");

  // Test updates
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "count", Value{600}));
  EXPECT_EQ(node_arena_->get_field_value(node, layout, "count").as_int32(),
            600);

  node_arena_->deallocate_node(node);
}

TEST_F(NodeArenaTest, MultipleNodesAndDeallocation) {
  std::vector<NodeHandle> nodes;

  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");

  // Allocate multiple nodes
  for (int i = 0; i < 10; ++i) {
    NodeHandle node = node_arena_->allocate_node("TestNode");
    ASSERT_FALSE(node.is_null());

    // Set unique values
    EXPECT_TRUE(node_arena_->set_field_value(node, layout, "id",
                                             Value{static_cast<int64_t>(i)}));
    EXPECT_TRUE(
        node_arena_->set_field_value(node, layout, "count", Value{i * 10}));
    EXPECT_TRUE(node_arena_->set_field_value(
        node, layout, "description", Value{"Node " + std::to_string(i)}));

    nodes.push_back(node);
  }

  // Verify all nodes have correct values
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(node_arena_->get_field_value(nodes[i], layout, "id").as_int64(),
              static_cast<int64_t>(i));
    EXPECT_EQ(
        node_arena_->get_field_value(nodes[i], layout, "count").as_int32(),
        i * 10);
    EXPECT_EQ(node_arena_->get_field_value(nodes[i], layout, "description")
                  .to_string(),
              "Node " + std::to_string(i));
  }

  // Deallocate every other node
  for (int i = 0; i < 10; i += 2) {
    node_arena_->deallocate_node(nodes[i]);
  }

  // Verify remaining nodes still work
  for (int i = 1; i < 10; i += 2) {
    EXPECT_EQ(node_arena_->get_field_value(nodes[i], layout, "id").as_int64(),
              static_cast<int64_t>(i));
    EXPECT_EQ(node_arena_->get_field_value(nodes[i], layout, "description")
                  .to_string(),
              "Node " + std::to_string(i));
  }

  // Clean up remaining nodes
  for (int i = 1; i < 10; i += 2) {
    node_arena_->deallocate_node(nodes[i]);
  }
}

TEST_F(NodeArenaTest, ArenaStatistics) {
  size_t initial_allocated = node_arena_->get_total_allocated();
  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");
  // Allocate some nodes
  std::vector<NodeHandle> nodes;
  for (int i = 0; i < 5; ++i) {
    NodeHandle node = node_arena_->allocate_node("TestNode");
    ASSERT_FALSE(node.is_null());

    // Add some strings
    EXPECT_TRUE(node_arena_->set_field_value(
        node, layout, "description",
        Value{"Test string " + std::to_string(i)}));
    nodes.push_back(node);
  }

  // Memory should still be within pre-allocated chunk (2MB is much larger than
  // 5 nodes)
  size_t after_allocation = node_arena_->get_total_allocated();
  EXPECT_EQ(after_allocation,
            initial_allocated);  // Should still be same 2MB chunk

  // Clean up
  for (auto& node : nodes) {
    node_arena_->deallocate_node(node);
  }

  // Memory usage should be tracked correctly
  EXPECT_GT(node_arena_->get_total_allocated(), 0);
}

TEST_F(NodeArenaTest, ResetAndClear) {
  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");
  // Allocate and populate some nodes
  std::vector<NodeHandle> nodes;
  for (int i = 0; i < 3; ++i) {
    NodeHandle node = node_arena_->allocate_node("TestNode");
    ASSERT_FALSE(node.is_null());
    EXPECT_TRUE(node_arena_->set_field_value(
        node, layout, "description",
        Value{"Test string for reset " + std::to_string(i)}));
    nodes.push_back(node);
  }

  size_t allocated_before = node_arena_->get_total_allocated();
  EXPECT_GT(allocated_before, 0);

  // Test reset (should keep chunks but reset usage)
  node_arena_->reset();

  // Old handles should be invalid now, but chunk memory is still allocated
  size_t after_reset_chunks = node_arena_->get_total_allocated();
  EXPECT_GT(after_reset_chunks, 0);  // Chunks still exist

  // Individual allocations should be reset to 0 (check via used_bytes)
  if (auto* free_list =
          dynamic_cast<FreeListArena*>(node_arena_->get_mem_arena())) {
    size_t after_reset_used = free_list->get_used_bytes();
    EXPECT_EQ(after_reset_used, 0);  // No used allocations after reset
  }

  // Should be able to allocate new nodes
  NodeHandle new_node = node_arena_->allocate_node("TestNode");
  EXPECT_FALSE(new_node.is_null());

  // Deallocate the node before calling clear
  node_arena_->deallocate_node(new_node);

  // Test clear (should free all memory)
  node_arena_->clear();

  // Should be able to allocate again after clear
  NodeHandle after_clear_node = node_arena_->allocate_node("TestNode");
  EXPECT_FALSE(after_clear_node.is_null());

  // Clean up the final node
  node_arena_->deallocate_node(after_clear_node);
}

TEST_F(NodeArenaTest, StringUpdateDeallocation) {
  // Test that updating string fields properly deallocates old strings
  NodeHandle node = node_arena_->allocate_node("TestNode");
  std::shared_ptr<SchemaLayout> layout = registry_->get_layout("TestNode");
  ASSERT_FALSE(node.is_null());

  // Get initial string arena state - StringPools pre-allocate 1MB chunks
  StringArena* string_arena = node_arena_->get_string_arena();
  size_t initial_chunk_memory = 0;
  size_t initial_used_memory = 0;
  if (auto* pool16 = string_arena->get_pool(ValueType::FIXED_STRING16)) {
    initial_chunk_memory =
        pool16->get_total_allocated();  // Should be 1MB (pre-allocated)
    initial_used_memory =
        pool16->get_used_bytes();  // Should be 0 (no strings yet)
  }

  // Set initial string value
  std::string original_string = "Hello World";
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "short_name",
                                           Value{original_string}));

  // Chunk memory should be unchanged (fits in pre-allocated 1MB)
  // But used memory should increase
  size_t after_first_chunk = 0;
  size_t after_first_used = 0;
  if (auto* pool16 = string_arena->get_pool(ValueType::FIXED_STRING16)) {
    after_first_chunk = pool16->get_total_allocated();
    after_first_used = pool16->get_used_bytes();
  }
  EXPECT_EQ(after_first_chunk, initial_chunk_memory);  // Same 1MB chunk
  EXPECT_GT(after_first_used, initial_used_memory);  // More bytes actually used

  // Update with new string value
  std::string new_string = "Goodbye";
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "short_name",
                                           Value{new_string}));

  // Verify the new value is set correctly
  Value retrieved = node_arena_->get_field_value(node, layout, "short_name");
  EXPECT_EQ(retrieved.to_string(), new_string);

  // Due to reference counting, both strings might still be allocated
  // But we should verify there's no unbounded growth with multiple updates
  std::string third_string = "Final";
  EXPECT_TRUE(node_arena_->set_field_value(node, layout, "short_name",
                                           Value{third_string}));

  Value final_value = node_arena_->get_field_value(node, layout, "short_name");
  EXPECT_EQ(final_value.to_string(), third_string);

  // Clean up
  node_arena_->deallocate_node(node);
}