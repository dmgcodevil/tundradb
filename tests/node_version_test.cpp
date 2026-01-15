#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include "../include/node_arena.hpp"
#include "../include/schema.hpp"
#include "../include/schema_layout.hpp"

using namespace tundradb;

class NodeVersionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    layout_registry_ = std::make_shared<LayoutRegistry>();

    // Create fields
    llvm::SmallVector<std::shared_ptr<Field>, 5> fields;
    fields.push_back(std::make_shared<Field>("id", ValueType::INT64));
    fields.push_back(std::make_shared<Field>("count", ValueType::INT32));
    fields.push_back(std::make_shared<Field>("score", ValueType::DOUBLE));
    fields.push_back(std::make_shared<Field>("active", ValueType::BOOL));
    fields.push_back(std::make_shared<Field>("description", ValueType::STRING));

    // Create schema (need to move the fields vector)
    schema_ = std::make_shared<Schema>(std::string("TestNode"), 1u,
                                       std::move(fields));

    // Get field pointers for tests
    id_field_ = schema_->get_field("id");
    count_field_ = schema_->get_field("count");
    score_field_ = schema_->get_field("score");
    active_field_ = schema_->get_field("active");
    desc_field_ = schema_->get_field("description");

    // Create layout from schema
    auto layout = std::make_unique<SchemaLayout>(schema_);
    total_node_size_ = layout->get_total_size_with_bitset();
    layout_registry_->register_layout(std::move(layout));

    // Get layout pointer
    layout_ = layout_registry_->get_layout("TestNode");

    // Create NodeArena WITH versioning enabled
    node_arena_versioned_ = node_arena_factory::create_free_list_arena(
        layout_registry_, 2 * 1024 * 1024, 64, true);

    // Create NodeArena WITHOUT versioning for comparison
    node_arena_non_versioned_ =
        node_arena_factory::create_free_list_arena(layout_registry_);
  }

  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;
  std::unique_ptr<NodeArena> node_arena_versioned_;
  std::unique_ptr<NodeArena> node_arena_non_versioned_;
  size_t total_node_size_;

  // Field pointers for convenience
  std::shared_ptr<Field> id_field_;
  std::shared_ptr<Field> count_field_;
  std::shared_ptr<Field> score_field_;
  std::shared_ptr<Field> active_field_;
  std::shared_ptr<Field> desc_field_;
};

// =============================================================================
// Basic Versioning Tests
// =============================================================================

TEST_F(NodeVersionTest, AllocateVersionedNode) {
  // Allocate node with versioning enabled
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  ASSERT_FALSE(handle.is_null());
  ASSERT_TRUE(handle.is_versioned());
  ASSERT_EQ(handle.get_version_id(), 0);  // Base version
  ASSERT_EQ(handle.count_versions(), 1);
}

TEST_F(NodeVersionTest, AllocateNonVersionedNode) {
  // Allocate node with versioning disabled
  NodeHandle handle = node_arena_non_versioned_->allocate_node("TestNode");

  ASSERT_FALSE(handle.is_null());
  ASSERT_FALSE(handle.is_versioned());
  ASSERT_EQ(handle.get_version_id(), 0);
  ASSERT_EQ(handle.count_versions(), 1);
}

TEST_F(NodeVersionTest, CreateSingleVersion) {
  // Create base node
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");
  ASSERT_TRUE(handle.is_versioned());
  ASSERT_EQ(handle.get_version_id(), 0);

  // Set initial value in v0 (use v0 method)
  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(100)));

  // Still at v0
  ASSERT_EQ(handle.get_version_id(), 0);
  ASSERT_EQ(handle.count_versions(), 1);

  // Get value back
  Value id_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_EQ(id_value.as_int64(), 100);

  // Update value (creates version 1)
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                         Value(int64_t(200)));

  // Verify version chain
  ASSERT_EQ(handle.count_versions(), 2);  // v0 + v1
  ASSERT_EQ(handle.get_version_id(), 1);  // Current version

  // Verify new value
  Value new_id_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_EQ(new_id_value.as_int64(), 200);
}

TEST_F(NodeVersionTest, CreateMultipleVersions) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  // Set initial value in v0
  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(0)));

  // Create 4 more versions (v1, v2, v3, v4)
  for (int64_t i = 1; i <= 4; i++) {
    node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                           Value(i * 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Verify version count (v0, v1, v2, v3, v4 = 5 versions)
  ASSERT_EQ(handle.count_versions(), 5);
  ASSERT_EQ(handle.get_version_id(), 4);  // Current version

  // Verify final value
  Value final_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_EQ(final_value.as_int64(), 400);
}

// =============================================================================
// Field-Level Copy-on-Write Tests
// =============================================================================

TEST_F(NodeVersionTest, FieldLevelCopyOnWrite) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  // Set initial values for multiple fields in v0
  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(100)));
  node_arena_versioned_->set_field_value_v0(handle, layout_, count_field_,
                                            Value(int32_t(10)));
  node_arena_versioned_->set_field_value_v0(handle, layout_, score_field_,
                                            Value(double(1.5)));

  // Still at v0
  ASSERT_EQ(handle.get_version_id(), 0);
  VersionInfo* v0 = handle.get_version_info();
  ASSERT_EQ(v0->version_id, 0);

  // Update only ONE field (should only store that field in new version)
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  node_arena_versioned_->set_field_value(handle, layout_, count_field_,
                                         Value(int32_t(20)));

  // Check new version
  ASSERT_EQ(handle.get_version_id(), 1);
  ASSERT_EQ(handle.count_versions(), 2);  // v0 + v1

  VersionInfo* v1 = handle.get_version_info();
  ASSERT_EQ(v1->updated_fields.size(), 1);  // Only 1 field changed in v1!
  ASSERT_EQ(v1->version_id, 1);
  ASSERT_NE(v1->prev, nullptr);
  ASSERT_EQ(v1->prev->version_id, 0);  // Linked to v0

  // Verify values
  Value id_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  Value count_value =
      node_arena_versioned_->get_field_value(handle, layout_, count_field_);
  Value score_value =
      node_arena_versioned_->get_field_value(handle, layout_, score_field_);

  ASSERT_EQ(id_value.as_int64(), 100);             // From v0 (base)
  ASSERT_EQ(count_value.as_int32(), 20);           // From v1 (updated)
  ASSERT_DOUBLE_EQ(score_value.as_double(), 1.5);  // From v0 (base)
}

// =============================================================================
// Temporal Validity Tests
// =============================================================================

TEST_F(NodeVersionTest, TemporalValidityIntervals) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(100)));

  VersionInfo* v0 = handle.get_version_info();
  uint64_t t0 = v0->valid_from;

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                         Value(int64_t(200)));

  VersionInfo* v1 = handle.get_version_info();
  uint64_t t1 = v1->valid_from;

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                         Value(int64_t(300)));

  VersionInfo* v2 = handle.get_version_info();
  uint64_t t2 = v2->valid_from;

  ASSERT_TRUE(v2->is_valid_at(t2));
  ASSERT_TRUE(v2->is_valid_at(t2 + 1000000));

  ASSERT_NE(v1, nullptr);
  v1 = v2->prev;
  ASSERT_TRUE(v1->is_valid_at(t1));
  ASSERT_FALSE(v1->is_valid_at(t2));

  ASSERT_NE(v1->prev, nullptr);
  v0 = v1->prev;
  ASSERT_TRUE(v0->is_valid_at(t0));
  ASSERT_FALSE(v0->is_valid_at(t1));
}

TEST_F(NodeVersionTest, FindVersionAtTime) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(100)));
  VersionInfo* v0 = handle.get_version_info();
  uint64_t t0 = v0->valid_from;

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                         Value(int64_t(200)));
  VersionInfo* v1 = handle.get_version_info();
  uint64_t t1 = v1->valid_from;

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                         Value(int64_t(300)));
  VersionInfo* v2 = handle.get_version_info();
  uint64_t t2 = v2->valid_from;

  const VersionInfo* version_at_t0 = handle.find_version_at_time(t0);
  const VersionInfo* version_at_t1 = handle.find_version_at_time(t1);
  const VersionInfo* version_at_t2 = handle.find_version_at_time(t2);

  ASSERT_NE(version_at_t0, nullptr);
  ASSERT_NE(version_at_t1, nullptr);
  ASSERT_NE(version_at_t2, nullptr);

  ASSERT_EQ(version_at_t0->version_id, 0);
  ASSERT_EQ(version_at_t1->version_id, 1);
  ASSERT_EQ(version_at_t2->version_id, 2);
}

// =============================================================================
// String Handling Tests
// =============================================================================

TEST_F(NodeVersionTest, StringVersioning) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  node_arena_versioned_->set_field_value_v0(handle, layout_, desc_field_,
                                            Value("Alice"));

  Value v0_value =
      node_arena_versioned_->get_field_value(handle, layout_, desc_field_);
  ASSERT_EQ(v0_value.as_string(), "Alice");

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  node_arena_versioned_->set_field_value(handle, layout_, desc_field_,
                                         Value("Alicia"));

  Value v1_value =
      node_arena_versioned_->get_field_value(handle, layout_, desc_field_);
  ASSERT_EQ(v1_value.as_string(), "Alicia");

  ASSERT_EQ(handle.count_versions(), 2);
}

// =============================================================================
// Non-Versioned Mode Tests
// =============================================================================

TEST_F(NodeVersionTest, NonVersionedUpdateNoVersionCreated) {
  NodeHandle handle = node_arena_non_versioned_->allocate_node("TestNode");

  ASSERT_FALSE(handle.is_versioned());

  node_arena_non_versioned_->set_field_value(handle, layout_, id_field_,
                                             Value(int64_t(100)));

  node_arena_non_versioned_->set_field_value(handle, layout_, id_field_,
                                             Value(int64_t(200)));

  ASSERT_EQ(handle.count_versions(), 1);
  ASSERT_FALSE(handle.is_versioned());

  Value value =
      node_arena_non_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_EQ(value.as_int64(), 200);
}

// =============================================================================
// Batch Update Tests
// =============================================================================

TEST_F(NodeVersionTest, BatchUpdateMultipleFields) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(100)));
  node_arena_versioned_->set_field_value_v0(handle, layout_, count_field_,
                                            Value(int32_t(10)));
  node_arena_versioned_->set_field_value_v0(handle, layout_, score_field_,
                                            Value(double(1.5)));

  ASSERT_EQ(handle.get_version_id(), 0);

  std::vector<std::pair<std::shared_ptr<Field>, Value>> updates = {
      {id_field_, Value(int64_t(200))},
      {count_field_, Value(int32_t(20))},
      {score_field_, Value(double(2.5))}};

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto result = node_arena_versioned_->update_fields(handle, layout_, updates);
  ASSERT_TRUE(result.ok()) << "Update failed: " << result.status().message();

  ASSERT_EQ(handle.get_version_id(), 1);
  ASSERT_EQ(handle.count_versions(), 2);

  VersionInfo* v1 = handle.get_version_info();
  ASSERT_EQ(v1->updated_fields.size(), 3);

  Value id = node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  Value count =
      node_arena_versioned_->get_field_value(handle, layout_, count_field_);
  Value score =
      node_arena_versioned_->get_field_value(handle, layout_, score_field_);

  ASSERT_EQ(id.as_int64(), 200);
  ASSERT_EQ(count.as_int32(), 20);
  ASSERT_DOUBLE_EQ(score.as_double(), 2.5);
}

// =============================================================================
// NULL Value Handling Tests
// =============================================================================

TEST_F(NodeVersionTest, NullValueHandling) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(100)));

  Value v0_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_FALSE(v0_value.is_null());
  ASSERT_EQ(v0_value.as_int64(), 100);

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  node_arena_versioned_->set_field_value(handle, layout_, id_field_, Value{});

  ASSERT_EQ(handle.get_version_id(), 1);
  ASSERT_EQ(handle.count_versions(), 2);

  Value v1_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_TRUE(v1_value.is_null());

  VersionInfo* v1 = handle.get_version_info();
  ASSERT_EQ(v1->updated_fields.size(), 1);

  const FieldLayout* id_layout = layout_->get_field_layout(id_field_);
  ASSERT_NE(id_layout, nullptr);
  ASSERT_EQ(v1->updated_fields[id_layout->index], nullptr);  // nullptr = NULL
}

TEST_F(NodeVersionTest, NullToNonNullTransition) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value{});

  Value v0_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_TRUE(v0_value.is_null());

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                         Value(int64_t(100)));

  ASSERT_EQ(handle.get_version_id(), 1);
  Value v1_value =
      node_arena_versioned_->get_field_value(handle, layout_, id_field_);
  ASSERT_FALSE(v1_value.is_null());
  ASSERT_EQ(v1_value.as_int64(), 100);
}

// =============================================================================
// Performance/Stats Tests
// =============================================================================

TEST_F(NodeVersionTest, VersionCounterIncreases) {
  NodeHandle handle = node_arena_versioned_->allocate_node("TestNode");

  uint64_t initial_counter = node_arena_versioned_->get_version_counter();
  node_arena_versioned_->set_field_value_v0(handle, layout_, id_field_,
                                            Value(int64_t(0)));

  for (int i = 1; i <= 10; i++) {
    node_arena_versioned_->set_field_value(handle, layout_, id_field_,
                                           Value(int64_t(i)));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  uint64_t final_counter = node_arena_versioned_->get_version_counter();

  // Counter should have increased by 10 (v1-v10)
  ASSERT_EQ(final_counter - initial_counter, 10);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
