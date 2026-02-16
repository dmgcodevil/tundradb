#include "../include/node_view.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <thread>

#include "../include/node.hpp"
#include "../include/node_arena.hpp"
#include "../include/schema.hpp"
#include "../include/schema_layout.hpp"
#include "../include/temporal_context.hpp"

using namespace tundradb;

class NodeViewTest : public ::testing::Test {
 protected:
  void SetUp() override {
    layout_registry_ = std::make_shared<LayoutRegistry>();

    // Create fields
    llvm::SmallVector<std::shared_ptr<Field>, 4> fields;
    fields.push_back(std::make_shared<Field>("id", ValueType::INT64));
    fields.push_back(std::make_shared<Field>("name", ValueType::STRING));
    fields.push_back(std::make_shared<Field>("age", ValueType::INT32));
    fields.push_back(std::make_shared<Field>("department", ValueType::STRING));

    // Create schema
    schema_ =
        std::make_shared<Schema>(std::string("User"), 1u, std::move(fields));

    // Get field pointers for tests
    id_field_ = schema_->get_field("id");
    name_field_ = schema_->get_field("name");
    age_field_ = schema_->get_field("age");
    department_field_ = schema_->get_field("department");

    // Create layout from schema
    auto layout = std::make_unique<SchemaLayout>(schema_);
    layout_registry_->register_layout(std::move(layout));

    // Get layout pointer
    layout_ = layout_registry_->get_layout("User");

    // Create NodeArena WITH versioning enabled
    node_arena_versioned_ = node_arena_factory::create_free_list_arena(
        layout_registry_, 2 * 1024 * 1024, 64, true);

    // Create NodeArena WITHOUT versioning for comparison
    node_arena_non_versioned_ = node_arena_factory::create_free_list_arena(
        layout_registry_, 2 * 1024 * 1024, 64, false);
  }

  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;
  std::shared_ptr<NodeArena> node_arena_versioned_;
  std::shared_ptr<NodeArena> node_arena_non_versioned_;

  // Field pointers for convenience
  std::shared_ptr<Field> id_field_;
  std::shared_ptr<Field> name_field_;
  std::shared_ptr<Field> age_field_;
  std::shared_ptr<Field> department_field_;
};

/**
 * Test: Basic NodeView without temporal context (current version)
 */
TEST_F(NodeViewTest, CurrentVersionView) {
  // Allocate versioned node
  NodeHandle handle = node_arena_versioned_->allocate_node("User");

  // Create Node wrapper
  Node node(0, "User", {}, std::make_unique<NodeHandle>(std::move(handle)),
            node_arena_versioned_, schema_, layout_);

  // Set initial values
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            id_field_, Value(int64_t(100)));
  node_arena_versioned_->set_field_value_v0(
      *node.get_handle(), layout_, name_field_, Value(std::string("Alice")));
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            age_field_, Value(int32_t(25)));
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            department_field_,
                                            Value(std::string("Engineering")));

  // Create view without temporal context (current version)
  NodeView view = node.view(nullptr);

  ASSERT_TRUE(view.is_visible());

  auto age_result = view.get_value(age_field_);
  ASSERT_TRUE(age_result.ok());
  EXPECT_EQ(age_result.ValueOrDie().as_int32(), 25);

  auto dept_result = view.get_value(department_field_);
  ASSERT_TRUE(dept_result.ok());
  EXPECT_EQ(dept_result.ValueOrDie().as_string(), "Engineering");
}

/**
 * Test: Time-travel query using VALIDTIME
 */
TEST_F(NodeViewTest, TimeTravelValidTime) {
  // Allocate versioned node
  NodeHandle handle = node_arena_versioned_->allocate_node("User");

  // Create Node wrapper
  Node node(0, "User", {}, std::make_unique<NodeHandle>(std::move(handle)),
            node_arena_versioned_, schema_, layout_);

  // v0: Alice, age=25, dept=Engineering
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            id_field_, Value(int64_t(100)));
  node_arena_versioned_->set_field_value_v0(
      *node.get_handle(), layout_, name_field_, Value(std::string("Alice")));
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            age_field_, Value(int32_t(25)));
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            department_field_,
                                            Value(std::string("Engineering")));

  uint64_t t0 = node.get_handle()->version_info_->valid_from;

  // v1: Update age to 26 at time t1
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  std::vector<std::pair<std::shared_ptr<Field>, Value>> updates1 = {
      {age_field_, Value(int32_t(26))}};
  auto update1_result = node_arena_versioned_->update_fields(*node.get_handle(),
                                                             layout_, updates1);
  ASSERT_TRUE(update1_result.ok());

  uint64_t t1 = node.get_handle()->version_info_->valid_from;

  // v2: Update department to Sales at time t2
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  std::vector<std::pair<std::shared_ptr<Field>, Value>> updates2 = {
      {department_field_, Value(std::string("Sales"))}};
  auto update2_result = node_arena_versioned_->update_fields(*node.get_handle(),
                                                             layout_, updates2);
  ASSERT_TRUE(update2_result.ok());

  uint64_t t2 = node.get_handle()->version_info_->valid_from;

  // Query at t0 (before any updates)
  TemporalContext ctx_t0(TemporalSnapshot::as_of_valid(t0));
  NodeView view_t0 = node.view(&ctx_t0);

  ASSERT_TRUE(view_t0.is_visible());
  auto age_t0 = view_t0.get_value(age_field_);
  ASSERT_TRUE(age_t0.ok());
  EXPECT_EQ(age_t0.ValueOrDie().as_int32(), 25);

  auto dept_t0 = view_t0.get_value(department_field_);
  ASSERT_TRUE(dept_t0.ok());
  EXPECT_EQ(dept_t0.ValueOrDie().as_string(), "Engineering");

  // Query at t1 (after age update, before dept update)
  TemporalContext ctx_t1(TemporalSnapshot::as_of_valid(t1));
  NodeView view_t1 = node.view(&ctx_t1);

  ASSERT_TRUE(view_t1.is_visible());
  auto age_t1 = view_t1.get_value(age_field_);
  ASSERT_TRUE(age_t1.ok());
  EXPECT_EQ(age_t1.ValueOrDie().as_int32(), 26);  // Updated

  auto dept_t1 = view_t1.get_value(department_field_);
  ASSERT_TRUE(dept_t1.ok());
  EXPECT_EQ(dept_t1.ValueOrDie().as_string(),
            "Engineering");  // Still old value

  // Query at t2 (after both updates)
  TemporalContext ctx_t2(TemporalSnapshot::as_of_valid(t2));
  NodeView view_t2 = node.view(&ctx_t2);

  ASSERT_TRUE(view_t2.is_visible());
  auto age_t2 = view_t2.get_value(age_field_);
  ASSERT_TRUE(age_t2.ok());
  EXPECT_EQ(age_t2.ValueOrDie().as_int32(), 26);

  auto dept_t2 = view_t2.get_value(department_field_);
  ASSERT_TRUE(dept_t2.ok());
  EXPECT_EQ(dept_t2.ValueOrDie().as_string(), "Sales");  // Updated
}

/**
 * Test: Temporal context caches resolved versions
 */
TEST_F(NodeViewTest, TemporalContextCaching) {
  // Allocate versioned node
  NodeHandle handle = node_arena_versioned_->allocate_node("User");

  // Create Node wrapper
  Node node(0, "User", {}, std::make_unique<NodeHandle>(std::move(handle)),
            node_arena_versioned_, schema_, layout_);

  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            age_field_, Value(int32_t(25)));

  uint64_t t0 = node.get_handle()->version_info_->valid_from;

  // Create temporal context
  TemporalContext ctx(TemporalSnapshot::as_of_valid(t0));

  // First view: should resolve and cache version
  NodeView view1 = node.view(&ctx);
  ASSERT_TRUE(view1.is_visible());

  // Second view: should use cached version
  NodeView view2 = node.view(&ctx);
  ASSERT_TRUE(view2.is_visible());

  // Both views should resolve to the same version
  EXPECT_EQ(view1.get_resolved_version(), view2.get_resolved_version());
}

/**
 * Test: NodeView works with non-versioned nodes
 */
TEST_F(NodeViewTest, NonVersionedNodeView) {
  // Allocate NON-versioned node
  NodeHandle handle = node_arena_non_versioned_->allocate_node("User");

  // Create Node wrapper
  Node node(0, "User", {}, std::make_unique<NodeHandle>(std::move(handle)),
            node_arena_non_versioned_, schema_, layout_);

  // Set values (non-versioned, direct writes)
  node_arena_non_versioned_->set_field_value_v0(
      *node.get_handle(), layout_, name_field_, Value(std::string("Bob")));
  node_arena_non_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                                age_field_, Value(int32_t(30)));

  // Create view without temporal context (should work for non-versioned)
  NodeView view = node.view(nullptr);

  ASSERT_TRUE(view.is_visible());

  auto name = view.get_value(name_field_);
  auto age = view.get_value(age_field_);

  ASSERT_TRUE(name.ok());
  ASSERT_TRUE(age.ok());

  EXPECT_EQ(name.ValueOrDie().as_string(), "Bob");
  EXPECT_EQ(age.ValueOrDie().as_int32(), 30);
}

/**
 * Test: Multiple field reads from same view (no repeated lookups)
 */
TEST_F(NodeViewTest, MultipleFieldReadsFromSameView) {
  // Allocate versioned node
  NodeHandle handle = node_arena_versioned_->allocate_node("User");

  // Create Node wrapper
  Node node(0, "User", {}, std::make_unique<NodeHandle>(std::move(handle)),
            node_arena_versioned_, schema_, layout_);

  node_arena_versioned_->set_field_value_v0(
      *node.get_handle(), layout_, name_field_, Value(std::string("Alice")));
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            age_field_, Value(int32_t(25)));
  node_arena_versioned_->set_field_value_v0(*node.get_handle(), layout_,
                                            department_field_,
                                            Value(std::string("Engineering")));

  uint64_t t0 = node.get_handle()->version_info_->valid_from;

  // Create view once
  TemporalContext ctx(TemporalSnapshot::as_of_valid(t0));
  NodeView view = node.view(&ctx);

  // Read multiple fields from same view
  auto name = view.get_value(name_field_);
  auto age = view.get_value(age_field_);
  auto dept = view.get_value(department_field_);

  ASSERT_TRUE(name.ok());
  ASSERT_TRUE(age.ok());
  ASSERT_TRUE(dept.ok());

  EXPECT_EQ(name.ValueOrDie().as_string(), "Alice");
  EXPECT_EQ(age.ValueOrDie().as_int32(), 25);
  EXPECT_EQ(dept.ValueOrDie().as_string(), "Engineering");
}

// namespace tundradb
