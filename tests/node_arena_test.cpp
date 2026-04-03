#include "../include/node_arena.hpp"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <vector>

#include "../include/field_update.hpp"
#include "../include/memory_arena.hpp"
#include "../include/node.hpp"
#include "../include/schema.hpp"
#include "../include/schema_layout.hpp"
#include "../include/types.hpp"

using namespace tundradb;

class NodeArenaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = std::make_shared<SchemaRegistry>();

    auto fields = std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("name", arrow::utf8(), false),
        arrow::field("age", arrow::int32(), true),
        arrow::field("score", arrow::float64(), true),
        arrow::field("active", arrow::boolean(), true),
        arrow::field("count", arrow::int64(), true),
        arrow::field("rating", arrow::float32(), true),
        arrow::field("tags", arrow::list(arrow::field("item", arrow::utf8())),
                     true),
        arrow::field("nums", arrow::list(arrow::field("item", arrow::int32())),
                     true),
        arrow::field("props", arrow::map(arrow::utf8(), arrow::binary()), true),
        arrow::field("i64_arr",
                     arrow::list(arrow::field("item", arrow::int64())), true),
        arrow::field("f64_arr",
                     arrow::list(arrow::field("item", arrow::float64())), true),
        arrow::field("bool_arr",
                     arrow::list(arrow::field("item", arrow::boolean())),
                     true)};
    ASSERT_TRUE(registry_->create("Full", arrow::schema(fields)).ok());

    mgr_ = std::make_unique<NodeManager>(registry_);
    mgr_versioned_ = std::make_unique<NodeManager>(registry_, true, true, true);
  }

  std::shared_ptr<SchemaRegistry> registry_;
  std::unique_ptr<NodeManager> mgr_;
  std::unique_ptr<NodeManager> mgr_versioned_;
};

// =============================================================================
// 1. nested_path FieldUpdate — all value types via Node::update_fields
// =============================================================================

TEST_F(NodeArenaTest, NestedPathAllTypes) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"Alice"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(1)}, UpdateType::SET,
                                   std::vector<std::string>{"i32"}}})
                  .ok());
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int64_t(2)}, UpdateType::SET,
                                   std::vector<std::string>{"i64"}}})
                  .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{3.14}, UpdateType::SET,
                                       std::vector<std::string>{"f64"}}})
          .ok());
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{float(1.5f)}, UpdateType::SET,
                                   std::vector<std::string>{"f32"}}})
                  .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{true}, UpdateType::SET,
                                       std::vector<std::string>{"b"}}})
          .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{"hello"}, UpdateType::SET,
                                       std::vector<std::string>{"s"}}})
          .ok());

  auto m = node->get_value(props).ValueOrDie().as_map_ref();
  EXPECT_EQ(m.get_value("i32").as_int32(), 1);
  EXPECT_EQ(m.get_value("i64").as_int64(), 2);
  EXPECT_DOUBLE_EQ(m.get_value("f64").as_double(), 3.14);
  EXPECT_EQ(m.get_value("b").as_bool(), true);
  EXPECT_EQ(m.get_value("s").as_string(), "hello");
}

// =============================================================================
// 2. nested_path FieldUpdate — COW growth across multiple updates
// =============================================================================

TEST_F(NodeArenaTest, NestedPathCowGrowth) {
  auto node = mgr_->create_node("Full", {{"name", Value{"Bob"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  for (int i = 0; i < 20; ++i) {
    ASSERT_TRUE(node->update_fields(
                        {FieldUpdate{props,
                                     Value{int32_t(i)},
                                     UpdateType::SET,
                                     {std::string("k") + std::to_string(i)}}})
                    .ok());
  }

  auto m = node->get_value(props).ValueOrDie().as_map_ref();
  EXPECT_EQ(m.count(), 20u);
  EXPECT_EQ(m.get_value("k0").as_int32(), 0);
  EXPECT_EQ(m.get_value("k19").as_int32(), 19);
}

// =============================================================================
// 4. append_to_array_field — non-versioned single element
// =============================================================================

TEST_F(NodeArenaTest, AppendToArrayFieldNonVersioned) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"Alice"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto nums_field = schema->get_field("nums");

  auto res = node->update_fields(
      {FieldUpdate{nums_field, Value{int32_t(10)}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());

  auto val = node->get_value(nums_field).ValueOrDie();
  ASSERT_TRUE(val.holds_array_ref());

  // Second append (COW path — existing array)
  res = node->update_fields(
      {FieldUpdate{nums_field, Value{int32_t(20)}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());
}

// =============================================================================
// 5. append_to_array_field — non-versioned raw array (batch append)
// =============================================================================

TEST_F(NodeArenaTest, AppendRawArrayNonVersioned) {
  auto node = mgr_->create_node("Full", {{"name", Value{"Bob"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto nums_field = schema->get_field("nums");

  std::vector<Value> batch = {Value{int32_t(1)}, Value{int32_t(2)},
                              Value{int32_t(3)}};
  auto res = node->update_fields(
      {FieldUpdate{nums_field, Value{batch}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());

  // Append more to existing array
  std::vector<Value> more = {Value{int32_t(4)}};
  res = node->update_fields(
      {FieldUpdate{nums_field, Value{more}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());
}

// =============================================================================
// 6. Versioned append — prepare_append_value
// =============================================================================

TEST_F(NodeArenaTest, VersionedAppendToArrayField) {
  auto node = mgr_versioned_->create_node("Full", {{"name", Value{"Carol"}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto nums_field = schema->get_field("nums");

  // Append to null array in versioned mode
  auto res = node->update_fields(
      {FieldUpdate{nums_field, Value{int32_t(1)}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());

  // Append to existing array in versioned mode (reads from version chain)
  res = node->update_fields(
      {FieldUpdate{nums_field, Value{int32_t(2)}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());

  // Versioned raw array append
  std::vector<Value> batch = {Value{int32_t(3)}, Value{int32_t(4)}};
  res = node->update_fields(
      {FieldUpdate{nums_field, Value{batch}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());
}

// =============================================================================
// 7. set_field_value_internal — overwrite MAP field (marks old for deletion)
// =============================================================================

TEST_F(NodeArenaTest, OverwriteNestedPathValue) {
  auto node = mgr_->create_node("Full", {{"name", Value{"Dave"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(1)}, UpdateType::SET,
                                   std::vector<std::string>{"x"}}})
                  .ok());
  EXPECT_EQ(node->get_value(props)
                .ValueOrDie()
                .as_map_ref()
                .get_value("x")
                .as_int32(),
            1);

  // Overwrite the same key — COW copy and update
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(99)}, UpdateType::SET,
                                   std::vector<std::string>{"x"}}})
                  .ok());
  EXPECT_EQ(node->get_value(props)
                .ValueOrDie()
                .as_map_ref()
                .get_value("x")
                .as_int32(),
            99);
}

// =============================================================================
// 8. VersionInfo bitemporal methods (cache)
// =============================================================================

TEST_F(NodeArenaTest, VersionInfoBitemporal) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"Eve"}},
                                         {"age", Value{int32_t(20)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age_field = schema->get_field("age");

  // Create a version
  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(21)},
                                               UpdateType::SET}})
                  .ok());

  const auto* handle = node->get_handle();
  auto* vi = handle->get_version_info();
  ASSERT_NE(vi, nullptr);

  EXPECT_FALSE(vi->is_field_cached(0));
  vi->mark_field_cached(0);
  EXPECT_TRUE(vi->is_field_cached(0));
  vi->clear_cache();
  EXPECT_FALSE(vi->is_field_cached(0));
}

// =============================================================================
// 9. NodeHandle — default ctor, move assignment, equality
// =============================================================================

TEST_F(NodeArenaTest, NodeHandleDefaultCtor) {
  NodeHandle h;
  EXPECT_TRUE(h.is_null());
  EXPECT_FALSE(h.is_versioned());
}

TEST_F(NodeArenaTest, NodeHandleMoveAssignment) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"Frank"}}}).ValueOrDie();
  NodeHandle h1 = *node->get_handle();
  NodeHandle h2;
  h2 = std::move(h1);
  EXPECT_TRUE(h1.is_null());
  EXPECT_FALSE(h2.is_null());
}

TEST_F(NodeArenaTest, NodeHandleEquality) {
  auto n1 = mgr_->create_node("Full", {{"name", Value{"A"}}}).ValueOrDie();
  auto n2 = mgr_->create_node("Full", {{"name", Value{"B"}}}).ValueOrDie();

  NodeHandle h1 = *n1->get_handle();
  NodeHandle h2 = *n1->get_handle();
  NodeHandle h3 = *n2->get_handle();

  EXPECT_EQ(h1, h2);
  EXPECT_NE(h1, h3);
}

// =============================================================================
// 10. Arena accessors
// =============================================================================

TEST_F(NodeArenaTest, ArenaAccessors) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"Grace"}}}).ValueOrDie();
  auto* arena = node->get_arena();

  EXPECT_NE(arena->get_string_arena(), nullptr);
  EXPECT_NE(arena->get_array_arena(), nullptr);
  EXPECT_NE(arena->get_map_arena(), nullptr);
  EXPECT_GT(arena->get_total_allocated(), 0u);
  EXPECT_GE(arena->get_chunk_count(), 1u);
  EXPECT_NE(arena->get_mem_arena(), nullptr);
}

// =============================================================================
// 11. append_single_element — different element types
// =============================================================================

TEST_F(NodeArenaTest, AppendDifferentElementTypes) {
  auto node = mgr_->create_node("Full", {{"name", Value{"Hank"}}}).ValueOrDie();
  auto schema = node->get_schema();

  // INT32 append via nums field
  auto nums_field = schema->get_field("nums");
  ASSERT_TRUE(node->update_fields({FieldUpdate{nums_field, Value{int32_t(42)},
                                               UpdateType::APPEND}})
                  .ok());

  // STRING append via tags field
  auto tags_field = schema->get_field("tags");
  ASSERT_TRUE(node->update_fields({FieldUpdate{tags_field, Value{"hello"},
                                               UpdateType::APPEND}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{tags_field, Value{"world"},
                                               UpdateType::APPEND}})
                  .ok());
}

// =============================================================================
// 12. is_versioning_enabled
// =============================================================================

TEST_F(NodeArenaTest, VersioningEnabled) {
  auto n1 = mgr_->create_node("Full", {{"name", Value{"I"}}}).ValueOrDie();
  auto n2 =
      mgr_versioned_->create_node("Full", {{"name", Value{"J"}}}).ValueOrDie();
  EXPECT_FALSE(n1->get_arena()->is_versioning_enabled());
  EXPECT_TRUE(n2->get_arena()->is_versioning_enabled());
}

// =============================================================================
// 13. NodeArena::reset and clear
// =============================================================================

TEST_F(NodeArenaTest, ResetAndClear) {
  // Use a separate manager so we don't corrupt shared state
  auto separate_mgr = std::make_unique<NodeManager>(registry_);
  auto node =
      separate_mgr->create_node("Full", {{"name", Value{"K"}}}).ValueOrDie();
  auto* arena = node->get_arena();
  // Verify these do not crash
  arena->reset();
  arena->clear();
}

// =============================================================================
// 14. NodeHandle temporal getters
// =============================================================================

TEST_F(NodeArenaTest, NodeHandleTemporalGetters) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"Lily"}},
                                         {"age", Value{int32_t(25)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age_field = schema->get_field("age");

  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(26)},
                                               UpdateType::SET}})
                  .ok());

  const auto* handle = node->get_handle();
  uint64_t vf = handle->get_valid_from();
  uint64_t vt = handle->get_valid_to();
  EXPECT_GT(vf, 0u);
  EXPECT_EQ(vt, std::numeric_limits<uint64_t>::max());
  EXPECT_TRUE(handle->is_valid_at(vf));
  EXPECT_NE(handle->get_prev_version(), nullptr);
}

// =============================================================================
// 15. get_value_at_version — unknown field produces KeyError
// =============================================================================

TEST_F(NodeArenaTest, GetValueAtVersionNullField) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"M"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age_field = schema->get_field("age");
  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(2)},
                                               UpdateType::SET}})
                  .ok());

  const auto* handle = node->get_handle();
  auto* vi = handle->get_version_info();
  auto layout = std::make_shared<SchemaLayout>(schema);

  // Null field triggers the nullptr check in get_field_layout -> KeyError
  std::shared_ptr<Field> null_field;
  auto result =
      NodeArena::get_value_at_version(*handle, vi, layout, null_field);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsKeyError());

  // Also verify get_value_ptr_at_version returns nullptr for null field
  const char* ptr =
      NodeArena::get_value_ptr_at_version(*handle, vi, layout, null_field);
  EXPECT_EQ(ptr, nullptr);
}

// =============================================================================
// 16. VersionInfo::find_version_at_snapshot
// =============================================================================

TEST_F(NodeArenaTest, FindVersionAtSnapshot) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"N"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age_field = schema->get_field("age");

  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(2)},
                                               UpdateType::SET}})
                  .ok());

  const auto* handle = node->get_handle();
  const auto* vi = handle->get_version_info();

  // is_visible_at with valid time and tx time
  uint64_t now = vi->valid_from;
  uint64_t tx = vi->tx_from;
  EXPECT_TRUE(vi->is_visible_at(now, tx));

  // find_version_at_snapshot — should find the current version
  const auto* found = vi->find_version_at_snapshot(now, tx);
  EXPECT_NE(found, nullptr);

  // Snapshot at time 0 — all versions have timestamps > 0
  const auto* not_found = vi->find_version_at_snapshot(0, 0);
  EXPECT_EQ(not_found, nullptr);
}

// =============================================================================
// 17. Overwrite string and array fields (marks old for deletion)
// =============================================================================

TEST_F(NodeArenaTest, OverwriteStringFieldNonVersioned) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"Original"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto name_field = schema->get_field("name");

  ASSERT_TRUE(node->update_fields({FieldUpdate{name_field, Value{"Updated"},
                                               UpdateType::SET}})
                  .ok());
  auto val = node->get_value(name_field).ValueOrDie();
  EXPECT_EQ(val.as_string(), "Updated");
}

TEST_F(NodeArenaTest, OverwriteArrayFieldNonVersioned) {
  auto node = mgr_->create_node("Full", {{"name", Value{"O"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto nums_field = schema->get_field("nums");

  // Set initial array via raw array
  std::vector<Value> arr1 = {Value{int32_t(1)}};
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{nums_field, Value{arr1}, UpdateType::SET}})
                  .ok());

  // Overwrite — old array marked for deletion
  std::vector<Value> arr2 = {Value{int32_t(2)}, Value{int32_t(3)}};
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{nums_field, Value{arr2}, UpdateType::SET}})
                  .ok());
}

// =============================================================================
// Extra: version_counter advances, count_versions, find_version_at_time
// =============================================================================

TEST_F(NodeArenaTest, VersionCounterAndChain) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"Z"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age_field = schema->get_field("age");

  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(2)},
                                               UpdateType::SET}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(3)},
                                               UpdateType::SET}})
                  .ok());

  const auto* handle = node->get_handle();
  EXPECT_EQ(handle->count_versions(), 3u);  // v0 + v1 + v2
  EXPECT_GT(handle->get_version_id(), 0u);

  // find_version_at_time for the latest version's valid_from
  uint64_t vf = handle->get_valid_from();
  const auto* v = handle->find_version_at_time(vf);
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->valid_from, vf);

  // Non-versioned handle returns null for find_version_at_time
  auto nv_node =
      mgr_->create_node("Full", {{"name", Value{"NV"}}}).ValueOrDie();
  EXPECT_EQ(nv_node->get_handle()->find_version_at_time(0), nullptr);
  EXPECT_EQ(nv_node->get_handle()->get_prev_version(), nullptr);
  EXPECT_EQ(nv_node->get_handle()->count_versions(), 1u);
}

// =============================================================================
// End-to-end: nested_path update via Node::update_fields (versioned)
// =============================================================================

TEST_F(NodeArenaTest, VersionedNestedPathUpdate) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"MapVer"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");
  auto age = schema->get_field("age");

  // v1: set props.score = 3.14
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{3.14}, UpdateType::SET,
                                       std::vector<std::string>{"score"}}})
          .ok());

  auto m1 = node->get_value(props).ValueOrDie().as_map_ref();
  EXPECT_DOUBLE_EQ(m1.get_value("score").as_double(), 3.14);

  // v2: update props.score = 6.28 and age = 2 in a single batch
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{props, Value{6.28}, UpdateType::SET,
                           std::vector<std::string>{"score"}},
               FieldUpdate{age, Value{int32_t(2)}, UpdateType::SET, {}}})
          .ok());

  auto m2 = node->get_value(props).ValueOrDie().as_map_ref();
  EXPECT_DOUBLE_EQ(m2.get_value("score").as_double(), 6.28);
  EXPECT_EQ(node->get_value(age).ValueOrDie().as_int32(), 2);

  // v3: add a new key to the map
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{true}, UpdateType::SET,
                                       std::vector<std::string>{"active"}}})
          .ok());

  auto m3 = node->get_value(props).ValueOrDie().as_map_ref();
  EXPECT_EQ(m3.count(), 2u);
  EXPECT_DOUBLE_EQ(m3.get_value("score").as_double(), 6.28);
  EXPECT_EQ(m3.get_value("active").as_bool(), true);
}

// =============================================================================
// End-to-end: nested_path update on null MAP field creates map automatically
// =============================================================================

TEST_F(NodeArenaTest, NestedPathOnNullFieldCreatesMap) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"NullMap"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  // MAP field is null initially — nested_path update should create it
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(42)}, UpdateType::SET,
                                   std::vector<std::string>{"answer"}}})
                  .ok());

  auto val = node->get_value(props).ValueOrDie();
  ASSERT_TRUE(val.holds_map_ref());
  EXPECT_EQ(val.as_map_ref().get_value("answer").as_int32(), 42);
}

TEST_F(NodeArenaTest, NestedPathDepthGreaterThanOneReturnsNotImplemented) {
  auto node =
      mgr_->create_node("Full", {{"name", Value{"Nested"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  auto update_result = node->update_fields(
      {FieldUpdate{props, Value{int32_t(1)}, UpdateType::SET,
                   std::vector<std::string>{"lvl1", "lvl2"}}});
  ASSERT_FALSE(update_result.ok());
  EXPECT_TRUE(update_result.status().IsNotImplemented());
}

// =============================================================================
// 18. append_single_element — INT64, DOUBLE, BOOL element types
// =============================================================================

TEST_F(NodeArenaTest, AppendInt64Elements) {
  auto node = mgr_->create_node("Full", {{"name", Value{"I64"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto i64_field = schema->get_field("i64_arr");

  ASSERT_TRUE(node->update_fields({FieldUpdate{i64_field, Value{int64_t(100)},
                                               UpdateType::APPEND}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{i64_field, Value{int64_t(200)},
                                               UpdateType::APPEND}})
                  .ok());

  auto val = node->get_value(i64_field).ValueOrDie();
  ASSERT_TRUE(val.holds_array_ref());
}

TEST_F(NodeArenaTest, AppendDoubleElements) {
  auto node = mgr_->create_node("Full", {{"name", Value{"F64"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto f64_field = schema->get_field("f64_arr");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{f64_field, Value{1.11}, UpdateType::APPEND}})
                  .ok());
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{f64_field, Value{2.22}, UpdateType::APPEND}})
                  .ok());

  auto val = node->get_value(f64_field).ValueOrDie();
  ASSERT_TRUE(val.holds_array_ref());
}

TEST_F(NodeArenaTest, AppendBoolElements) {
  auto node = mgr_->create_node("Full", {{"name", Value{"Bool"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto bool_field = schema->get_field("bool_arr");

  ASSERT_TRUE(node->update_fields({FieldUpdate{bool_field, Value{true},
                                               UpdateType::APPEND}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{bool_field, Value{false},
                                               UpdateType::APPEND}})
                  .ok());

  auto val = node->get_value(bool_field).ValueOrDie();
  ASSERT_TRUE(val.holds_array_ref());
}

TEST_F(NodeArenaTest, AppendStringElements) {
  auto node = mgr_->create_node("Full", {{"name", Value{"Str"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto tags_field = schema->get_field("tags");

  ASSERT_TRUE(node->update_fields({FieldUpdate{tags_field, Value{"alpha"},
                                               UpdateType::APPEND}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{tags_field, Value{"beta"},
                                               UpdateType::APPEND}})
                  .ok());

  auto val = node->get_value(tags_field).ValueOrDie();
  ASSERT_TRUE(val.holds_array_ref());
}

// =============================================================================
// 19. Versioned append for INT64, DOUBLE, BOOL arrays
// =============================================================================

TEST_F(NodeArenaTest, VersionedAppendInt64) {
  auto node = mgr_versioned_->create_node("Full", {{"name", Value{"VI64"}}})
                  .ValueOrDie();
  auto f = node->get_schema()->get_field("i64_arr");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{f, Value{int64_t(1)}, UpdateType::APPEND}})
                  .ok());
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{f, Value{int64_t(2)}, UpdateType::APPEND}})
                  .ok());
}

TEST_F(NodeArenaTest, VersionedAppendDouble) {
  auto node = mgr_versioned_->create_node("Full", {{"name", Value{"VF64"}}})
                  .ValueOrDie();
  auto f = node->get_schema()->get_field("f64_arr");

  ASSERT_TRUE(
      node->update_fields({FieldUpdate{f, Value{1.5}, UpdateType::APPEND}})
          .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{f, Value{2.5}, UpdateType::APPEND}})
          .ok());
}

TEST_F(NodeArenaTest, VersionedAppendBool) {
  auto node = mgr_versioned_->create_node("Full", {{"name", Value{"VBool"}}})
                  .ValueOrDie();
  auto f = node->get_schema()->get_field("bool_arr");

  ASSERT_TRUE(
      node->update_fields({FieldUpdate{f, Value{true}, UpdateType::APPEND}})
          .ok());
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{f, Value{false}, UpdateType::APPEND}})
          .ok());
}

// =============================================================================
// 20. NodeArena::allocate_map
// =============================================================================

TEST_F(NodeArenaTest, AllocateMapDirect) {
  auto node = mgr_->create_node("Full", {{"name", Value{"AM"}}}).ValueOrDie();
  auto* arena = node->get_arena();

  auto res = arena->allocate_map();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);
  EXPECT_FALSE(ref.is_null());
}

// =============================================================================
// 21. node_arena_factory::create_simple_arena
// =============================================================================

TEST_F(NodeArenaTest, CreateSimpleArenaFactory) {
  auto layout_reg = std::make_shared<LayoutRegistry>();
  auto arena =
      node_arena_factory::create_simple_arena(layout_reg, 64 * 1024, false);
  EXPECT_NE(arena, nullptr);
  EXPECT_FALSE(arena->is_versioning_enabled());
}

// =============================================================================
// 22. Overwrite MAP field entirely (set_field_value_internal mark old map)
// =============================================================================

TEST_F(NodeArenaTest, OverwriteEntireMapField) {
  auto node = mgr_->create_node("Full", {{"name", Value{"OMF"}}}).ValueOrDie();
  auto schema = node->get_schema();
  auto props = schema->get_field("props");

  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props, Value{int32_t(1)}, UpdateType::SET,
                                   std::vector<std::string>{"a"}}})
                  .ok());

  auto* arena = node->get_arena();
  auto new_map = arena->allocate_map().ValueOrDie();
  ASSERT_TRUE(
      node->update_fields({FieldUpdate{props, Value{std::move(new_map)}}})
          .ok());

  auto val = node->get_value(props).ValueOrDie();
  ASSERT_TRUE(val.holds_map_ref());
  EXPECT_EQ(val.as_map_ref().count(), 0u);
}

// =============================================================================
// 23. VersionInfo::find_version_at_time returning nullptr (no match)
// =============================================================================

TEST_F(NodeArenaTest, FindVersionAtTimeNoMatch) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"NoMatch"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  auto handle = node->get_handle();
  const auto* vi = handle->get_version_info();

  const auto* found = vi->find_version_at_time(0);
  EXPECT_EQ(found, nullptr);
}

// =============================================================================
// 24. NodeHandle::set_version_info
// =============================================================================

TEST_F(NodeArenaTest, SetVersionInfo) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"SVI"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  NodeHandle h = *node->get_handle();
  auto* original_vi = h.get_version_info();
  ASSERT_NE(original_vi, nullptr);

  VersionInfo dummy_vi;
  h.set_version_info(&dummy_vi);
  EXPECT_EQ(h.get_version_info(), &dummy_vi);

  h.set_version_info(original_vi);
}

// =============================================================================
// 25. Versioned empty updates shortcut (line 567-569)
// =============================================================================

TEST_F(NodeArenaTest, VersionedEmptyUpdatesNoOp) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"Empty"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();

  size_t versions_before = node->get_handle()->count_versions();
  auto res = node->update_fields({});
  ASSERT_TRUE(res.ok());
  EXPECT_EQ(node->get_handle()->count_versions(), versions_before);
}

// =============================================================================
// 26. get_value on versioned node — explicit NULL in version chain
// =============================================================================

TEST_F(NodeArenaTest, GetValueVersionedExplicitNull) {
  auto node = mgr_versioned_
                  ->create_node("Full", {{"name", Value{"NullV"}},
                                         {"age", Value{int32_t(1)}}})
                  .ValueOrDie();
  auto schema = node->get_schema();
  auto age_field = schema->get_field("age");

  ASSERT_TRUE(
      node->update_fields({FieldUpdate{age_field, Value{}, UpdateType::SET}})
          .ok());

  auto val = node->get_value(age_field).ValueOrDie();
  EXPECT_TRUE(val.is_null());
}

// =============================================================================
// 27. Versioned raw-array append with empty batch
// =============================================================================

TEST_F(NodeArenaTest, VersionedAppendEmptyBatch) {
  auto node =
      mgr_versioned_->create_node("Full", {{"name", Value{"EB"}}}).ValueOrDie();
  auto nums_field = node->get_schema()->get_field("nums");

  std::vector<Value> empty;
  auto res = node->update_fields(
      {FieldUpdate{nums_field, Value{empty}, UpdateType::APPEND}});
  ASSERT_TRUE(res.ok());
}

TEST_F(NodeArenaTest, VersionedAppendEmptyBatchToExistingArray) {
  auto node = mgr_versioned_->create_node("Full", {{"name", Value{"EB2"}}})
                  .ValueOrDie();
  auto nums_field = node->get_schema()->get_field("nums");

  ASSERT_TRUE(node->update_fields({FieldUpdate{nums_field, Value{int32_t(1)},
                                               UpdateType::APPEND}})
                  .ok());

  std::vector<Value> empty;
  ASSERT_TRUE(node->update_fields({FieldUpdate{nums_field, Value{empty},
                                               UpdateType::APPEND}})
                  .ok());
}
