#include "../include/map_arena.hpp"

#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "../include/array_arena.hpp"
#include "common/clock.hpp"
#include "../include/edge.hpp"
#include "../include/node.hpp"
#include "../include/node_arena.hpp"
#include "../include/schema.hpp"
#include "../include/schema_layout.hpp"
#include "common/types.hpp"
#include "edge_store.hpp"

using namespace tundradb;

// ============================================================================
// MapArena unit tests
// ============================================================================

class MapArenaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    map_arena_ = std::make_unique<MapArena>();
    string_arena_ = std::make_unique<StringArena>();
  }

  void TearDown() override {
    map_arena_.reset();
    string_arena_.reset();
  }

  std::unique_ptr<MapArena> map_arena_;
  std::unique_ptr<StringArena> string_arena_;
};

TEST_F(MapArenaTest, AllocateEmpty) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);
  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.count(), 0);
  EXPECT_EQ(ref.capacity(), MapArena::DEFAULT_CAPACITY);
  EXPECT_EQ(ref.get_ref_count(), 1);
  EXPECT_EQ(map_arena_->get_active_allocs(), 1);
}

TEST_F(MapArenaTest, AllocateZeroCapacity) {
  auto res = map_arena_->allocate(0);
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);
  EXPECT_TRUE(ref.is_null());
}

TEST_F(MapArenaTest, SetAndGetPrimitiveEntries) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key_a = string_arena_->store_string_auto("a").ValueOrDie();
  auto key_b = string_arena_->store_string_auto("b").ValueOrDie();
  auto key_c = string_arena_->store_string_auto("c").ValueOrDie();

  int32_t val_a = 42;
  double val_b = 3.14;
  bool val_c = true;

  ASSERT_TRUE(MapArena::set_entry(ref, key_a, ValueType::INT32, &val_a).ok());
  ASSERT_TRUE(MapArena::set_entry(ref, key_b, ValueType::DOUBLE, &val_b).ok());
  ASSERT_TRUE(MapArena::set_entry(ref, key_c, ValueType::BOOL, &val_c).ok());

  EXPECT_EQ(ref.count(), 3);

  EXPECT_EQ(ref.get_value("a").as_int32(), 42);
  EXPECT_DOUBLE_EQ(ref.get_value("b").as_double(), 3.14);
  EXPECT_EQ(ref.get_value("c").as_bool(), true);
  EXPECT_TRUE(ref.get_value("missing").is_null());
}

TEST_F(MapArenaTest, ContainsKey) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("x").ValueOrDie();
  int32_t val = 1;
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::INT32, &val).ok());

  EXPECT_TRUE(ref.contains("x"));
  EXPECT_FALSE(ref.contains("y"));
}

TEST_F(MapArenaTest, OverwriteExistingEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("k").ValueOrDie();
  int32_t v1 = 10;
  int32_t v2 = 20;

  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::INT32, &v1).ok());
  EXPECT_EQ(ref.get_value("k").as_int32(), 10);
  EXPECT_EQ(ref.count(), 1);

  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::INT32, &v2).ok());
  EXPECT_EQ(ref.get_value("k").as_int32(), 20);
  EXPECT_EQ(ref.count(), 1);
}

TEST_F(MapArenaTest, RemoveEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key_a = string_arena_->store_string_auto("a").ValueOrDie();
  auto key_b = string_arena_->store_string_auto("b").ValueOrDie();
  int32_t v1 = 1, v2 = 2;

  MapArena::set_entry(ref, key_a, ValueType::INT32, &v1);
  MapArena::set_entry(ref, key_b, ValueType::INT32, &v2);
  EXPECT_EQ(ref.count(), 2);

  EXPECT_TRUE(MapArena::remove_entry(ref, "a"));
  EXPECT_EQ(ref.count(), 1);
  EXPECT_TRUE(ref.get_value("a").is_null());
  EXPECT_EQ(ref.get_value("b").as_int32(), 2);

  EXPECT_FALSE(MapArena::remove_entry(ref, "nonexistent"));
}

TEST_F(MapArenaTest, CopyCreatesIndependentMap) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef original = std::move(*res);

  auto key = string_arena_->store_string_auto("x").ValueOrDie();
  int32_t val = 99;
  MapArena::set_entry(original, key, ValueType::INT32, &val);

  auto copy_res = map_arena_->copy(original);
  ASSERT_TRUE(copy_res.ok());
  MapRef copy = std::move(*copy_res);

  EXPECT_EQ(copy.count(), 1);
  EXPECT_EQ(copy.get_value("x").as_int32(), 99);
  EXPECT_EQ(map_arena_->get_active_allocs(), 2);

  // Modify original — copy should be unaffected
  int32_t new_val = 100;
  MapArena::set_entry(original, key, ValueType::INT32, &new_val);
  EXPECT_EQ(original.get_value("x").as_int32(), 100);
  EXPECT_EQ(copy.get_value("x").as_int32(), 99);
}

TEST_F(MapArenaTest, COWGrowthOnCapacityExhaustion) {
  auto res = map_arena_->allocate(2);
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);
  EXPECT_EQ(ref.capacity(), 2);

  auto key_a = string_arena_->store_string_auto("a").ValueOrDie();
  auto key_b = string_arena_->store_string_auto("b").ValueOrDie();
  int32_t v1 = 1, v2 = 2;

  MapArena::set_entry(ref, key_a, ValueType::INT32, &v1);
  MapArena::set_entry(ref, key_b, ValueType::INT32, &v2);
  EXPECT_EQ(ref.count(), 2);

  // Third entry should fail with CapacityError
  auto key_c = string_arena_->store_string_auto("c").ValueOrDie();
  int32_t v3 = 3;
  auto status = MapArena::set_entry(ref, key_c, ValueType::INT32, &v3);
  EXPECT_TRUE(status.IsCapacityError());

  // COW grow
  auto grown_res = map_arena_->copy(ref, ref.capacity());
  ASSERT_TRUE(grown_res.ok());
  map_arena_->mark_for_deletion(ref);
  ref = std::move(*grown_res);

  EXPECT_GE(ref.capacity(), 4);
  ASSERT_TRUE(MapArena::set_entry(ref, key_c, ValueType::INT32, &v3).ok());
  EXPECT_EQ(ref.count(), 3);
  EXPECT_EQ(ref.get_value("c").as_int32(), 3);
}

TEST_F(MapArenaTest, RefCountingCopyAndDestruct) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref1 = std::move(*res);
  EXPECT_EQ(ref1.get_ref_count(), 1);

  {
    MapRef ref2 = ref1;  // copy constructor
    EXPECT_EQ(ref1.get_ref_count(), 2);
    EXPECT_EQ(ref2.get_ref_count(), 2);
  }
  // ref2 destroyed, ref_count back to 1
  EXPECT_EQ(ref1.get_ref_count(), 1);
}

TEST_F(MapArenaTest, StringValueInEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("name").ValueOrDie();
  auto val = string_arena_->store_string_auto("Alice").ValueOrDie();

  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::STRING, &val).ok());

  Value result = ref.get_value("name");
  EXPECT_EQ(result.type(), ValueType::STRING);
  EXPECT_EQ(result.as_string(), "Alice");
}

TEST_F(MapArenaTest, MarkForDeletionAndRelease) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);
  EXPECT_EQ(map_arena_->get_active_allocs(), 1);

  map_arena_->mark_for_deletion(ref);
  EXPECT_TRUE(ref.is_marked_for_deletion());

  // Dropping the last ref should free the allocation
  ref = MapRef{};
  EXPECT_EQ(map_arena_->get_active_allocs(), 0);
}

// ============================================================================
// Ref-counted value destruction & copy (memory safety critical paths)
// ============================================================================

TEST_F(MapArenaTest, OverwriteStringEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("name").ValueOrDie();
  auto val1 = string_arena_->store_string_auto("Alice").ValueOrDie();
  auto val2 = string_arena_->store_string_auto("Bob").ValueOrDie();

  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::STRING, &val1).ok());
  EXPECT_EQ(ref.get_value("name").as_string(), "Alice");

  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::STRING, &val2).ok());
  EXPECT_EQ(ref.get_value("name").as_string(), "Bob");
  EXPECT_EQ(ref.count(), 1);
}

TEST_F(MapArenaTest, RemoveStringEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("s").ValueOrDie();
  auto val = string_arena_->store_string_auto("hello").ValueOrDie();
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::STRING, &val).ok());
  EXPECT_EQ(ref.count(), 1);

  EXPECT_TRUE(MapArena::remove_entry(ref, "s"));
  EXPECT_EQ(ref.count(), 0);
  EXPECT_TRUE(ref.get_value("s").is_null());
}

TEST_F(MapArenaTest, OverwriteStringWithDifferentType) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("val").ValueOrDie();
  auto str_val = string_arena_->store_string_auto("text").ValueOrDie();
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::STRING, &str_val).ok());
  EXPECT_EQ(ref.get_value("val").as_string(), "text");

  int32_t int_val = 42;
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::INT32, &int_val).ok());
  EXPECT_EQ(ref.get_value("val").as_int32(), 42);
  EXPECT_EQ(ref.count(), 1);
}

TEST_F(MapArenaTest, ArrayRefValueInEntry) {
  ArrayArena array_arena;
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  int32_t elems[] = {10, 20, 30};
  auto arr_res = array_arena.allocate_with_data(ValueType::INT32, elems, 3);
  ASSERT_TRUE(arr_res.ok());
  ArrayRef arr = std::move(*arr_res);

  auto key = string_arena_->store_string_auto("nums").ValueOrDie();
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::ARRAY, &arr).ok());

  Value result = ref.get_value("nums");
  EXPECT_EQ(result.type(), ValueType::ARRAY);
}

TEST_F(MapArenaTest, OverwriteArrayRefEntry) {
  ArrayArena array_arena;
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("arr").ValueOrDie();

  int32_t elems1[] = {1, 2};
  auto arr1 =
      array_arena.allocate_with_data(ValueType::INT32, elems1, 2).ValueOrDie();
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::ARRAY, &arr1).ok());

  int32_t elems2[] = {3, 4, 5};
  auto arr2 =
      array_arena.allocate_with_data(ValueType::INT32, elems2, 3).ValueOrDie();
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::ARRAY, &arr2).ok());
  EXPECT_EQ(ref.count(), 1);
}

TEST_F(MapArenaTest, RemoveArrayRefEntry) {
  ArrayArena array_arena;
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  auto key = string_arena_->store_string_auto("arr").ValueOrDie();
  int32_t elems[] = {1};
  auto arr =
      array_arena.allocate_with_data(ValueType::INT32, elems, 1).ValueOrDie();
  ASSERT_TRUE(MapArena::set_entry(ref, key, ValueType::ARRAY, &arr).ok());

  EXPECT_TRUE(MapArena::remove_entry(ref, "arr"));
  EXPECT_EQ(ref.count(), 0);
}

TEST_F(MapArenaTest, NestedMapRefValueInEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef outer = std::move(*res);

  auto inner_res = map_arena_->allocate();
  ASSERT_TRUE(inner_res.ok());
  MapRef inner = std::move(*inner_res);

  auto inner_key = string_arena_->store_string_auto("x").ValueOrDie();
  int32_t val = 99;
  MapArena::set_entry(inner, inner_key, ValueType::INT32, &val);

  auto outer_key = string_arena_->store_string_auto("nested").ValueOrDie();
  ASSERT_TRUE(
      MapArena::set_entry(outer, outer_key, ValueType::MAP, &inner).ok());

  Value result = outer.get_value("nested");
  EXPECT_TRUE(result.holds_map_ref());
  EXPECT_EQ(result.as_map_ref().get_value("x").as_int32(), 99);
}

TEST_F(MapArenaTest, OverwriteNestedMapRefEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef outer = std::move(*res);

  auto inner1_res = map_arena_->allocate();
  auto inner2_res = map_arena_->allocate();
  MapRef inner1 = std::move(*inner1_res);
  MapRef inner2 = std::move(*inner2_res);

  auto k1 = string_arena_->store_string_auto("a").ValueOrDie();
  auto k2 = string_arena_->store_string_auto("b").ValueOrDie();
  int32_t v1 = 1, v2 = 2;
  MapArena::set_entry(inner1, k1, ValueType::INT32, &v1);
  MapArena::set_entry(inner2, k2, ValueType::INT32, &v2);

  auto outer_key = string_arena_->store_string_auto("child").ValueOrDie();
  ASSERT_TRUE(
      MapArena::set_entry(outer, outer_key, ValueType::MAP, &inner1).ok());
  ASSERT_TRUE(
      MapArena::set_entry(outer, outer_key, ValueType::MAP, &inner2).ok());

  Value result = outer.get_value("child");
  EXPECT_EQ(result.as_map_ref().get_value("b").as_int32(), 2);
  EXPECT_TRUE(result.as_map_ref().get_value("a").is_null());
}

TEST_F(MapArenaTest, RemoveNestedMapRefEntry) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef outer = std::move(*res);

  auto inner_res = map_arena_->allocate();
  MapRef inner = std::move(*inner_res);

  auto outer_key = string_arena_->store_string_auto("child").ValueOrDie();
  ASSERT_TRUE(
      MapArena::set_entry(outer, outer_key, ValueType::MAP, &inner).ok());

  EXPECT_TRUE(MapArena::remove_entry(outer, "child"));
  EXPECT_EQ(outer.count(), 0);
}

TEST_F(MapArenaTest, CopyMapWithStringValues) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef original = std::move(*res);

  auto key = string_arena_->store_string_auto("name").ValueOrDie();
  auto val = string_arena_->store_string_auto("Alice").ValueOrDie();
  MapArena::set_entry(original, key, ValueType::STRING, &val);

  auto copy_res = map_arena_->copy(original);
  ASSERT_TRUE(copy_res.ok());
  MapRef copy = std::move(*copy_res);

  EXPECT_EQ(copy.get_value("name").as_string(), "Alice");
  EXPECT_EQ(copy.count(), 1);
}

TEST_F(MapArenaTest, CopyMapWithArrayValues) {
  ArrayArena array_arena;
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef original = std::move(*res);

  int32_t elems[] = {1, 2, 3};
  auto arr =
      array_arena.allocate_with_data(ValueType::INT32, elems, 3).ValueOrDie();

  auto key = string_arena_->store_string_auto("nums").ValueOrDie();
  MapArena::set_entry(original, key, ValueType::ARRAY, &arr);

  auto copy_res = map_arena_->copy(original);
  ASSERT_TRUE(copy_res.ok());
  MapRef copy = std::move(*copy_res);

  EXPECT_EQ(copy.count(), 1);
  Value v = copy.get_value("nums");
  EXPECT_EQ(v.type(), ValueType::ARRAY);
}

TEST_F(MapArenaTest, CopyMapWithNestedMapValues) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef outer = std::move(*res);

  auto inner_res = map_arena_->allocate();
  MapRef inner = std::move(*inner_res);
  auto ik = string_arena_->store_string_auto("x").ValueOrDie();
  int32_t iv = 42;
  MapArena::set_entry(inner, ik, ValueType::INT32, &iv);

  auto ok = string_arena_->store_string_auto("nested").ValueOrDie();
  MapArena::set_entry(outer, ok, ValueType::MAP, &inner);

  auto copy_res = map_arena_->copy(outer);
  ASSERT_TRUE(copy_res.ok());
  MapRef copy = std::move(*copy_res);

  Value v = copy.get_value("nested");
  EXPECT_TRUE(v.holds_map_ref());
  EXPECT_EQ(v.as_map_ref().get_value("x").as_int32(), 42);
}

// ============================================================================
// Statistics & lifecycle
// ============================================================================

TEST_F(MapArenaTest, StatsAfterAllocations) {
  size_t initial_total = map_arena_->get_total_allocated();
  size_t initial_used = map_arena_->get_used_bytes();
  EXPECT_EQ(map_arena_->get_freed_bytes(), 0u);

  auto ref1 = map_arena_->allocate().ValueOrDie();
  EXPECT_GE(map_arena_->get_total_allocated(), initial_total);
  EXPECT_GT(map_arena_->get_used_bytes(), initial_used);

  auto ref2 = map_arena_->allocate().ValueOrDie();
  size_t used_after_two = map_arena_->get_used_bytes();
  EXPECT_GT(used_after_two, 0u);

  map_arena_->mark_for_deletion(ref1);
  ref1 = MapRef{};
  EXPECT_GT(map_arena_->get_freed_bytes(), 0u);
}

TEST_F(MapArenaTest, ResetClearsState) {
  {
    auto ref = map_arena_->allocate().ValueOrDie();
    EXPECT_GT(map_arena_->get_used_bytes(), 0u);
  }
  map_arena_->reset();
  EXPECT_EQ(map_arena_->get_used_bytes(), 0u);
}

TEST_F(MapArenaTest, ClearDeallocatesMemory) {
  {
    auto ref = map_arena_->allocate().ValueOrDie();
    EXPECT_GT(map_arena_->get_total_allocated(), 0u);
  }
  map_arena_->clear();
  EXPECT_EQ(map_arena_->get_used_bytes(), 0u);
}

// ============================================================================
// MapRef API coverage
// ============================================================================

TEST_F(MapArenaTest, EmptyAndHasCapacity) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);

  EXPECT_TRUE(ref.empty());
  EXPECT_TRUE(ref.has_capacity());

  auto key = string_arena_->store_string_auto("k").ValueOrDie();
  int32_t val = 1;
  MapArena::set_entry(ref, key, ValueType::INT32, &val);

  EXPECT_FALSE(ref.empty());
}

TEST_F(MapArenaTest, EqualityOperators) {
  auto res1 = map_arena_->allocate();
  auto res2 = map_arena_->allocate();
  ASSERT_TRUE(res1.ok());
  ASSERT_TRUE(res2.ok());
  MapRef a = std::move(*res1);
  MapRef b = std::move(*res2);

  // Both empty — different allocations but same content
  EXPECT_EQ(a, b);

  auto key = string_arena_->store_string_auto("k").ValueOrDie();
  int32_t val = 1;
  MapArena::set_entry(a, key, ValueType::INT32, &val);

  EXPECT_NE(a, b);

  // Null comparisons
  MapRef null1;
  MapRef null2;
  EXPECT_EQ(null1, null2);
  EXPECT_NE(null1, a);
}

TEST_F(MapArenaTest, SelfEquality) {
  auto res = map_arena_->allocate();
  ASSERT_TRUE(res.ok());
  MapRef ref = std::move(*res);
  EXPECT_EQ(ref, ref);
}

// ============================================================================
// Node with MAP field tests
// ============================================================================

class NodeMapTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_registry_ = std::make_shared<SchemaRegistry>();

    auto arrow_fields = std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("name", arrow::utf8(), false),
        arrow::field("age", arrow::int32(), true),
        arrow::field("props", arrow::map(arrow::utf8(), arrow::binary()),
                     true)};
    auto arrow_schema = arrow::schema(arrow_fields);
    ASSERT_TRUE(schema_registry_->create("UserWithMap", arrow_schema).ok());

    node_manager_ = std::make_unique<NodeManager>(schema_registry_);
  }

  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::unique_ptr<NodeManager> node_manager_;
};

TEST_F(NodeMapTest, CreateNodeWithMapField) {
  std::unordered_map<std::string, Value> data = {{"name", Value{"Alice"}},
                                                 {"age", Value{int32_t(30)}}};

  auto node_res = node_manager_->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok()) << node_res.status().ToString();
  auto node = node_res.ValueOrDie();

  auto schema = node->get_schema();
  auto props_field = schema->get_field("props");
  ASSERT_NE(props_field, nullptr);
  EXPECT_EQ(props_field->type(), ValueType::MAP);

  // The MAP field should exist but be empty/null initially
  auto val_res = node->get_value(props_field);
  ASSERT_TRUE(val_res.ok());
}

TEST_F(NodeMapTest, SetAndGetMapField) {
  std::unordered_map<std::string, Value> data = {{"name", Value{"Bob"}},
                                                 {"age", Value{int32_t(25)}}};

  auto node_res = node_manager_->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok()) << node_res.status().ToString();
  auto node = node_res.ValueOrDie();

  auto schema = node->get_schema();
  auto props_field = schema->get_field("props");

  // Use nested_path FieldUpdate to set individual keys inside the MAP field
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props_field, Value{3.14}, UpdateType::SET,
                                   std::vector<std::string>{"score"}}})
                  .ok());
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props_field, Value{true}, UpdateType::SET,
                                   std::vector<std::string>{"active"}}})
                  .ok());

  auto val_res = node->get_value(props_field);
  ASSERT_TRUE(val_res.ok());
  Value v = val_res.ValueOrDie();
  ASSERT_TRUE(v.holds_map_ref());

  const MapRef& read_map = v.as_map_ref();
  EXPECT_EQ(read_map.count(), 2);
  EXPECT_DOUBLE_EQ(read_map.get_value("score").as_double(), 3.14);
  EXPECT_EQ(read_map.get_value("active").as_bool(), true);
  EXPECT_TRUE(read_map.get_value("missing").is_null());
}

TEST_F(NodeMapTest, SetMapKeyViFieldUpdate) {
  std::unordered_map<std::string, Value> data = {{"name", Value{"MapKey"}},
                                                 {"age", Value{int32_t(1)}}};
  auto node_res = node_manager_->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok()) << node_res.status().ToString();
  auto node = node_res.ValueOrDie();

  auto schema = node->get_schema();
  auto props_field = schema->get_field("props");

  // Set a key when MAP field is null (creates a new map)
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{props_field, Value{int32_t(42)}, UpdateType::SET,
                           std::vector<std::string>{"slot"}}})
          .ok());

  auto val = node->get_value(props_field).ValueOrDie();
  ASSERT_TRUE(val.holds_map_ref());
  EXPECT_EQ(val.as_map_ref().get_value("slot").as_int32(), 42);
}

TEST_F(NodeMapTest, SetMapKeyAllPrimitiveTypes) {
  std::unordered_map<std::string, Value> data = {{"name", Value{"Types"}},
                                                 {"age", Value{int32_t(1)}}};
  auto node_res = node_manager_->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok());
  auto node = node_res.ValueOrDie();

  auto schema = node->get_schema();
  auto props_field = schema->get_field("props");

  // Set each primitive type via nested_path FieldUpdate
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{props_field, Value{int32_t(7)}, UpdateType::SET,
                           std::vector<std::string>{"i32"}}})
          .ok());
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{props_field, Value{int64_t(9007199254740992LL)},
                           UpdateType::SET, std::vector<std::string>{"i64"}}})
          .ok());
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{props_field, Value{double(2.718)}, UpdateType::SET,
                           std::vector<std::string>{"dbl"}}})
          .ok());
  ASSERT_TRUE(
      node->update_fields(
              {FieldUpdate{props_field, Value{float(1.25f)}, UpdateType::SET,
                           std::vector<std::string>{"flt"}}})
          .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{props_field, Value{bool(false)},
                                               UpdateType::SET,
                                               std::vector<std::string>{"ok"}}})
                  .ok());
  ASSERT_TRUE(node->update_fields(
                      {FieldUpdate{props_field, Value{"text"}, UpdateType::SET,
                                   std::vector<std::string>{"s"}}})
                  .ok());

  auto val = node->get_value(props_field).ValueOrDie();
  ASSERT_TRUE(val.holds_map_ref());
  const auto& m = val.as_map_ref();
  EXPECT_EQ(m.get_value("i32").as_int32(), 7);
  EXPECT_EQ(m.get_value("i64").as_int64(), 9007199254740992LL);
  EXPECT_DOUBLE_EQ(m.get_value("dbl").as_double(), 2.718);
  EXPECT_FLOAT_EQ(m.get_value("flt").as_float(), 1.25f);
  EXPECT_EQ(m.get_value("ok").as_bool(), false);
  EXPECT_EQ(m.get_value("s").as_string(), "text");
}

TEST_F(NodeMapTest, MapKeyCowGrowthWhenMapFull) {
  std::unordered_map<std::string, Value> data = {{"name", Value{"Cow"}},
                                                 {"age", Value{int32_t(1)}}};
  auto node_res = node_manager_->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok());
  auto node = node_res.ValueOrDie();

  auto schema = node->get_schema();
  auto props_field = schema->get_field("props");

  // Set 3 keys one by one — each COW-copies the map to add a key
  ASSERT_TRUE(node->update_fields({FieldUpdate{props_field, Value{int32_t(1)},
                                               UpdateType::SET,
                                               std::vector<std::string>{"a"}}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{props_field, Value{int32_t(2)},
                                               UpdateType::SET,
                                               std::vector<std::string>{"b"}}})
                  .ok());
  ASSERT_TRUE(node->update_fields({FieldUpdate{props_field, Value{int32_t(3)},
                                               UpdateType::SET,
                                               std::vector<std::string>{"c"}}})
                  .ok());

  auto val = node->get_value(props_field).ValueOrDie();
  ASSERT_TRUE(val.holds_map_ref());
  EXPECT_EQ(val.as_map_ref().count(), 3u);
  EXPECT_EQ(val.as_map_ref().get_value("c").as_int32(), 3);
}

TEST_F(NodeMapTest, NodeHandleTemporalValidity) {
  MockClock mock;
  Clock::set_instance(&mock);
  mock.set_time(1000);

  auto mgr = std::make_unique<NodeManager>(schema_registry_, true, true, true);
  std::unordered_map<std::string, Value> data = {{"name", Value{"Zoe"}},
                                                 {"age", Value{int32_t(20)}}};
  auto node_res = mgr->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok());
  auto node = node_res.ValueOrDie();

  const NodeHandle* h = node->get_handle();
  ASSERT_TRUE(h->is_versioned());
  EXPECT_EQ(h->get_valid_from(), 1000u);
  EXPECT_EQ(h->get_valid_to(), std::numeric_limits<uint64_t>::max());
  EXPECT_TRUE(h->is_valid_at(1000));
  EXPECT_FALSE(h->is_valid_at(999));

  mock.set_time(2000);
  auto age_field = node->get_schema()->get_field("age");
  ASSERT_TRUE(node->update_fields({FieldUpdate{age_field, Value{int32_t(21)},
                                               UpdateType::SET}})
                  .ok());

  EXPECT_EQ(h->get_valid_from(), 2000u);
  EXPECT_EQ(h->get_valid_to(), std::numeric_limits<uint64_t>::max());
  EXPECT_TRUE(h->is_valid_at(2000));
  EXPECT_FALSE(h->is_valid_at(1999));

  const VersionInfo* prev = h->get_prev_version();
  ASSERT_NE(prev, nullptr);
  EXPECT_TRUE(prev->is_valid_at(1500));
  EXPECT_FALSE(prev->is_valid_at(2000));

  Clock::reset();
}

TEST_F(NodeMapTest, NodeHandleEqualityOperators) {
  std::unordered_map<std::string, Value> d1 = {{"name", Value{"A"}},
                                               {"age", Value{int32_t(1)}}};
  std::unordered_map<std::string, Value> d2 = {{"name", Value{"B"}},
                                               {"age", Value{int32_t(2)}}};
  auto n1 = node_manager_->create_node("UserWithMap", d1).ValueOrDie();
  auto n2 = node_manager_->create_node("UserWithMap", d2).ValueOrDie();

  const NodeHandle* h1 = n1->get_handle();
  const NodeHandle* h2 = n2->get_handle();

  EXPECT_EQ(*h1, *h1);
  EXPECT_EQ(*h2, *h2);
  EXPECT_NE(*h1, *h2);
  EXPECT_TRUE(*h1 != *h2);
  EXPECT_FALSE(*h1 == *h2);
}

// ============================================================================
// Edge with MAP field tests
// ============================================================================

class EdgeMapTest : public ::testing::Test {
 protected:
  void SetUp() override { store_ = std::make_unique<EdgeStore>(0); }
  void TearDown() override { store_.reset(); }
  std::unique_ptr<EdgeStore> store_;
};

TEST_F(EdgeMapTest, RegisterEdgeSchemaWithMapField) {
  auto res = store_->register_edge_schema(
      "rated", {std::make_shared<Field>("weight", ValueType::DOUBLE),
                std::make_shared<Field>("meta", TypeDescriptor::properties())});
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(store_->has_edge_schema("rated"));

  auto schema = store_->get_edge_schema("rated");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->num_fields(), 2);
  EXPECT_EQ(schema->field(0)->name(), "weight");
  EXPECT_EQ(schema->field(0)->type(), ValueType::DOUBLE);
  EXPECT_EQ(schema->field(1)->name(), "meta");
  EXPECT_EQ(schema->field(1)->type(), ValueType::MAP);
}
