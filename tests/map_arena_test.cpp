#include "../include/map_arena.hpp"

#include <gtest/gtest.h>

#include <string>

#include "../include/edge.hpp"
#include "../include/node.hpp"
#include "../include/node_arena.hpp"
#include "../include/schema.hpp"
#include "../include/schema_layout.hpp"
#include "../include/types.hpp"
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

  ASSERT_TRUE(
      MapArena::set_entry(ref, key_a, ValueType::INT32, &val_a).ok());
  ASSERT_TRUE(
      MapArena::set_entry(ref, key_b, ValueType::DOUBLE, &val_b).ok());
  ASSERT_TRUE(
      MapArena::set_entry(ref, key_c, ValueType::BOOL, &val_c).ok());

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

  ASSERT_TRUE(
      MapArena::set_entry(ref, key, ValueType::STRING, &val).ok());

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
  std::unordered_map<std::string, Value> data = {
      {"name", Value{"Alice"}}, {"age", Value{int32_t(30)}}};

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
  std::unordered_map<std::string, Value> data = {
      {"name", Value{"Bob"}}, {"age", Value{int32_t(25)}}};

  auto node_res = node_manager_->create_node("UserWithMap", data);
  ASSERT_TRUE(node_res.ok()) << node_res.status().ToString();
  auto node = node_res.ValueOrDie();

  auto* arena = node->get_arena();
  auto schema = node->get_schema();
  auto props_field = schema->get_field("props");

  // Allocate a map and populate it
  auto map_res = arena->allocate_map();
  ASSERT_TRUE(map_res.ok());
  MapRef map = std::move(*map_res);
  ASSERT_TRUE(arena->set_map_entry(map, "score", Value{3.14}).ok());
  ASSERT_TRUE(arena->set_map_entry(map, "active", Value{true}).ok());

  // Write map to the node via update_fields
  auto update_res = node->update_fields(
      {FieldUpdate{props_field, Value{map}, UpdateType::SET}});
  ASSERT_TRUE(update_res.ok());

  // Read it back
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
      "rated",
      {std::make_shared<Field>("weight", ValueType::DOUBLE),
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
