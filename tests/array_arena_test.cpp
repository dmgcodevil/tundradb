#include "memory/array_arena.hpp"

#include <gtest/gtest.h>

#include <string>

#include "memory/string_arena.hpp"

using namespace tundradb;

class ArrayArenaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    arena_ = std::make_unique<ArrayArena>();
    string_arena_ = std::make_unique<StringArena>();
  }

  std::unique_ptr<ArrayArena> arena_;
  std::unique_ptr<StringArena> string_arena_;
};

// ============================================================================
// Basic allocate / copy / append (primitives) — baseline sanity
// ============================================================================

TEST_F(ArrayArenaTest, AllocateEmptyInt32) {
  auto res = arena_->allocate(ValueType::INT32, 4);
  ASSERT_TRUE(res.ok());
  ArrayRef ref = std::move(*res);
  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.length(), 0u);
  EXPECT_EQ(ref.capacity(), 4u);
  EXPECT_EQ(ref.elem_type(), ValueType::INT32);
  EXPECT_EQ(arena_->get_active_allocs(), 1);
}

TEST_F(ArrayArenaTest, AllocateWithDataInt32) {
  int32_t data[] = {10, 20, 30};
  auto res = arena_->allocate_with_data(ValueType::INT32, data, 3);
  ASSERT_TRUE(res.ok());
  ArrayRef ref = std::move(*res);
  EXPECT_EQ(ref.length(), 3u);
  EXPECT_GE(ref.capacity(), 3u);
}

TEST_F(ArrayArenaTest, AllocateWithDataZeroCount) {
  auto res = arena_->allocate_with_data(ValueType::INT32, nullptr, 0);
  ASSERT_TRUE(res.ok());
  ArrayRef ref = std::move(*res);
  EXPECT_TRUE(ref.is_null());
}

TEST_F(ArrayArenaTest, AppendSingleElement) {
  auto res = arena_->allocate(ValueType::INT32, 2);
  ASSERT_TRUE(res.ok());
  ArrayRef ref = std::move(*res);

  int32_t v1 = 42;
  ASSERT_TRUE(arena_->append(ref, &v1).ok());
  EXPECT_EQ(ref.length(), 1u);

  int32_t v2 = 99;
  ASSERT_TRUE(arena_->append(ref, &v2).ok());
  EXPECT_EQ(ref.length(), 2u);
}

TEST_F(ArrayArenaTest, AppendTriggersCowGrowth) {
  auto res = arena_->allocate(ValueType::INT32, 1);
  ASSERT_TRUE(res.ok());
  ArrayRef ref = std::move(*res);

  int32_t v = 1;
  ASSERT_TRUE(arena_->append(ref, &v).ok());
  EXPECT_EQ(ref.length(), 1u);

  int32_t v2 = 2;
  ASSERT_TRUE(arena_->append(ref, &v2).ok());
  EXPECT_EQ(ref.length(), 2u);
  EXPECT_GE(ref.capacity(), 2u);
}

TEST_F(ArrayArenaTest, CopyArray) {
  int32_t data[] = {1, 2, 3};
  auto ref = arena_->allocate_with_data(ValueType::INT32, data, 3).ValueOrDie();

  auto copy_res = arena_->copy(ref);
  ASSERT_TRUE(copy_res.ok());
  ArrayRef copy = std::move(*copy_res);
  EXPECT_EQ(copy.length(), 3u);
  EXPECT_EQ(arena_->get_active_allocs(), 2);
}

TEST_F(ArrayArenaTest, MarkForDeletionAndRelease) {
  auto ref = arena_->allocate(ValueType::INT32, 4).ValueOrDie();
  EXPECT_EQ(arena_->get_active_allocs(), 1);

  arena_->mark_for_deletion(ref);
  EXPECT_TRUE(ref.is_marked_for_deletion());
  ref = ArrayRef{};
  EXPECT_EQ(arena_->get_active_allocs(), 0);
}

// ============================================================================
// Statistics & lifecycle
// ============================================================================

TEST_F(ArrayArenaTest, StatsAfterAllocations) {
  size_t initial_total = arena_->get_total_allocated();
  EXPECT_EQ(arena_->get_used_bytes(), 0u);
  EXPECT_EQ(arena_->get_freed_bytes(), 0u);

  auto ref = arena_->allocate(ValueType::INT32, 8).ValueOrDie();
  EXPECT_GE(arena_->get_total_allocated(), initial_total);
  EXPECT_GT(arena_->get_used_bytes(), 0u);

  arena_->mark_for_deletion(ref);
  ref = ArrayRef{};
  EXPECT_GT(arena_->get_freed_bytes(), 0u);
}

TEST_F(ArrayArenaTest, ResetClearsState) {
  {
    auto ref = arena_->allocate(ValueType::INT32, 4).ValueOrDie();
    EXPECT_GT(arena_->get_used_bytes(), 0u);
  }
  arena_->reset();
  EXPECT_EQ(arena_->get_used_bytes(), 0u);
}

TEST_F(ArrayArenaTest, ClearDeallocatesMemory) {
  {
    auto ref = arena_->allocate(ValueType::INT32, 4).ValueOrDie();
    EXPECT_GT(arena_->get_total_allocated(), 0u);
  }
  arena_->clear();
  EXPECT_EQ(arena_->get_used_bytes(), 0u);
}

// ============================================================================
// Nested arrays: arrays of ArrayRef  (covers is_array_type branches)
// ============================================================================

TEST_F(ArrayArenaTest, ZeroInitElementsForArrayType) {
  auto res = arena_->allocate(ValueType::ARRAY, 4);
  ASSERT_TRUE(res.ok());
  ArrayRef outer = std::move(*res);
  EXPECT_FALSE(outer.is_null());
  EXPECT_EQ(outer.length(), 0u);
  EXPECT_EQ(outer.elem_type(), ValueType::ARRAY);
}

TEST_F(ArrayArenaTest, AppendArrayRefElement) {
  auto outer = arena_->allocate(ValueType::ARRAY, 4).ValueOrDie();

  int32_t inner_data[] = {10, 20};
  auto inner =
      arena_->allocate_with_data(ValueType::INT32, inner_data, 2).ValueOrDie();

  ASSERT_TRUE(arena_->append(outer, &inner).ok());
  EXPECT_EQ(outer.length(), 1u);
}

TEST_F(ArrayArenaTest, CopyInitElementsForArrayType) {
  auto outer = arena_->allocate(ValueType::ARRAY, 4).ValueOrDie();

  int32_t d1[] = {1, 2};
  auto inner1 =
      arena_->allocate_with_data(ValueType::INT32, d1, 2).ValueOrDie();
  int32_t d2[] = {3, 4, 5};
  auto inner2 =
      arena_->allocate_with_data(ValueType::INT32, d2, 3).ValueOrDie();

  ASSERT_TRUE(arena_->append(outer, &inner1).ok());
  ASSERT_TRUE(arena_->append(outer, &inner2).ok());
  EXPECT_EQ(outer.length(), 2u);

  auto copy = arena_->copy(outer).ValueOrDie();
  EXPECT_EQ(copy.length(), 2u);
  EXPECT_EQ(copy.elem_type(), ValueType::ARRAY);
}

TEST_F(ArrayArenaTest, AllocateWithDataForArrayType) {
  int32_t d1[] = {10};
  auto inner1 =
      arena_->allocate_with_data(ValueType::INT32, d1, 1).ValueOrDie();
  int32_t d2[] = {20};
  auto inner2 =
      arena_->allocate_with_data(ValueType::INT32, d2, 1).ValueOrDie();

  ArrayRef inners[] = {inner1, inner2};
  auto outer =
      arena_->allocate_with_data(ValueType::ARRAY, inners, 2, 4).ValueOrDie();
  EXPECT_EQ(outer.length(), 2u);
  EXPECT_EQ(outer.capacity(), 4u);
}

TEST_F(ArrayArenaTest, AssignElementForArrayType) {
  auto outer = arena_->allocate(ValueType::ARRAY, 4).ValueOrDie();

  int32_t d1[] = {1};
  auto inner = arena_->allocate_with_data(ValueType::INT32, d1, 1).ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &inner).ok());

  int32_t d2[] = {99};
  auto replacement =
      arena_->allocate_with_data(ValueType::INT32, d2, 1).ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &replacement).ok());
  EXPECT_EQ(outer.length(), 2u);
}

TEST_F(ArrayArenaTest, DestructElementsForArrayType) {
  auto outer = arena_->allocate(ValueType::ARRAY, 4).ValueOrDie();

  int32_t d[] = {5};
  auto inner = arena_->allocate_with_data(ValueType::INT32, d, 1).ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &inner).ok());

  arena_->mark_for_deletion(outer);
  outer = ArrayRef{};
}

TEST_F(ArrayArenaTest, AppendCowGrowthForArrayType) {
  auto outer = arena_->allocate(ValueType::ARRAY, 1).ValueOrDie();

  int32_t d1[] = {1};
  auto inner1 =
      arena_->allocate_with_data(ValueType::INT32, d1, 1).ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &inner1).ok());
  EXPECT_EQ(outer.length(), 1u);

  int32_t d2[] = {2};
  auto inner2 =
      arena_->allocate_with_data(ValueType::INT32, d2, 1).ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &inner2).ok());
  EXPECT_EQ(outer.length(), 2u);
  EXPECT_GE(outer.capacity(), 2u);
}

// ============================================================================
// Arrays of StringRef (covers is_string_type branches — baseline)
// ============================================================================

TEST_F(ArrayArenaTest, AllocateStringArray) {
  auto outer = arena_->allocate(ValueType::STRING, 4).ValueOrDie();
  EXPECT_EQ(outer.elem_type(), ValueType::STRING);
  EXPECT_EQ(outer.length(), 0u);
}

TEST_F(ArrayArenaTest, AppendStringRefElement) {
  auto outer = arena_->allocate(ValueType::STRING, 4).ValueOrDie();

  auto sr = string_arena_->store_string_auto("hello").ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &sr).ok());
  EXPECT_EQ(outer.length(), 1u);
}

TEST_F(ArrayArenaTest, CopyStringArray) {
  auto outer = arena_->allocate(ValueType::STRING, 4).ValueOrDie();

  auto s1 = string_arena_->store_string_auto("aaa").ValueOrDie();
  auto s2 = string_arena_->store_string_auto("bbb").ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &s1).ok());
  ASSERT_TRUE(arena_->append(outer, &s2).ok());

  auto copy = arena_->copy(outer).ValueOrDie();
  EXPECT_EQ(copy.length(), 2u);
}

TEST_F(ArrayArenaTest, DestructStringElements) {
  auto outer = arena_->allocate(ValueType::STRING, 4).ValueOrDie();
  auto sr = string_arena_->store_string_auto("text").ValueOrDie();
  ASSERT_TRUE(arena_->append(outer, &sr).ok());

  arena_->mark_for_deletion(outer);
  outer = ArrayRef{};
}

// ============================================================================
// Error paths
// ============================================================================

TEST_F(ArrayArenaTest, AppendToNullRefFails) {
  ArrayRef null_ref;
  int32_t v = 1;
  auto status = arena_->append(null_ref, &v);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalid());
}

TEST_F(ArrayArenaTest, CopyNullRefFails) {
  ArrayRef null_ref;
  auto res = arena_->copy(null_ref);
  EXPECT_FALSE(res.ok());
  EXPECT_TRUE(res.status().IsInvalid());
}
