/**
 * Arena leak detection and ref-count lifecycle tests.
 *
 * These tests verify that:
 * - StringRef/ArrayRef ref counts are correct at every lifecycle stage
 * - Arena allocations are fully reclaimed after mark + release
 * - Copy-on-write (COW) arrays properly increment element ref counts
 * - Node deallocation cleans up both string and array fields
 * - No double-release or missed-release bugs exist
 *
 * The tests use get_active_allocs() counters on StringPool/ArrayArena
 * to detect leaks without relying on OS-level tools (which can't see
 * inside custom arenas).
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "memory/array_arena.hpp"
#include "memory/string_arena.hpp"

using namespace tundradb;

// ============================================================================
// STRING ARENA LIFECYCLE TESTS
// ============================================================================

class StringLeakTest : public ::testing::Test {
 protected:
  void SetUp() override {
    arena = std::make_unique<StringArena>();
    arena->enable_deduplication(false);
  }

  std::unique_ptr<StringArena> arena;
};

TEST_F(StringLeakTest, AllocAndRelease) {
  EXPECT_EQ(arena->get_active_allocs(), 0);

  {
    StringRef ref = arena->store_string_auto("hello").ValueOrDie();
    EXPECT_EQ(ref.get_ref_count(), 1);
    EXPECT_EQ(arena->get_active_allocs(), 1);

    arena->mark_for_deletion(ref);
  }
  // ref destroyed → ref_count 1→0 + marked → pool deallocation

  EXPECT_EQ(arena->get_active_allocs(), 0);
}

TEST_F(StringLeakTest, CopyKeepsAlive) {
  StringRef ref1 = arena->store_string_auto("world").ValueOrDie();
  EXPECT_EQ(ref1.get_ref_count(), 1);

  {
    StringRef ref2 = ref1;
    EXPECT_EQ(ref1.get_ref_count(), 2);
    EXPECT_EQ(arena->get_active_allocs(), 1);

    arena->mark_for_deletion(ref1);
    EXPECT_TRUE(ref1.is_marked_for_deletion());
  }
  // ref2 destroyed → ref_count 2→1, still > 0 → NOT freed
  EXPECT_EQ(ref1.get_ref_count(), 1);
  EXPECT_EQ(arena->get_active_allocs(), 1);

  ref1 = StringRef{};
  // ref1 destroyed → ref_count 1→0 + marked → freed
  EXPECT_EQ(arena->get_active_allocs(), 0);
}

TEST_F(StringLeakTest, MoveDoesNotLeak) {
  EXPECT_EQ(arena->get_active_allocs(), 0);

  {
    StringRef ref1 = arena->store_string_auto("move-me").ValueOrDie();
    EXPECT_EQ(ref1.get_ref_count(), 1);

    StringRef ref2 = std::move(ref1);
    EXPECT_TRUE(ref1.is_null());
    EXPECT_EQ(ref2.get_ref_count(), 1);
    EXPECT_EQ(arena->get_active_allocs(), 1);

    arena->mark_for_deletion(ref2);
  }

  EXPECT_EQ(arena->get_active_allocs(), 0);
}

TEST_F(StringLeakTest, MultipleStringsIndependent) {
  StringRef a = arena->store_string_auto("aaa").ValueOrDie();
  StringRef b = arena->store_string_auto("bbb").ValueOrDie();
  StringRef c = arena->store_string_auto("ccc").ValueOrDie();
  EXPECT_EQ(arena->get_active_allocs(), 3);

  arena->mark_for_deletion(b);
  b = StringRef{};
  EXPECT_EQ(arena->get_active_allocs(), 2);

  arena->mark_for_deletion(a);
  a = StringRef{};
  EXPECT_EQ(arena->get_active_allocs(), 1);

  arena->mark_for_deletion(c);
  c = StringRef{};
  EXPECT_EQ(arena->get_active_allocs(), 0);
}

TEST_F(StringLeakTest, NotMarkedNotFreed) {
  StringRef ref = arena->store_string_auto("sticky").ValueOrDie();
  EXPECT_EQ(arena->get_active_allocs(), 1);

  // Destroy without marking → memory stays allocated (intentional)
  ref = StringRef{};
  EXPECT_EQ(arena->get_active_allocs(), 1);
}

// ============================================================================
// ARRAY ARENA LIFECYCLE TESTS
// ============================================================================

class ArrayLeakTest : public ::testing::Test {
 protected:
  void SetUp() override {
    array_arena = std::make_unique<ArrayArena>();
    string_arena = std::make_unique<StringArena>();
    string_arena->enable_deduplication(false);
  }

  std::unique_ptr<ArrayArena> array_arena;
  std::unique_ptr<StringArena> string_arena;
};

TEST_F(ArrayLeakTest, AllocAndRelease_Primitives) {
  EXPECT_EQ(array_arena->get_active_allocs(), 0);

  {
    ArrayRef arr = array_arena->allocate(ValueType::INT32, 4).ValueOrDie();
    EXPECT_EQ(arr.get_ref_count(), 1);
    EXPECT_EQ(array_arena->get_active_allocs(), 1);

    array_arena->mark_for_deletion(arr);
  }

  EXPECT_EQ(array_arena->get_active_allocs(), 0);
}

TEST_F(ArrayLeakTest, EmptyArrayNoAllocation) {
  ArrayRef arr = array_arena->allocate(ValueType::INT32, 0).ValueOrDie();
  EXPECT_TRUE(arr.is_null());
  EXPECT_EQ(array_arena->get_active_allocs(), 0);
}

TEST_F(ArrayLeakTest, AppendInPlaceNoExtraAlloc) {
  ArrayRef arr = array_arena->allocate(ValueType::INT32, 4).ValueOrDie();
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  int32_t val = 42;
  ASSERT_TRUE(array_arena->append(arr, &val).ok());
  EXPECT_EQ(arr.length(), 1);
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  array_arena->mark_for_deletion(arr);
  arr = ArrayRef{};
  EXPECT_EQ(array_arena->get_active_allocs(), 0);
}

TEST_F(ArrayLeakTest, AppendWithReallocation) {
  // Allocate with capacity 1, then append to force reallocation
  ArrayRef arr = array_arena->allocate(ValueType::INT32, 1).ValueOrDie();
  int32_t v1 = 10;
  ASSERT_TRUE(array_arena->append(arr, &v1).ok());
  EXPECT_EQ(arr.length(), 1);
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  // This should trigger reallocation (capacity was 1, now full)
  int32_t v2 = 20;
  ASSERT_TRUE(array_arena->append(arr, &v2).ok());
  EXPECT_EQ(arr.length(), 2);
  // Old allocation freed, new one created → still 1
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  array_arena->mark_for_deletion(arr);
  arr = ArrayRef{};
  EXPECT_EQ(array_arena->get_active_allocs(), 0);
}

TEST_F(ArrayLeakTest, CopyCreatesIndependentAllocation) {
  ArrayRef original = array_arena->allocate(ValueType::INT32, 4).ValueOrDie();
  int32_t v = 99;
  ASSERT_TRUE(array_arena->append(original, &v).ok());
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  ArrayRef copy = array_arena->copy(original).ValueOrDie();
  EXPECT_EQ(array_arena->get_active_allocs(), 2);
  EXPECT_EQ(copy.length(), 1);
  EXPECT_NE(copy.data(), original.data());

  // Free original
  array_arena->mark_for_deletion(original);
  original = ArrayRef{};
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  // Copy still alive
  EXPECT_EQ(copy.get_ref_count(), 1);

  array_arena->mark_for_deletion(copy);
  copy = ArrayRef{};
  EXPECT_EQ(array_arena->get_active_allocs(), 0);
}

// ============================================================================
// ARRAY WITH STRING ELEMENTS — the critical combo
// ============================================================================

TEST_F(ArrayLeakTest, StringElementsReleasedWithArray) {
  // Allocate array for 2 string elements
  ArrayRef arr = array_arena->allocate(ValueType::STRING, 2).ValueOrDie();
  EXPECT_EQ(array_arena->get_active_allocs(), 1);

  // Store strings and append to array
  StringRef s1 = string_arena->store_string_auto("alpha").ValueOrDie();
  ASSERT_TRUE(array_arena->append(arr, &s1).ok());
  // s1 (local) + array element → ref_count = 2
  EXPECT_EQ(s1.get_ref_count(), 2);
  EXPECT_EQ(string_arena->get_active_allocs(), 1);

  StringRef s2 = string_arena->store_string_auto("beta").ValueOrDie();
  ASSERT_TRUE(array_arena->append(arr, &s2).ok());
  EXPECT_EQ(s2.get_ref_count(), 2);
  EXPECT_EQ(string_arena->get_active_allocs(), 2);

  // Drop local refs — array elements are the sole owners now
  s1 = StringRef{};
  s2 = StringRef{};
  EXPECT_EQ(string_arena->get_active_allocs(), 2);

  // Free the array → destruct_elements marks strings + calls ~StringRef
  array_arena->mark_for_deletion(arr);
  arr = ArrayRef{};

  EXPECT_EQ(array_arena->get_active_allocs(), 0);
  EXPECT_EQ(string_arena->get_active_allocs(), 0);
}

TEST_F(ArrayLeakTest, COWArrayPreservesStringRefCounts) {
  // Build array with a string element
  ArrayRef arr = array_arena->allocate(ValueType::STRING, 4).ValueOrDie();
  StringRef s = string_arena->store_string_auto("shared").ValueOrDie();
  ASSERT_TRUE(array_arena->append(arr, &s).ok());

  // s (local) + arr element → ref_count = 2
  EXPECT_EQ(s.get_ref_count(), 2);

  // COW copy (simulates versioned update path)
  ArrayRef cow = array_arena->copy(arr).ValueOrDie();
  EXPECT_EQ(array_arena->get_active_allocs(), 2);

  // copy_init_elements increments ref_count: local + old element + new element
  EXPECT_EQ(s.get_ref_count(), 3);

  // Free original array
  s = StringRef{};  // drop local → ref_count = 2 (old elem + new elem)
  array_arena->mark_for_deletion(arr);
  arr = ArrayRef{};

  // Old array freed, its string element released → ref_count = 1 (new elem)
  EXPECT_EQ(array_arena->get_active_allocs(), 1);
  EXPECT_EQ(string_arena->get_active_allocs(), 1);

  // Free COW copy
  array_arena->mark_for_deletion(cow);
  cow = ArrayRef{};

  EXPECT_EQ(array_arena->get_active_allocs(), 0);
  EXPECT_EQ(string_arena->get_active_allocs(), 0);
}

TEST_F(ArrayLeakTest, AppendStringWithReallocation) {
  // Start with capacity 1 to force reallocation on second append
  ArrayRef arr = array_arena->allocate(ValueType::STRING, 1).ValueOrDie();

  StringRef s1 = string_arena->store_string_auto("first").ValueOrDie();
  ASSERT_TRUE(array_arena->append(arr, &s1).ok());
  EXPECT_EQ(s1.get_ref_count(), 2);  // local + element
  s1 = StringRef{};                  // ref_count = 1 (element only)

  // Second append forces reallocation: old elements copied to new array
  StringRef s2 = string_arena->store_string_auto("second").ValueOrDie();
  ASSERT_TRUE(array_arena->append(arr, &s2).ok());
  s2 = StringRef{};

  // After realloc: old array freed (its string element released),
  // but new array has a copy (ref_count properly incremented by
  // copy_init_elements)
  EXPECT_EQ(arr.length(), 2);
  EXPECT_EQ(array_arena->get_active_allocs(), 1);
  EXPECT_EQ(string_arena->get_active_allocs(), 2);

  // Full cleanup
  array_arena->mark_for_deletion(arr);
  arr = ArrayRef{};
  EXPECT_EQ(array_arena->get_active_allocs(), 0);
  EXPECT_EQ(string_arena->get_active_allocs(), 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
