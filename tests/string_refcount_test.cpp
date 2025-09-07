#include <gtest/gtest.h>

#include "../include/string_arena.hpp"

using namespace tundradb;

TEST(StringRefCountTest, DISABLED_BasicRefCounting) {
  StringPool pool(100);  // 100 char max

  // Store same string twice - should get same pointer
  std::string test_str = "hello world";
  StringRef ref1 = pool.store_string(test_str, 1);
  StringRef ref2 = pool.store_string(test_str, 1);

  EXPECT_EQ(ref1.data, ref2.data);  // Same pointer
  EXPECT_EQ(ref1.length, ref2.length);

  // Should have 1 unique string with 2 references
  EXPECT_EQ(pool.get_string_count(), 1);
  EXPECT_EQ(pool.get_total_references(), 2);

  // Deallocate first reference - string should still exist
  pool.deallocate_string(ref1);
  EXPECT_EQ(pool.get_string_count(), 1);      // Still exists
  EXPECT_EQ(pool.get_total_references(), 1);  // 1 reference left

  // String should still be accessible via ref2
  std::string_view view = pool.get_string_view(ref2);
  EXPECT_EQ(view, test_str);

  // Deallocate second reference - now string should be freed
  pool.deallocate_string(ref2);
  EXPECT_EQ(pool.get_string_count(), 0);  // Completely deallocated
  EXPECT_EQ(pool.get_total_references(), 0);
}

TEST(StringRefCountTest, DISABLED_MultipleStringsRefCount) {
  StringPool pool(100);

  // Store different strings
  StringRef ref_a1 = pool.store_string(std::string("apple"), 1);
  StringRef ref_a2 =
      pool.store_string(std::string("apple"), 1);  // Same as above
  StringRef ref_b1 =
      pool.store_string(std::string("banana"), 1);  // Different string

  // Should have 2 unique strings
  EXPECT_EQ(pool.get_string_count(), 2);
  EXPECT_EQ(pool.get_total_references(),
            3);  // 2 refs to "apple" + 1 ref to "banana"

  // Same string should have same pointer
  EXPECT_EQ(ref_a1.data, ref_a2.data);
  EXPECT_NE(ref_a1.data, ref_b1.data);

  // Deallocate one "apple" reference
  pool.deallocate_string(ref_a1);
  EXPECT_EQ(pool.get_string_count(), 2);  // Both strings still exist
  EXPECT_EQ(pool.get_total_references(),
            2);  // 1 ref to "apple" + 1 ref to "banana"

  // Deallocate "banana"
  pool.deallocate_string(ref_b1);
  EXPECT_EQ(pool.get_string_count(), 1);  // Only "apple" left
  EXPECT_EQ(pool.get_total_references(), 1);

  // Deallocate last "apple" reference
  pool.deallocate_string(ref_a2);
  EXPECT_EQ(pool.get_string_count(), 0);  // All deallocated
  EXPECT_EQ(pool.get_total_references(), 0);
}

TEST(StringRefCountTest, WithDeduplicationDisabled) {
  StringPool pool(100);
  pool.enable_deduplication(false);  // Disable deduplication

  // Store same string twice - should get different pointers
  std::string test_str = "hello world";
  StringRef ref1 = pool.store_string(test_str, 1);
  StringRef ref2 = pool.store_string(test_str, 1);

  EXPECT_NE(ref1.data, ref2.data);  // Different pointers when dedup disabled
  EXPECT_EQ(ref1.length, ref2.length);

  // Deallocate both independently
  pool.deallocate_string(ref1);  // Should not affect ref2
  pool.deallocate_string(ref2);  // Independent deallocation

  // No crashes should occur
}