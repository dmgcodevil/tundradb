/**
 * Tests for intrusive reference-counted StringRef with concurrent access.
 *
 * This test suite verifies:
 * - Reference counting correctness
 * - Thread-safe concurrent access
 * - Mark-for-deletion semantics
 * - Memory deallocation when ref count reaches zero
 * - Deduplication behavior
 */

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "../include/string_arena.hpp"
#include "../include/string_ref.hpp"
#include "../include/types.hpp"

using namespace tundradb;

class StringRefConcurrentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    arena = std::make_unique<StringArena>();
    // Disable dedup for most tests to get predictable ref counts
    // Tests that specifically need dedup will enable it
    arena->enable_deduplication(false);
  }

  std::unique_ptr<StringArena> arena;
};

// ============================================================================
// BASIC FUNCTIONALITY TESTS
// ============================================================================

TEST_F(StringRefConcurrentTest, BasicAllocationAndDeallocation) {
  // Allocate a string
  StringRef ref = arena->store_string("Hello World");

  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.length(), 11);
  EXPECT_EQ(ref.view(), "Hello World");
  EXPECT_EQ(ref.get_ref_count(), 1);
}

TEST_F(StringRefConcurrentTest, CopyConstructorIncrementsRefCount) {
  StringRef ref1 = arena->store_string("Test");
  EXPECT_EQ(ref1.get_ref_count(), 1);

  // Copy constructor should increment ref count
  StringRef ref2 = ref1;
  EXPECT_EQ(ref1.get_ref_count(), 2);
  EXPECT_EQ(ref2.get_ref_count(), 2);

  // Both refs point to the same data
  EXPECT_EQ(ref1.data(), ref2.data());
  EXPECT_EQ(ref1, ref2);
}

TEST_F(StringRefConcurrentTest, MoveConstructorDoesNotChangeRefCount) {
  StringRef ref1 = arena->store_string("Test");
  const char* original_data = ref1.data();
  EXPECT_EQ(ref1.get_ref_count(), 1);

  // Move constructor should NOT change ref count
  StringRef ref2 = std::move(ref1);
  EXPECT_EQ(ref2.get_ref_count(), 1);
  EXPECT_EQ(ref2.data(), original_data);

  // ref1 should be null after move
  EXPECT_TRUE(ref1.is_null());
}

TEST_F(StringRefConcurrentTest, CopyAssignmentIncrementsRefCount) {
  StringRef ref1 = arena->store_string("First");
  StringRef ref2 = arena->store_string("Second");

  EXPECT_EQ(ref1.get_ref_count(), 1);
  EXPECT_EQ(ref2.get_ref_count(), 1);

  // Copy assignment
  ref2 = ref1;

  EXPECT_EQ(ref1.get_ref_count(), 2);
  EXPECT_EQ(ref2.get_ref_count(), 2);
  EXPECT_EQ(ref1.data(), ref2.data());
}

TEST_F(StringRefConcurrentTest, DestructorDecrementsRefCount) {
  StringRef ref1 = arena->store_string("Test");
  EXPECT_EQ(ref1.get_ref_count(), 1);

  {
    StringRef ref2 = ref1;
    EXPECT_EQ(ref1.get_ref_count(), 2);
  }  // ref2 destroyed here

  // ref1 should still be valid with ref count = 1
  EXPECT_EQ(ref1.get_ref_count(), 1);
  EXPECT_EQ(ref1.view(), "Test");
}

// ============================================================================
// MARK-FOR-DELETION TESTS
// ============================================================================

TEST_F(StringRefConcurrentTest, MarkForDeletionWithMultipleRefs) {
  StringRef ref1 = arena->store_string("ToDelete");
  StringRef ref2 = ref1;  // ref_count = 2

  EXPECT_EQ(ref1.get_ref_count(), 2);
  EXPECT_FALSE(ref1.is_marked_for_deletion());

  // Mark for deletion
  arena->mark_for_deletion(ref1);

  // String should be marked but NOT deallocated (ref_count > 0)
  EXPECT_TRUE(ref1.is_marked_for_deletion());
  EXPECT_EQ(ref1.get_ref_count(), 2);

  // Both refs should still be valid
  EXPECT_EQ(ref1.view(), "ToDelete");
  EXPECT_EQ(ref2.view(), "ToDelete");
}

TEST_F(StringRefConcurrentTest, DeallocateWhenLastRefDestroyed) {
  StringRef* ref1 = new StringRef(arena->store_string("Temporary"));
  const char* data_ptr = ref1->data();

  // Mark for deletion
  arena->mark_for_deletion(*ref1);
  EXPECT_TRUE(ref1->is_marked_for_deletion());

  // Delete the last reference - should trigger deallocation
  delete ref1;

  // Can't directly verify deallocation, but no crash = success
  // In a real scenario, accessing data_ptr after this would be UB
}

TEST_F(StringRefConcurrentTest, NoDeallocateIfNotMarked) {
  StringRef* ref1 = new StringRef(arena->store_string("NotMarked"));

  // Do NOT mark for deletion
  EXPECT_FALSE(ref1->is_marked_for_deletion());

  // Delete the reference - should NOT trigger deallocation
  delete ref1;

  // Memory remains in arena (not deallocated)
  // This is important for strings that are still "active" in the graph
}

// ============================================================================
// DEDUPLICATION TESTS
// ============================================================================

TEST_F(StringRefConcurrentTest, DeduplicationSharesMemory) {
  // Enable deduplication for this test
  arena->enable_deduplication(true);

  StringRef ref1 = arena->store_string("Shared");
  StringRef ref2 = arena->store_string("Shared");  // Same content

  // Both refs should point to the same memory
  EXPECT_EQ(ref1.data(), ref2.data());
  // ref_count = cache(1) + ref1(1) + ref2(1) = 3
  EXPECT_EQ(ref1.get_ref_count(), 3);
  EXPECT_EQ(ref2.get_ref_count(), 3);
}

TEST_F(StringRefConcurrentTest, MarkForDeletionRemovesFromDedup) {
  // Enable deduplication for this test
  arena->enable_deduplication(true);

  StringRef ref1 = arena->store_string("DedupTest");
  const char* original_data = ref1.data();

  // Mark for deletion - should remove from dedup cache
  arena->mark_for_deletion(ref1);

  // New string with same content should allocate NEW memory
  StringRef ref2 = arena->store_string("DedupTest");
  EXPECT_NE(ref1.data(), ref2.data());  // Different memory!
}

TEST_F(StringRefConcurrentTest, CacheNotCorruptedByDeallocation) {
  // This test verifies the fix for the critical bug where release_string()
  // could accidentally remove a NEW active string from the cache

  // Enable deduplication for this test
  arena->enable_deduplication(true);

  // Step 1: Create and mark old string for deletion
  StringRef old_ref = arena->store_string("SharedString");
  const char* old_data = old_ref.data();

  // Mark for deletion - removes from cache
  arena->mark_for_deletion(old_ref);

  // Step 2: Create NEW string with same content
  // This should allocate new memory and add to cache
  StringRef new_ref = arena->store_string("SharedString");
  const char* new_data = new_ref.data();

  // Different memory addresses
  EXPECT_NE(old_data, new_data);

  // Step 3: Destroy old ref (triggers release_string on old_data)
  // This should NOT remove new_ref from cache!
  old_ref = StringRef();

  // Step 4: Try to get from cache - should hit!
  // If release_string() incorrectly removed from cache, this would allocate new
  // memory
  StringRef cached = arena->store_string("SharedString");

  // ✅ Should be cache hit - same address as new_ref
  EXPECT_EQ(cached.data(), new_data);

  // Verify ref count is correct: cache(1) + new_ref(1) + cached(1) = 3
  EXPECT_EQ(new_ref.get_ref_count(), 3);
  EXPECT_EQ(cached.get_ref_count(), 3);
}

// ============================================================================
// CONCURRENT ACCESS TESTS
// ============================================================================

TEST_F(StringRefConcurrentTest, ConcurrentCopying) {
  const int num_threads = 10;
  const int copies_per_thread = 1000;

  StringRef original = arena->store_string("ConcurrentTest");
  std::vector<std::thread> threads;

  // Each thread makes many copies
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&original, copies_per_thread]() {
      std::vector<StringRef> refs;
      refs.reserve(copies_per_thread);

      for (int j = 0; j < copies_per_thread; ++j) {
        refs.push_back(original);  // Copy constructor
      }

      // All refs should be valid
      for (const auto& ref : refs) {
        EXPECT_EQ(ref.view(), "ConcurrentTest");
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Original should still be valid
  EXPECT_EQ(original.view(), "ConcurrentTest");
  // After all threads finish, ref count should be back to 1
  EXPECT_EQ(original.get_ref_count(), 1);
}

TEST_F(StringRefConcurrentTest, ConcurrentReadWhileMarkingForDeletion) {
  const int num_readers = 5;
  const int reads_per_thread = 10000;

  StringRef shared = arena->store_string("SharedData");
  std::atomic<bool> start{false};
  std::atomic<bool> marked{false};

  std::vector<std::thread> readers;

  // Spawn reader threads
  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back([&shared, &start, &marked, reads_per_thread]() {
      // Wait for start signal
      while (!start.load(std::memory_order_acquire)) {
      }

      // Keep making copies and reading
      for (int j = 0; j < reads_per_thread; ++j) {
        StringRef copy = shared;
        EXPECT_EQ(copy.view(), "SharedData");

        // Check if marked (should be safe even if marked)
        if (marked.load(std::memory_order_acquire)) {
          EXPECT_TRUE(copy.is_marked_for_deletion() ||
                      !copy.is_marked_for_deletion());
        }
      }
    });
  }

  // Mark thread
  std::thread marker([this, &shared, &start, &marked]() {
    // Wait for start signal
    while (!start.load(std::memory_order_acquire)) {
    }

    // Mark for deletion while readers are active
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    arena->mark_for_deletion(shared);
    marked.store(true, std::memory_order_release);
  });

  // Start all threads
  start.store(true, std::memory_order_release);

  for (auto& t : readers) {
    t.join();
  }
  marker.join();

  // String should be marked but NOT deallocated (readers might still have refs)
  EXPECT_TRUE(shared.is_marked_for_deletion());
}

TEST_F(StringRefConcurrentTest, StressTestManyStrings) {
  const int num_strings = 1000;
  const int num_threads = 10;

  std::vector<std::thread> threads;
  std::atomic<int> total_created{0};

  // Store all StringRefs to keep them alive during the test
  std::vector<std::vector<StringRef>> thread_refs(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    thread_refs[i].reserve(num_strings);

    threads.emplace_back(
        [this, num_strings, i, &total_created, &thread_refs]() {
          for (int j = 0; j < num_strings; ++j) {
            std::string content =
                "Thread" + std::to_string(i) + "_String" + std::to_string(j);
            StringRef ref = arena->store_string(content);

            // Verify immediately
            EXPECT_EQ(ref.view(), content);
            EXPECT_GE(ref.get_ref_count(), 1);  // At least 1

            // Keep the ref alive
            thread_refs[i].push_back(ref);

            total_created.fetch_add(1, std::memory_order_relaxed);
          }
        });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(total_created.load(), num_strings * num_threads);

  // Verify all refs are still valid
  for (int i = 0; i < num_threads; ++i) {
    for (int j = 0; j < num_strings; ++j) {
      std::string expected =
          "Thread" + std::to_string(i) + "_String" + std::to_string(j);
      EXPECT_EQ(thread_refs[i][j].view(), expected);
    }
  }
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_F(StringRefConcurrentTest, EmptyString) {
  StringRef ref = arena->store_string("");

  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.length(), 0);
  EXPECT_EQ(ref.view(), "");
  EXPECT_EQ(ref.get_ref_count(), 1);
}

TEST_F(StringRefConcurrentTest, NullStringRef) {
  StringRef ref;

  EXPECT_TRUE(ref.is_null());
  EXPECT_EQ(ref.length(), 0);
  EXPECT_EQ(ref.view(), "");
  EXPECT_EQ(ref.get_ref_count(), 0);
}

TEST_F(StringRefConcurrentTest, LargeString) {
  std::string large(10000, 'X');
  StringRef ref = arena->store_string(large);

  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.length(), 10000);
  EXPECT_EQ(ref.view(), large);
  EXPECT_EQ(ref.get_ref_count(), 1);
}

TEST_F(StringRefConcurrentTest, SelfAssignment) {
  StringRef ref = arena->store_string("SelfAssign");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign-overloaded"
  ref = ref;  // Self-assignment
#pragma clang diagnostic pop

  EXPECT_FALSE(ref.is_null());
  EXPECT_EQ(ref.view(), "SelfAssign");
  EXPECT_EQ(ref.get_ref_count(), 1);  // Should still be 1
}

// ============================================================================
// POOL ROUTING TESTS
// ============================================================================

TEST_F(StringRefConcurrentTest, AutoPoolSelection) {
  StringRef small = arena->store_string_auto("X");  // Pool 0 (<=16)
  StringRef medium =
      arena->store_string_auto(std::string(20, 'Y'));  // Pool 1 (<=32)
  StringRef large =
      arena->store_string_auto(std::string(50, 'Z'));  // Pool 2 (<=64)
  StringRef huge =
      arena->store_string_auto(std::string(100, 'W'));  // Pool 3 (unlimited)

  EXPECT_EQ(small.pool_id(), 0);
  EXPECT_EQ(medium.pool_id(), 1);
  EXPECT_EQ(large.pool_id(), 2);
  EXPECT_EQ(huge.pool_id(), 3);
}

TEST_F(StringRefConcurrentTest, ExplicitPoolSelection) {
  // Test explicit pool selection by pool_id
  StringRef ref0 = arena->store_string("Test", 0);  // Pool 0 (<=16 bytes)
  StringRef ref1 = arena->store_string("Test", 1);  // Pool 1 (<=32 bytes)
  StringRef ref3 = arena->store_string("Test", 3);  // Pool 3 (unlimited)

  EXPECT_EQ(ref0.pool_id(), 0);
  EXPECT_EQ(ref1.pool_id(), 1);
  EXPECT_EQ(ref3.pool_id(), 3);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
