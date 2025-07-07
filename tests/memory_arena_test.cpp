#include "../include/memory_arena.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

using namespace tundradb;

class MemoryArenaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a small arena for testing (1KB chunks)
    arena = std::make_unique<MemoryArena>(1024);
  }

  void TearDown() override { arena.reset(); }

  std::unique_ptr<MemoryArena> arena;
};

// Test basic allocation functionality
TEST_F(MemoryArenaTest, BasicAllocation) {
  // Test basic allocation
  void* ptr1 = arena->allocate(100);
  ASSERT_NE(ptr1, nullptr);

  void* ptr2 = arena->allocate(200);
  ASSERT_NE(ptr2, nullptr);

  // Pointers should be different
  EXPECT_NE(ptr1, ptr2);

  // Second pointer should be after first (plus alignment)
  EXPECT_GT(static_cast<char*>(ptr2), static_cast<char*>(ptr1));

  // Test that we can write to allocated memory
  char* char_ptr = static_cast<char*>(ptr1);
  for (int i = 0; i < 100; ++i) {
    char_ptr[i] = static_cast<char>(i % 256);
  }

  // Verify data integrity
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(char_ptr[i], static_cast<char>(i % 256));
  }
}

// Test alignment requirements
TEST_F(MemoryArenaTest, AlignmentTest) {
  // Test different alignment requirements
  void* ptr1 = arena->allocate(10, 1);   // 1-byte aligned
  void* ptr2 = arena->allocate(10, 4);   // 4-byte aligned
  void* ptr3 = arena->allocate(10, 8);   // 8-byte aligned
  void* ptr4 = arena->allocate(10, 16);  // 16-byte aligned

  ASSERT_NE(ptr1, nullptr);
  ASSERT_NE(ptr2, nullptr);
  ASSERT_NE(ptr3, nullptr);
  ASSERT_NE(ptr4, nullptr);

  // Check alignment
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 1, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 4, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 8, 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr4) % 16, 0);
}

// Test chunk overflow and new chunk allocation
TEST_F(MemoryArenaTest, ChunkOverflowTest) {
  // Get initial chunk count
  size_t initial_chunks = arena->get_chunk_count();
  EXPECT_EQ(initial_chunks, 1);  // Should start with 1 chunk

  // Allocate small amounts that fit in first chunk
  std::vector<void*> small_ptrs;
  for (int i = 0; i < 10; ++i) {
    void* ptr = arena->allocate(50);  // 50 bytes each
    ASSERT_NE(ptr, nullptr);
    small_ptrs.push_back(ptr);
  }

  // Should still be in first chunk
  EXPECT_EQ(arena->get_chunk_count(), 1);

  // Now allocate something that won't fit in remaining space
  void* large_ptr = arena->allocate(800);  // This should trigger new chunk
  ASSERT_NE(large_ptr, nullptr);

  // Should now have 2 chunks
  EXPECT_EQ(arena->get_chunk_count(), 2);

  // Verify all pointers are still valid and distinct
  for (size_t i = 0; i < small_ptrs.size(); ++i) {
    EXPECT_NE(small_ptrs[i], nullptr);
    for (size_t j = i + 1; j < small_ptrs.size(); ++j) {
      EXPECT_NE(small_ptrs[i], small_ptrs[j]);
    }
  }
}

// Test large allocation (bigger than default chunk size)
TEST_F(MemoryArenaTest, LargeAllocationTest) {
  size_t large_size = 2048;  // Larger than default 1KB chunk

  void* large_ptr = arena->allocate(large_size);
  ASSERT_NE(large_ptr, nullptr);

  // Should have created a new chunk to accommodate the large allocation
  EXPECT_GE(arena->get_chunk_count(), 1);

  // Test that we can write to the entire allocation
  char* char_ptr = static_cast<char*>(large_ptr);
  for (size_t i = 0; i < large_size; ++i) {
    char_ptr[i] = static_cast<char>(i % 256);
  }

  // Verify data integrity
  for (size_t i = 0; i < large_size; ++i) {
    EXPECT_EQ(char_ptr[i], static_cast<char>(i % 256));
  }
}

// Test construct method with different types
TEST_F(MemoryArenaTest, ConstructTest) {
  // Test constructing basic types
  int* int_ptr = arena->construct<int>(42);
  ASSERT_NE(int_ptr, nullptr);
  EXPECT_EQ(*int_ptr, 42);

  double* double_ptr = arena->construct<double>(3.14159);
  ASSERT_NE(double_ptr, nullptr);
  EXPECT_DOUBLE_EQ(*double_ptr, 3.14159);

  // Test constructing with proper alignment
  EXPECT_EQ(reinterpret_cast<uintptr_t>(int_ptr) % alignof(int), 0);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(double_ptr) % alignof(double), 0);
}

// Test construct with custom class
class TestClass {
 public:
  int value;
  double score;

  TestClass(int v, double s) : value(v), score(s) {}

  bool operator==(const TestClass& other) const {
    return value == other.value && score == other.score;
  }
};

TEST_F(MemoryArenaTest, ConstructCustomClassTest) {
  TestClass* obj = arena->construct<TestClass>(123, 456.789);
  ASSERT_NE(obj, nullptr);

  EXPECT_EQ(obj->value, 123);
  EXPECT_DOUBLE_EQ(obj->score, 456.789);

  // Test alignment
  EXPECT_EQ(reinterpret_cast<uintptr_t>(obj) % alignof(TestClass), 0);
}

// Test reset functionality
TEST_F(MemoryArenaTest, ResetTest) {
  // Allocate some memory
  void* ptr1 = arena->allocate(100);
  void* ptr2 = arena->allocate(200);
  ASSERT_NE(ptr1, nullptr);
  ASSERT_NE(ptr2, nullptr);

  size_t allocated_before = arena->get_total_allocated();
  size_t chunks_before = arena->get_chunk_count();

  EXPECT_GT(allocated_before, 0);

  // Reset the arena
  arena->reset();

  // Check that statistics are reset
  EXPECT_EQ(arena->get_total_allocated(), 0);
  EXPECT_EQ(arena->get_chunk_count(), chunks_before);  // Chunks should remain

  // Should be able to allocate again from the beginning
  void* ptr3 = arena->allocate(50);
  ASSERT_NE(ptr3, nullptr);

  EXPECT_EQ(arena->get_total_allocated(), 50);
}

// Test clear functionality
TEST_F(MemoryArenaTest, ClearTest) {
  // Allocate some memory
  void* ptr1 = arena->allocate(100);
  void* ptr2 = arena->allocate(200);
  ASSERT_NE(ptr1, nullptr);
  ASSERT_NE(ptr2, nullptr);

  EXPECT_GT(arena->get_total_allocated(), 0);
  EXPECT_GT(arena->get_chunk_count(), 0);

  // Clear the arena
  arena->clear();

  // Everything should be reset
  EXPECT_EQ(arena->get_total_allocated(), 0);
  EXPECT_EQ(arena->get_chunk_count(), 0);

  // Should be able to allocate again (will create new chunk)
  void* ptr3 = arena->allocate(50);
  ASSERT_NE(ptr3, nullptr);

  EXPECT_EQ(arena->get_total_allocated(), 50);
  EXPECT_EQ(arena->get_chunk_count(), 1);
}

// Test edge cases
TEST_F(MemoryArenaTest, EdgeCasesTest) {
  // Test zero-size allocation
  void* ptr_zero = arena->allocate(0);
  EXPECT_NE(ptr_zero, nullptr);  // Should still return valid pointer

  // Test very large alignment
  void* ptr_large_align = arena->allocate(8, 128);
  ASSERT_NE(ptr_large_align, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr_large_align) % 128, 0);

  // Test power-of-2 alignments
  for (size_t align = 1; align <= 64; align *= 2) {
    void* ptr = arena->allocate(16, align);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % align, 0);
  }
}

// Test memory usage patterns
TEST_F(MemoryArenaTest, MemoryUsageTest) {
  // Test fragmentation behavior
  std::vector<void*> ptrs;

  // Allocate many small objects
  for (int i = 0; i < 100; ++i) {
    void* ptr = arena->allocate(10 + (i % 20));  // Varying sizes
    ASSERT_NE(ptr, nullptr);
    ptrs.push_back(ptr);
  }

  // All pointers should be unique
  std::sort(ptrs.begin(), ptrs.end());
  auto it = std::unique(ptrs.begin(), ptrs.end());
  EXPECT_EQ(it, ptrs.end());  // No duplicates

  // Check that total allocated makes sense
  size_t total_allocated = arena->get_total_allocated();
  EXPECT_GT(total_allocated, 100 * 10);  // At least 100 * min_size
  EXPECT_LT(total_allocated, 100 * 30);  // Less than 100 * max_size
}

// Test statistics accuracy
TEST_F(MemoryArenaTest, StatisticsTest) {
  EXPECT_EQ(arena->get_total_allocated(), 0);
  EXPECT_EQ(arena->get_chunk_count(), 1);  // Constructor creates one chunk

  // Allocate known amounts
  arena->allocate(100);
  EXPECT_EQ(arena->get_total_allocated(), 100);

  arena->allocate(200);
  EXPECT_EQ(arena->get_total_allocated(), 300);

  arena->allocate(50);
  EXPECT_EQ(arena->get_total_allocated(), 350);

  // Force new chunk with large allocation
  arena->allocate(2000);
  EXPECT_EQ(arena->get_total_allocated(), 2350);
  EXPECT_EQ(arena->get_chunk_count(), 2);
}

// Performance test - allocate many small objects quickly
TEST_F(MemoryArenaTest, PerformanceTest) {
  const int num_allocations = 10000;

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_allocations; ++i) {
    void* ptr = arena->allocate(64);  // 64-byte allocations
    ASSERT_NE(ptr, nullptr);

    // Touch the memory to ensure it's real
    *static_cast<char*>(ptr) = static_cast<char>(i % 256);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << "Allocated " << num_allocations << " objects in "
            << duration.count() << " microseconds\n";
  std::cout << "Average: "
            << (static_cast<double>(duration.count()) / num_allocations)
            << " microseconds per allocation\n";

  // Should be very fast - less than 1 microsecond per allocation on average
  EXPECT_LT(duration.count(), num_allocations);  // Less than 1μs per allocation
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}