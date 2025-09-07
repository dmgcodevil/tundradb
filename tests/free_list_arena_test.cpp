#define TESTING_ENABLED
#include "../include/free_list_arena.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <random>
#include <vector>

using namespace tundradb;

class FreeListArenaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create arena with small chunks for easier testing
    arena = std::make_unique<FreeListArena>(
        1024, 64);  // 1KB chunks, 64-byte min fragment
  }

  void TearDown() override { arena.reset(); }

  std::unique_ptr<FreeListArena> arena;
};

// Basic allocation and deallocation tests
TEST_F(FreeListArenaTest, BasicAllocation) {
  void* ptr1 = arena->allocate(100);
  ASSERT_NE(ptr1, nullptr);

  void* ptr2 = arena->allocate(200);
  ASSERT_NE(ptr2, nullptr);

  // Pointers should be different
  EXPECT_NE(ptr1, ptr2);

  // Should be able to write to allocated memory
  char* char_ptr = static_cast<char*>(ptr1);
  for (int i = 0; i < 100; ++i) {
    char_ptr[i] = static_cast<char>(i % 256);
  }

  // Verify data integrity
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(char_ptr[i], static_cast<char>(i % 256));
  }
}

TEST_F(FreeListArenaTest, AllocationAlignment) {
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

TEST_F(FreeListArenaTest, BasicDeallocation) {
  size_t initial_used = arena->get_used_bytes();  // Should be 0 initially

  void* ptr1 = arena->allocate(96);   // 96 is already 8-byte aligned
  void* ptr2 = arena->allocate(200);  // 200 is already 8-byte aligned

  // After allocation: used should be 296 bytes
  EXPECT_EQ(arena->get_used_bytes(), initial_used + 296);

  arena->deallocate(ptr1);

  // After deallocating 96 bytes: used should be 200 bytes
  EXPECT_EQ(arena->get_freed_bytes(), 96);
  EXPECT_EQ(arena->get_used_bytes(), initial_used + 200);

  arena->deallocate(ptr2);

  // After deallocating everything: used should be 0 bytes
  EXPECT_EQ(arena->get_freed_bytes(), 296);
  EXPECT_EQ(arena->get_used_bytes(), initial_used);
}

TEST_F(FreeListArenaTest, FreeListManagement) {
  // Allocate blocks with spacers to prevent coalescing
  void* ptr1 = arena->allocate(96);     // 8-byte aligned
  void* spacer1 = arena->allocate(32);  // Keep allocated to prevent coalescing
  void* ptr2 = arena->allocate(200);    // 8-byte aligned
  void* spacer2 = arena->allocate(32);  // Keep allocated to prevent coalescing
  void* ptr3 = arena->allocate(152);    // 8-byte aligned

  arena->deallocate(ptr1);  // 96-byte free block
  arena->deallocate(ptr3);  // 152-byte free block

  auto& free_blocks = arena->get_free_blocks_for_testing();

  // Should have blocks in free list
  EXPECT_EQ(arena->get_free_block_count(), 2);

  // Check specific sizes are present
  EXPECT_TRUE(free_blocks.find(96) != free_blocks.end());
  EXPECT_TRUE(free_blocks.find(152) != free_blocks.end());

  // Deallocate remaining block (spacers prevent coalescing)
  arena->deallocate(ptr2);
  EXPECT_EQ(arena->get_free_block_count(), 3);

  // Clean up spacers
  arena->deallocate(spacer1);
  arena->deallocate(spacer2);
}

TEST_F(FreeListArenaTest, BlockReuse) {
  // Allocate and deallocate
  void* ptr1 = arena->allocate(96);  // 8-byte aligned
  arena->deallocate(ptr1);

  // Allocate same size - should reuse the block
  void* ptr2 = arena->allocate(96);
  EXPECT_EQ(ptr1, ptr2);  // Should be exact same pointer

  // Free list should be empty now
  EXPECT_EQ(arena->get_free_block_count(), 0);
}

TEST_F(FreeListArenaTest, BlockSplitting) {
  // Allocate a large block and deallocate it
  void* large_ptr = arena->allocate(400);
  arena->deallocate(large_ptr);

  // Allocate smaller block - should split the large one
  void* small_ptr = arena->allocate(96);  // 96 is 8-byte aligned

  // Should have one free block remaining (original 400 - 96 - header)
  size_t expected_remaining =
      400 - 96 - BlockHeader::HEADER_SIZE;  // 400 - 96 - 24 = 280
  auto& free_blocks = arena->get_free_blocks_for_testing();

  EXPECT_EQ(arena->get_free_block_count(), 1);
  EXPECT_TRUE(free_blocks.find(expected_remaining) != free_blocks.end());
}

TEST_F(FreeListArenaTest, NoSplittingForSmallFragments) {
  size_t min_fragment = arena->get_min_fragment_size_for_testing();

  // Create a block that would leave a fragment smaller than minimum
  size_t large_size = 200;
  size_t small_size = large_size - BlockHeader::HEADER_SIZE - min_fragment + 10;

  void* large_ptr = arena->allocate(large_size);
  arena->deallocate(large_ptr);

  // This allocation should NOT split because remaining fragment would be too
  // small
  void* small_ptr = arena->allocate(small_size);

  // Should have no free blocks (entire block was given)
  EXPECT_EQ(arena->get_free_block_count(), 0);

  // The block header should show the full original size was allocated
  BlockHeader* header = arena->get_block_header_for_testing(small_ptr);
  EXPECT_EQ(header->size, large_size);
}

TEST_F(FreeListArenaTest, ForwardCoalescing) {
  printf("\n=== FORWARD COALESCING TEST START ===\n");

  // Allocate three consecutive blocks
  printf("Allocating three blocks...\n");
  void* ptr1 = arena->allocate(96);  // 8-byte aligned
  void* ptr2 = arena->allocate(96);  // 8-byte aligned
  void* ptr3 = arena->allocate(96);  // 8-byte aligned

  printf("ptr1=%p, ptr2=%p, ptr3=%p\n", ptr1, ptr2, ptr3);

  // Deallocate first two blocks (should coalesce)
  printf("Deallocating ptr1...\n");
  arena->deallocate(ptr1);

  printf("Deallocating ptr2...\n");
  arena->deallocate(ptr2);

  printf("=== FORWARD COALESCING TEST END ===\n\n");

  // Should have one large free block instead of two separate ones
  size_t expected_size =
      96 + BlockHeader::HEADER_SIZE + 96;  // 96 + 24 + 96 = 216
  auto& free_blocks = arena->get_free_blocks_for_testing();

  printf("Expected size: %zu, actual free block count: %zu\n", expected_size,
         arena->get_free_block_count());
  for (const auto& [size, blocks] : free_blocks) {
    printf("Free block size: %zu, count: %zu\n", size, blocks.size());
  }

  EXPECT_EQ(arena->get_free_block_count(), 1);
  EXPECT_TRUE(free_blocks.find(expected_size) != free_blocks.end());
}

TEST_F(FreeListArenaTest, BackwardCoalescing) {
  // Allocate three consecutive blocks
  void* ptr1 = arena->allocate(96);  // 8-byte aligned
  void* ptr2 = arena->allocate(96);  // 8-byte aligned
  void* ptr3 = arena->allocate(96);  // 8-byte aligned

  // Deallocate in reverse order to test backward coalescing
  arena->deallocate(ptr2);
  arena->deallocate(ptr1);  // This should coalesce backward with ptr2

  // Should have one large free block
  size_t expected_size =
      96 + BlockHeader::HEADER_SIZE + 96;  // 96 + 24 + 96 = 216
  auto& free_blocks = arena->get_free_blocks_for_testing();

  EXPECT_EQ(arena->get_free_block_count(), 1);
  EXPECT_TRUE(free_blocks.find(expected_size) != free_blocks.end());
}

TEST_F(FreeListArenaTest, BidirectionalCoalescing) {
  // Allocate four consecutive blocks
  void* ptr1 = arena->allocate(96);  // 8-byte aligned
  void* ptr2 = arena->allocate(96);  // 8-byte aligned
  void* ptr3 = arena->allocate(96);  // 8-byte aligned
  void* ptr4 = arena->allocate(96);  // 8-byte aligned

  // Deallocate outer blocks first
  arena->deallocate(ptr1);
  arena->deallocate(ptr3);

  // Should have two separate free blocks
  EXPECT_EQ(arena->get_free_block_count(), 2);

  // Now deallocate middle block - should coalesce with both neighbors
  arena->deallocate(ptr2);

  // Should have one large free block
  size_t expected_size = 96 + BlockHeader::HEADER_SIZE + 96 +
                         BlockHeader::HEADER_SIZE + 96;  // 96+24+96+24+96 = 336
  auto& free_blocks = arena->get_free_blocks_for_testing();

  EXPECT_EQ(arena->get_free_block_count(), 1);
  EXPECT_TRUE(free_blocks.find(expected_size) != free_blocks.end());
}

TEST_F(FreeListArenaTest, ChunkManagement) {
  // Initial state should have one chunk
  EXPECT_EQ(arena->get_chunk_count(), 1);

  auto& chunk_sizes = arena->get_chunk_sizes_for_testing();
  EXPECT_EQ(chunk_sizes[0], 1024);  // Default chunk size

  // Allocate something larger than remaining space to trigger new chunk
  void* ptr1 = arena->allocate(800);  // Uses most of first chunk
  void* ptr2 = arena->allocate(500);  // Should trigger new chunk

  EXPECT_EQ(arena->get_chunk_count(), 2);
  EXPECT_EQ(chunk_sizes[1], 1024);  // Second chunk should be default size
}

TEST_F(FreeListArenaTest, LargeAllocationChunk) {
  // Allocate something larger than default chunk size
  size_t large_size = 2000;  // Larger than 1024 default
  void* ptr = arena->allocate(large_size);

  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(arena->get_chunk_count(), 2);  // Original + new large chunk

  auto& chunk_sizes = arena->get_chunk_sizes_for_testing();
  EXPECT_GE(chunk_sizes[1], large_size + BlockHeader::HEADER_SIZE);
}

TEST_F(FreeListArenaTest, BestFitAllocation) {
  // Create free blocks of different sizes with spacers to prevent coalescing
  void* ptr1 = arena->allocate(96);     // 8-byte aligned
  void* spacer1 = arena->allocate(32);  // Keep allocated to prevent coalescing
  void* ptr2 = arena->allocate(200);    // 8-byte aligned
  void* spacer2 = arena->allocate(32);  // Keep allocated to prevent coalescing
  void* ptr3 = arena->allocate(152);    // 8-byte aligned

  arena->deallocate(ptr1);  // 96-byte free block
  arena->deallocate(ptr2);  // 200-byte free block
  arena->deallocate(ptr3);  // 152-byte free block

  // Allocate 120 bytes - should use the 152-byte block (best fit)
  void* ptr4 =
      arena->allocate(120);  // Will be aligned to 120 (already aligned)

  auto& free_blocks = arena->get_free_blocks_for_testing();

  // Should still have 96 and 200 byte blocks
  EXPECT_TRUE(free_blocks.find(96) != free_blocks.end());
  EXPECT_TRUE(free_blocks.find(200) != free_blocks.end());

  // 152-byte block should be gone (or split)
  EXPECT_TRUE(free_blocks.find(152) == free_blocks.end());

  // Clean up
  arena->deallocate(spacer1);
  arena->deallocate(spacer2);
  arena->deallocate(ptr4);
}

TEST_F(FreeListArenaTest, Statistics) {
  size_t initial_chunk_memory =
      arena->get_total_allocated();  // Chunk memory (1024 initially -
                                     // pre-allocated)
  size_t initial_used = arena->get_used_bytes();  // Used memory (0 initially)

  void* ptr1 = arena->allocate(96);   // 8-byte aligned
  void* ptr2 = arena->allocate(200);  // 8-byte aligned

  // These fit in first chunk, so chunk memory unchanged
  EXPECT_EQ(arena->get_total_allocated(), initial_chunk_memory);

  // Used memory should be exactly the allocated blocks
  EXPECT_EQ(arena->get_used_bytes(), initial_used + 296);  // 96 + 200
  EXPECT_EQ(arena->get_freed_bytes(), 0);

  size_t chunk_memory_before_deallocate = arena->get_total_allocated();
  arena->deallocate(ptr1);

  // Chunk memory unchanged (chunks not freed)
  EXPECT_EQ(arena->get_total_allocated(), chunk_memory_before_deallocate);

  // Used memory decreased by deallocated block
  EXPECT_EQ(arena->get_used_bytes(), initial_used + 200);  // 200 remaining
  EXPECT_EQ(arena->get_freed_bytes(), 96);                 // 96 freed

  double frag_ratio = arena->get_fragmentation_ratio();
  EXPECT_GT(frag_ratio, 0);
  EXPECT_LT(frag_ratio, 1);

  // Now allocate enough to trigger a second chunk (1024 - 296 - headers ≈ 700
  // bytes available)
  void* ptr3 = arena->allocate(800);  // This should trigger a new chunk
  EXPECT_GT(arena->get_total_allocated(),
            initial_chunk_memory);  // Now 2048 > 1024
  EXPECT_EQ(arena->get_total_allocated(),
            initial_chunk_memory * 2);  // Should be exactly 2 chunks
}

TEST_F(FreeListArenaTest, Clear) {
  // Allocate some memory
  arena->allocate(96);   // 8-byte aligned
  arena->allocate(200);  // 8-byte aligned

  EXPECT_GT(arena->get_chunk_count(), 0);
  EXPECT_GT(arena->get_total_allocated(), 0);

  // Clear arena
  arena->clear();

  // Everything should be cleared
  EXPECT_EQ(arena->get_chunk_count(), 0);
  EXPECT_EQ(arena->get_total_allocated(), 0);
  EXPECT_EQ(arena->get_freed_bytes(), 0);
  EXPECT_EQ(arena->get_free_block_count(), 0);
}

TEST_F(FreeListArenaTest, BlockIntegrity) {
  // Allocate and deallocate various blocks
  std::vector<void*> ptrs;
  for (int i = 0; i < 10; ++i) {
    ptrs.push_back(arena->allocate(50 + i * 10));
  }

  // Deallocate every other block
  for (size_t i = 0; i < ptrs.size(); i += 2) {
    arena->deallocate(ptrs[i]);
  }

  // Verify block integrity
  EXPECT_TRUE(arena->verify_block_integrity_for_testing());

  // Allocate more blocks
  for (int i = 0; i < 5; ++i) {
    arena->allocate(30 + i * 5);
  }

  // Verify integrity again
  EXPECT_TRUE(arena->verify_block_integrity_for_testing());
}

TEST_F(FreeListArenaTest, CustomFragmentSize) {
  // Test with different fragment sizes
  auto arena32 = std::make_unique<FreeListArena>(1024, 32);
  auto arena128 = std::make_unique<FreeListArena>(1024, 128);

  EXPECT_EQ(arena32->get_min_fragment_size_for_testing(), 32);
  EXPECT_EQ(arena128->get_min_fragment_size_for_testing(), 128);

  // Test splitting behavior with different fragment sizes
  void* ptr32 = arena32->allocate(200);
  void* ptr128 = arena128->allocate(200);

  arena32->deallocate(ptr32);
  arena128->deallocate(ptr128);

  // Allocate sizes that would create different fragment sizes
  arena32->allocate(150);   // Fragment: 200-150-24 = 26 < 32, so no split
  arena128->allocate(150);  // Fragment: 200-150-24 = 26 < 128, so no split

  // Both should have no free blocks (no splitting occurred)
  EXPECT_EQ(arena32->get_free_block_count(), 0);
  EXPECT_EQ(arena128->get_free_block_count(), 0);
}

// Stress tests and edge cases
TEST_F(FreeListArenaTest, FragmentationStressTest) {
  std::vector<void*> ptrs;

  // Allocate many small blocks
  for (int i = 0; i < 100; ++i) {
    ptrs.push_back(arena->allocate(32));
  }

  // Deallocate every other block to create fragmentation
  for (size_t i = 0; i < ptrs.size(); i += 2) {
    arena->deallocate(ptrs[i]);
  }

  size_t free_blocks_after_fragmentation = arena->get_free_block_count();
  EXPECT_GT(free_blocks_after_fragmentation, 0);

  // Try to allocate blocks that should trigger coalescing
  for (size_t i = 1; i < ptrs.size(); i += 2) {
    arena->deallocate(ptrs[i]);
  }

  // After deallocating adjacent blocks, should have fewer free blocks due to
  // coalescing
  size_t free_blocks_after_coalescing = arena->get_free_block_count();
  EXPECT_LT(free_blocks_after_coalescing, free_blocks_after_fragmentation);

  // Verify integrity
  EXPECT_TRUE(arena->verify_block_integrity_for_testing());
}

TEST_F(FreeListArenaTest, ZeroSizeAllocation) {
  void* ptr = arena->allocate(0);
  // Behavior for zero-size allocation is implementation-defined
  // Should either return nullptr or a valid pointer with no usable space
  if (ptr) {
    arena->deallocate(ptr);  // Should not crash
  }
}

TEST_F(FreeListArenaTest, DoubleDeallocation) {
  void* ptr = arena->allocate(100);
  arena->deallocate(ptr);

  // Double deallocation should be caught by assertion in debug mode
  // In release mode, behavior is undefined but shouldn't crash the test
  // framework We'll skip this test in debug mode due to assertions
#ifdef NDEBUG
  // This might corrupt the arena state, so we create a separate arena
  auto test_arena = std::make_unique<FreeListArena>(1024, 64);
  void* test_ptr = test_arena->allocate(100);
  test_arena->deallocate(test_ptr);

  // Second deallocation - behavior is undefined but shouldn't crash
  // test_arena->deallocate(test_ptr);  // Uncomment if you want to test this
#endif
}

// Performance tests
TEST_F(FreeListArenaTest, AllocationPerformance) {
  const int num_allocations = 10000;
  std::vector<void*> ptrs;
  ptrs.reserve(num_allocations);

  auto start = std::chrono::high_resolution_clock::now();

  // Allocate many blocks
  for (int i = 0; i < num_allocations; ++i) {
    void* ptr = arena->allocate(64 + (i % 128));  // Variable sizes
    ptrs.push_back(ptr);
  }

  auto mid = std::chrono::high_resolution_clock::now();

  // Deallocate all blocks
  for (void* ptr : ptrs) {
    arena->deallocate(ptr);
  }

  auto end = std::chrono::high_resolution_clock::now();

  auto alloc_time =
      std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
  auto dealloc_time =
      std::chrono::duration_cast<std::chrono::microseconds>(end - mid);

  std::cout << "Allocation time for " << num_allocations
            << " blocks: " << alloc_time.count() << " μs" << std::endl;
  std::cout << "Deallocation time: " << dealloc_time.count() << " μs"
            << std::endl;
  std::cout << "Average allocation time: "
            << (static_cast<double>(alloc_time.count()) / num_allocations)
            << " μs" << std::endl;

  // Sanity check - shouldn't be too slow
  EXPECT_LT(alloc_time.count(),
            num_allocations * 10);  // Less than 10μs per allocation
}

TEST_F(FreeListArenaTest, LargeAlignmentTest) {
  // Test alignment larger than natural alignment
  void* ptr1 = arena->allocate(64, 1024);  // 1KB alignment
  ASSERT_NE(ptr1, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr1) % 1024, 0);

  void* ptr2 = arena->allocate(128, 2048);  // 2KB alignment
  ASSERT_NE(ptr2, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr2) % 2048, 0);

  void* ptr3 = arena->allocate(32, 4096);  // 4KB alignment
  ASSERT_NE(ptr3, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr3) % 4096, 0);

  // Verify all pointers are different
  EXPECT_NE(ptr1, ptr2);
  EXPECT_NE(ptr2, ptr3);
  EXPECT_NE(ptr1, ptr3);
}

TEST_F(FreeListArenaTest, CoalescingPerformance) {
  const int num_blocks = 10;
  std::vector<void*> ptrs;

  // Allocate many blocks
  for (int i = 0; i < num_blocks; ++i) {
    ptrs.push_back(arena->allocate(128));
  }

  auto start = std::chrono::high_resolution_clock::now();

  // Deallocate all blocks (triggers coalescing)
  for (void* ptr : ptrs) {
    arena->deallocate(ptr);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto coalesce_time =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << "Coalescing time for " << num_blocks
            << " blocks: " << coalesce_time.count() << " μs" << std::endl;

  // After coalescing, should have 2 merged blocks (one per chunk with 1KB chunk
  // size) With 1KB chunks, 6 blocks fit in first chunk, 4 in second chunk
  EXPECT_EQ(arena->get_free_block_count(), 2);
}

TEST_F(FreeListArenaTest, Reset) {
  // Allocate some blocks to create non-empty state
  void* ptr1 = arena->allocate(96);   // 8-byte aligned
  void* ptr2 = arena->allocate(200);  // 8-byte aligned
  void* ptr3 = arena->allocate(152);  // 8-byte aligned

  // Remember the first allocation address for later comparison
  void* original_first_ptr = ptr1;

  // Verify initial state - should have allocated memory
  EXPECT_GT(arena->get_total_allocated(), 0);
  EXPECT_EQ(arena->get_freed_bytes(), 0);
  EXPECT_GT(arena->get_used_bytes(), 0);
  EXPECT_EQ(arena->get_free_block_count(), 0);

  // Deallocate some blocks to create free blocks
  arena->deallocate(ptr1);
  arena->deallocate(ptr3);

  // Verify state after deallocation - should have free blocks and freed bytes
  EXPECT_GT(arena->get_freed_bytes(), 0);
  EXPECT_GT(arena->get_free_block_count(), 0);
  size_t freed_bytes_before_reset = arena->get_freed_bytes();
  size_t free_block_count_before_reset = arena->get_free_block_count();

  // Reset the arena
  arena->reset();

  // Verify everything is reset
  EXPECT_EQ(arena->get_total_allocated(), 1024);
  EXPECT_EQ(arena->get_freed_bytes(), 0);
  EXPECT_EQ(arena->get_used_bytes(), 0);
  EXPECT_EQ(arena->get_free_block_count(), 0);

  // Verify we had something to reset
  EXPECT_GT(freed_bytes_before_reset, 0);
  EXPECT_GT(free_block_count_before_reset, 0);

  // After reset, should be able to allocate again and get same address
  // (since we're back to offset 0 in the first chunk)
  void* new_ptr1 = arena->allocate(96);
  EXPECT_EQ(new_ptr1, original_first_ptr);  // Same address as before reset

  // Should be able to allocate more blocks normally
  void* new_ptr2 = arena->allocate(200);
  void* new_ptr3 = arena->allocate(152);
  EXPECT_NE(new_ptr2, nullptr);
  EXPECT_NE(new_ptr3, nullptr);
  EXPECT_NE(new_ptr1, new_ptr2);
  EXPECT_NE(new_ptr2, new_ptr3);

  // Statistics should reflect new allocations
  EXPECT_GT(arena->get_total_allocated(), 0);
  EXPECT_EQ(arena->get_freed_bytes(), 0);
  EXPECT_GT(arena->get_used_bytes(), 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}