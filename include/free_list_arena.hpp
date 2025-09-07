#ifndef FREE_LIST_ARENA_HPP
#define FREE_LIST_ARENA_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <ranges>
#include <set>
#include <vector>

#include "logger.hpp"
#include "mem_arena.hpp"
#include "mem_utils.hpp"

namespace tundradb {

/**
 * Block header stored before each allocated block
 * Contains metadata needed for free list management
 */
struct BlockHeader {
  size_t size;   // Size of the data block (excluding header)
  bool is_free;  // Whether this block is free
  static constexpr size_t HEADER_SIZE = 16;  // 8 + 1 + 7(padding) = 16
  static constexpr size_t ALIGNMENT = 8;
};

/**
 * Arena with free list support for variable-sized allocations
 * Provides O(log n) allocation/deallocation with block reuse
 *
 * Use cases:
 * - Variable-length strings that get updated frequently
 * - Objects that need individual deallocation
 * - Memory pools where fragmentation is acceptable
 */
class FreeListArena : public MemArena {
 public:
  explicit FreeListArena(
      size_t initial_size = 1024 * 1024,  // 1MB default
      size_t min_fragment_size = 64)      // 64 bytes minimum fragment
      : chunk_size_(initial_size),
        current_chunk_size_(0),
        min_fragment_size_(min_fragment_size) {
    allocate_new_chunk(chunk_size_);
  }

  ~FreeListArena() override { clear(); }

  // Non-copyable but movable
  FreeListArena(const FreeListArena&) = delete;
  FreeListArena& operator=(const FreeListArena&) = delete;
  FreeListArena(FreeListArena&&) = default;
  FreeListArena& operator=(FreeListArena&&) = default;

  /**
   * Allocate memory with free list reuse
   * @param size Number of bytes to allocate
   * @param alignment Memory alignment requirement (default: 8 bytes)
   * @return Pointer to allocated memory, or nullptr if allocation fails
   */
  void* allocate(size_t size, size_t alignment = 8) override {
    size = align_up(size, alignment);

    // Try to find a suitable free block first
    void* reused_block = find_free_block(size);
    if (reused_block) {
      return reused_block;
    }

    // No suitable free block, allocate new memory
    return allocate_new_block(size, alignment);
  }

  /**
   * Deallocate a block and add it to the free list
   * @param ptr Pointer returned by allocate()
   */
  void deallocate(void* ptr) override {
    if (!ptr) return;

    // Get the block header
    BlockHeader* header = get_block_header(ptr);

    assert(!header->is_free && "Double free detected");

    // log_debug("DEALLOCATE: ptr={}, header={}, size={}", ptr,
    //           static_cast<void*>(header), header->size);

    // Mark as free (coalesce_blocks will handle adding to free list)
    header->is_free = true;

    // Try to coalesce with adjacent blocks
    coalesce_blocks(ptr);

    freed_bytes_ += header->size;  // For fragmentation ratio calculation
    total_used_ -= header->size;   // Decrement live memory usage

    // log_debug("DEALLOCATE DONE: free_block_count={}",
    // get_free_block_count());
  }

  /**
   * Reset the arena - clears all allocations and free lists
   */
  void reset() override {
    // Clear free lists
    free_blocks_by_size_.clear();

    // Reset all chunk allocated sizes
    for (size_t i = 0; i < chunk_allocated_sizes_.size(); ++i) {
      chunk_allocated_sizes_[i] = 0;
    }

    // Reset chunk management to use first chunk
    current_offset_ = 0;
    if (!chunks_.empty()) {
      current_chunk_ = chunks_[0].get();
      current_chunk_size_ = chunk_sizes_[0];
    }

    // Reset statistics
    total_used_ = 0;  // Reset individual block usage (can be reused)
    freed_bytes_ = 0;
    // NOTE: total_allocated_ (chunk memory) is NOT reset - chunks are still
    // allocated
  }

  /**
   * Clear all allocated memory
   */
  void clear() override {
    chunks_.clear();
    chunk_sizes_.clear();
    chunk_allocated_sizes_.clear();
    free_blocks_by_size_.clear();
    current_chunk_ = nullptr;
    current_chunk_size_ = 0;
    current_offset_ = 0;
    total_allocated_ = 0;  // Reset chunk memory (chunks are freed)
    total_used_ = 0;       // Reset individual block usage
    freed_bytes_ = 0;
  }

  /**
   * STATISTICS DOCUMENTATION:
   *
   * total_allocated_: Total chunk memory allocated (never decreases except
   * clear()) total_used_:      Sum of sizes of individual used blocks
   * (decreases on deallocate) freed_bytes_:     Cumulative bytes freed (for
   * fragmentation ratio calculation) used_bytes:       Currently used memory
   * (same as total_used_)
   *
   * Example flow:
   * allocate(100) → total_allocated=1024, used_bytes=100, freed_bytes=0
   * allocate(200) → total_allocated=1024, used_bytes=300, freed_bytes=0
   * deallocate(100) → total_allocated=1024, used_bytes=200, freed_bytes=100
   * reset() → total_allocated=1024, used_bytes=0, freed_bytes=0 (chunks kept)
   * clear() → total_allocated=0, used_bytes=0, freed_bytes=0 (chunks freed)
   */

  // Statistics
  size_t get_total_allocated() const override {
    return total_allocated_;
  }  // Total chunk memory
  size_t get_freed_bytes() const {
    return freed_bytes_;
  }  // Cumulative deallocations (for fragmentation)
  size_t get_used_bytes() const {
    return total_used_;
  }  // Currently used memory
  size_t get_chunk_count() const override { return chunks_.size(); }
  size_t get_free_block_count() const {
    size_t count = 0;
    for (const auto& blocks : free_blocks_by_size_ | std::views::values) {
      count += blocks.size();
    }
    return count;
  }

  double get_fragmentation_ratio() const {
    if (total_allocated_ == 0) return 0.0;
    return static_cast<double>(freed_bytes_) / total_allocated_;
  }

  // Testing interface - expose internals for verification
#ifdef TESTING_ENABLED
  const std::map<size_t, std::set<BlockHeader*>>& get_free_blocks_for_testing()
      const {
    return free_blocks_by_size_;
  }

  const std::vector<size_t>& get_chunk_sizes_for_testing() const {
    return chunk_sizes_;
  }

  BlockHeader* get_block_header_for_testing(void* ptr) {
    return get_block_header(ptr);
  }

  bool verify_block_integrity_for_testing() {
    // Verify that all blocks in chunks are properly laid out in memory
    for (size_t i = 0; i < chunks_.size(); ++i) {
      char* chunk_start = chunks_[i].get();
      char* chunk_allocated_end = chunk_start + chunk_allocated_sizes_[i];

      char* current_ptr = chunk_start;
      while (current_ptr < chunk_allocated_end) {
        BlockHeader* current = reinterpret_cast<BlockHeader*>(current_ptr);

        // Verify block is within allocated bounds
        char* block_end =
            current_ptr + BlockHeader::HEADER_SIZE + current->size;
        if (block_end > chunk_allocated_end) {
          return false;  // Block extends beyond allocated space
        }

        // Move to next block using physical traversal
        current_ptr = block_end;
      }
    }
    return true;
  }

  size_t get_min_fragment_size_for_testing() const {
    return min_fragment_size_;
  }
#endif

 private:
  // Chunk management with variable sizes
  std::vector<std::unique_ptr<char[]>> chunks_;
  std::vector<size_t> chunk_sizes_;  // Track actual size of each chunk
  std::vector<size_t>
      chunk_allocated_sizes_;  // Track allocated portion of each chunk
  char* current_chunk_ = nullptr;
  size_t chunk_size_;          // Default chunk size
  size_t current_chunk_size_;  // Size of current chunk
  size_t current_offset_ = 0;
  size_t min_fragment_size_;  // Minimum fragment size for splitting

  // Free list management: size -> set of free block headers
  std::map<size_t, std::set<BlockHeader*>> free_blocks_by_size_;

  // Statistics
  size_t total_allocated_ =
      0;  // Total chunk memory allocated (never decreases except clear())
  size_t total_used_ =
      0;  // Sum of sizes of individual live blocks (decreases on deallocate)
  size_t freed_bytes_ =
      0;  // Cumulative bytes freed (for fragmentation ratio calculation)

  void allocate_new_chunk(size_t size) {
    auto new_chunk = std::make_unique<char[]>(size);
    current_chunk_ = new_chunk.get();
    current_chunk_size_ = size;
    chunks_.push_back(std::move(new_chunk));
    chunk_sizes_.push_back(size);
    chunk_allocated_sizes_.push_back(0);  // Start with 0 allocated
    current_offset_ = 0;

    // Track total chunk memory allocated (persists across reset)
    total_allocated_ += size;
  }

  // Find which chunk contains this pointer
  char* find_chunk_start(void* ptr) {
    char* char_ptr = static_cast<char*>(ptr);
    for (size_t i = 0; i < chunks_.size(); ++i) {
      char* chunk_start = chunks_[i].get();
      char* chunk_end = chunk_start + chunk_sizes_[i];
      if (char_ptr >= chunk_start && char_ptr < chunk_end) {
        return chunk_start;
      }
    }
    return nullptr;
  }

  // Find previous block by traversing physically through memory
  BlockHeader* find_prev_block(BlockHeader* target) {
    char* chunk_start = find_chunk_start(target);
    if (!chunk_start) return nullptr;

    char* target_ptr = reinterpret_cast<char*>(target);
    if (target_ptr == chunk_start) {
      return nullptr;  // First block in chunk has no previous
    }

    // Walk through memory from chunk start to find previous block
    char* current_ptr = chunk_start;
    BlockHeader* prev = nullptr;

    while (current_ptr < target_ptr) {
      BlockHeader* current = reinterpret_cast<BlockHeader*>(current_ptr);

      // Calculate next block position
      char* next_ptr = current_ptr + BlockHeader::HEADER_SIZE + current->size;

      if (next_ptr == target_ptr) {
        return current;  // Found the block immediately before target
      }

      prev = current;
      current_ptr = next_ptr;
    }

    return nullptr;  // Shouldn't happen if memory is properly managed
  }

  BlockHeader* get_block_header(void* ptr) {
    return reinterpret_cast<BlockHeader*>(static_cast<char*>(ptr) -
                                          BlockHeader::HEADER_SIZE);
  }

  void* allocate_new_block(size_t size, size_t alignment) {
    // Align the requested size
    size_t aligned_size = align_up(size, alignment);

    // Calculate the aligned offset for the data portion
    size_t data_aligned_offset = calculate_aligned_offset(
        current_chunk_, current_offset_ + BlockHeader::HEADER_SIZE, alignment);

    size_t header_offset = data_aligned_offset - BlockHeader::HEADER_SIZE;
    size_t total_size = data_aligned_offset + aligned_size - current_offset_;

    // Check if we need a new chunk
    if (current_chunk_ == nullptr ||
        current_offset_ + total_size > current_chunk_size_) {
      size_t needed_chunk_size =
          std::max(chunk_size_, total_size) + get_alignment_overhead(alignment);
      allocate_new_chunk(needed_chunk_size);

      // Recalculate offsets for new chunk
      data_aligned_offset = calculate_aligned_offset(
          current_chunk_, BlockHeader::HEADER_SIZE, alignment);
      header_offset = data_aligned_offset - BlockHeader::HEADER_SIZE;
      total_size = data_aligned_offset + aligned_size;
    }

    // Place header and data at properly aligned positions
    char* header_start = current_chunk_ + header_offset;
    BlockHeader* header = reinterpret_cast<BlockHeader*>(header_start);

    header->size = aligned_size;  // Store the aligned size
    header->is_free = false;

    char* data_ptr = current_chunk_ + data_aligned_offset;
    // log_debug(
    //     "ALLOCATE_NEW: data_ptr={}, header={}, size={}, total_size={}, "
    //     "offset={}",
    //     data_ptr, static_cast<void*>(header), aligned_size, total_size,
    //     current_offset_);

    current_offset_ = data_aligned_offset + aligned_size;
    // Track individual block allocation (live memory usage)
    total_used_ += aligned_size;

    // Update allocated size for current chunk
    chunk_allocated_sizes_.back() = current_offset_;

    return data_ptr;
  }

  void* find_free_block(size_t size) {
    // Find best fit from free list
    auto it = free_blocks_by_size_.lower_bound(size);
    if (it != free_blocks_by_size_.end()) {
      auto& blocks = it->second;
      if (!blocks.empty()) {
        BlockHeader* header = *blocks.begin();
        blocks.erase(blocks.begin());

        if (blocks.empty()) {
          free_blocks_by_size_.erase(it);
        }

        // Split block if it's much larger
        if (header->size >
            size + BlockHeader::HEADER_SIZE + min_fragment_size_) {
          split_block(header, size);
        }

        header->is_free = false;
        return reinterpret_cast<char*>(header) + BlockHeader::HEADER_SIZE;
      }
    }

    return nullptr;
  }

  void split_block(BlockHeader* header, size_t needed_size) {
    size_t remaining_size =
        header->size - needed_size - BlockHeader::HEADER_SIZE;

    // Create new block from remainder
    char* new_block_start = reinterpret_cast<char*>(header) +
                            BlockHeader::HEADER_SIZE + needed_size;
    BlockHeader* new_header = reinterpret_cast<BlockHeader*>(new_block_start);

    new_header->size = remaining_size;
    new_header->is_free = true;

    header->size = needed_size;

    // Add remainder to free list (pass data pointer to add_to_free_list)
    add_to_free_list(new_block_start + BlockHeader::HEADER_SIZE,
                     remaining_size);
  }

  void add_to_free_list(void* ptr, size_t size) {
    BlockHeader* header = get_block_header(ptr);
    // log_debug("ADD_TO_FREE_LIST: ptr={}, header={}, size={}", ptr,
    //           static_cast<void*>(header), size);
    free_blocks_by_size_[size].insert(header);
  }

  void remove_block_from_free_list(BlockHeader* block) {
    auto it = free_blocks_by_size_.find(block->size);
    if (it != free_blocks_by_size_.end()) {
      it->second.erase(block);
      if (it->second.empty()) {
        free_blocks_by_size_.erase(it);
      }
      // log_debug("COALESCE: removed block from free list");
    }
  }

  // Find the next block using physical adjacency
  BlockHeader* find_next_block(BlockHeader* header) {
    // Need byte-level arithmetic (header + header->size would be wrong pointer
    // math)
    char* current_ptr = reinterpret_cast<char*>(header);
    char* next_ptr = current_ptr + BlockHeader::HEADER_SIZE + header->size;

    // Check if the next block is within the same chunk
    char* chunk_start = find_chunk_start(header);
    if (!chunk_start) {
      // Defensive check - should not happen with valid blocks
      // log_debug("FIND_NEXT: header={}, no chunk found",
      //           static_cast<void*>(header));
      return nullptr;
    }

    size_t chunk_index = 0;
    for (size_t i = 0; i < chunks_.size(); ++i) {
      if (chunks_[i].get() == chunk_start) {
        chunk_index = i;
        break;
      }
    }

    char* chunk_allocated_end =
        chunk_start + chunk_allocated_sizes_[chunk_index];

    // log_debug(
    //     "FIND_NEXT: header={}, next_ptr={}, chunk_start={}, "
    //     "chunk_allocated_end={}",
    //     static_cast<void*>(header), next_ptr, chunk_start,
    //     chunk_allocated_end);

    // Check if next block would be within allocated portion of chunk
    // Must ensure entire header fits (not just start position)
    if (next_ptr + BlockHeader::HEADER_SIZE <= chunk_allocated_end) {
      BlockHeader* next_header = reinterpret_cast<BlockHeader*>(next_ptr);
      // Using physical traversal - don't rely on header.next pointer
      // log_debug("FIND_NEXT: found next block={}, size={}, is_free={}",
      //           static_cast<void*>(next_header), next_header->size,
      //           next_header->is_free);
      return next_header;
    }

    // log_debug("FIND_NEXT: next block would be outside allocated portion");
    return nullptr;  // Next block would be outside allocated portion
  }

  void coalesce_blocks(void* ptr) {
    BlockHeader* header = get_block_header(ptr);

    // log_debug("COALESCE START: ptr={}, header={}, size={}", ptr,
    //           static_cast<void*>(header), header->size);

    // Coalesce with next block (forward coalescing)
    BlockHeader* next = find_next_block(header);
    if (next && next->is_free) {
      // log_debug("COALESCE: merging with NEXT block={}, size={}",
      //           static_cast<void*>(next), next->size);

      // Remove next block from free list
      remove_block_from_free_list(next);

      // Merge blocks
      size_t old_size = header->size;
      header->size += BlockHeader::HEADER_SIZE + next->size;
      // log_debug("COALESCE: merged forward - old_size={}, new_size={}",
      // old_size,
      //           header->size);
    } else {
      // log_debug("COALESCE: no next block to merge (next={}, is_free={})",
      //           static_cast<void*>(next), next ? next->is_free : false);
    }

    // Coalesce with previous block (backward coalescing)
    BlockHeader* prev = find_prev_block(header);
    if (prev && prev->is_free) {
      // log_debug("COALESCE: merging with PREV block={}, size={}",
      //           static_cast<void*>(prev), prev->size);

      // Remove prev block from free list
      remove_block_from_free_list(prev);

      // Merge blocks
      size_t old_size = prev->size;
      prev->size += BlockHeader::HEADER_SIZE + header->size;
      // log_debug("COALESCE: merged backward - old_size={}, new_size={}",
      //           old_size, prev->size);

      // Update header to point to merged block
      header = prev;
    } else {
      // log_debug("COALESCE: no prev block to merge (prev={}, is_free={})",
      //           static_cast<void*>(prev), prev ? prev->is_free : false);
    }

    // Add the coalesced block back to free list
    // log_debug("COALESCE: adding final block to free list: header={},
    // size={}",
    //           static_cast<void*>(header), header->size);
    add_to_free_list(reinterpret_cast<char*>(header) + BlockHeader::HEADER_SIZE,
                     header->size);
  }
};

}  // namespace tundradb

#endif  // FREE_LIST_ARENA_HPP