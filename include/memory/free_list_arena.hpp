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

#include "common/logger.hpp"
#include "memory/mem_arena.hpp"
#include "memory/mem_utils.hpp"

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
      size_t min_fragment_size = 64);     // 64 bytes minimum fragment

  ~FreeListArena() override { FreeListArena::clear(); }

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
  void* allocate(size_t size, size_t alignment = 8) override;

  /**
   * Deallocate a block and add it to the free list
   * @param ptr Pointer returned by allocate()
   */
  void deallocate(void* ptr) override;

  /**
   * Reset the arena - clears all allocations and free lists
   */
  void reset() override;

  /**
   * Clear all allocated memory
   */
  void clear() override;

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
   * allocate(100) -> total_allocated=1024, used_bytes=100, freed_bytes=0
   * allocate(200) -> total_allocated=1024, used_bytes=300, freed_bytes=0
   * deallocate(100) -> total_allocated=1024, used_bytes=200, freed_bytes=100
   * reset() -> total_allocated=1024, used_bytes=0, freed_bytes=0 (chunks kept)
   * clear() -> total_allocated=0, used_bytes=0, freed_bytes=0 (chunks freed)
   */

  // Statistics
  /// Total bytes reserved in all chunks (unchanged by reset; cleared by
  /// clear()).
  size_t get_total_allocated() const override {
    return total_allocated_;
  }  // Total chunk memory
  /// Cumulative bytes returned to the free list (used for fragmentation
  /// metrics).
  size_t get_freed_bytes() const {
    return freed_bytes_;
  }  // Cumulative deallocations (for fragmentation)
  /// Sum of live allocated block sizes (decreases on deallocate).
  size_t get_used_bytes() const {
    return total_used_;
  }  // Currently used memory
  /// Number of allocated chunks.
  size_t get_chunk_count() const override { return chunks_.size(); }
  /// Number of distinct free blocks indexed in the size map.
  size_t get_free_block_count() const;

  /// Ratio of cumulative freed bytes to total chunk memory (0 if no chunks).
  double get_fragmentation_ratio() const;

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
    for (size_t i = 0; i < chunks_.size(); ++i) {
      char* chunk_start = chunks_[i].get();
      char* chunk_allocated_end = chunk_start + chunk_allocated_sizes_[i];

      char* current_ptr = chunk_start;
      while (current_ptr < chunk_allocated_end) {
        BlockHeader* current = reinterpret_cast<BlockHeader*>(current_ptr);

        char* block_end =
            current_ptr + BlockHeader::HEADER_SIZE + current->size;
        if (block_end > chunk_allocated_end) {
          return false;
        }

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

  void allocate_new_chunk(size_t size);
  char* find_chunk_start(void* ptr);
  BlockHeader* find_prev_block(BlockHeader* target);

  BlockHeader* get_block_header(void* ptr) {
    return reinterpret_cast<BlockHeader*>(static_cast<char*>(ptr) -
                                          BlockHeader::HEADER_SIZE);
  }

  void* allocate_new_block(size_t size, size_t alignment);
  void* find_free_block(size_t size);
  void split_block(BlockHeader* header, size_t needed_size);
  void add_to_free_list(void* ptr, size_t size);
  void remove_block_from_free_list(BlockHeader* block);
  BlockHeader* find_next_block(BlockHeader* header);
  void coalesce_blocks(void* ptr);
};

}  // namespace tundradb

#endif  // FREE_LIST_ARENA_HPP
