#ifndef STRING_ARENA_HPP
#define STRING_ARENA_HPP

#include <arrow/result.h>
#include <arrow/status.h>
#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "common/value_type.hpp"  // ValueType enum and helpers
#include "memory/free_list_arena.hpp"
#include "memory/memory_arena.hpp"
#include "memory/string_ref.hpp"  // StringRef class

namespace tundradb {

// Forward declarations
class StringPool;
class StringArena;

/**
 * String pool for strings of a specific maximum size.
 * Uses FreeListArena for efficient allocation and deallocation.
 *
 * Each pool handles strings up to a certain size:
 * - Pool 0: strings up to 16 bytes (FIXED_STRING16)
 * - Pool 1: strings up to 32 bytes (FIXED_STRING32)
 * - Pool 2: strings up to 64 bytes (FIXED_STRING64)
 * - Pool 3: unlimited size strings (STRING)
 *
 * Features:
 * - Optional deduplication (same string content shares memory)
 * - Intrusive reference counting (no separate metadata map)
 * - Thread-safe concurrent access
 * - Automatic cleanup when ref count reaches zero
 */
class StringPool {
 public:
  /**
   * Create a string pool.
   *
   * @param max_size Maximum string size this pool handles (SIZE_MAX for
   * unlimited)
   * @param initial_arena_size Initial arena size (default: 1MB)
   */
  explicit StringPool(size_t max_size, size_t initial_arena_size = 1024 * 1024)
      : max_size_(max_size),
        arena_(std::make_unique<FreeListArena>(initial_arena_size, 16)) {}

  // Non-copyable (contains mutex)
  StringPool(const StringPool&) = delete;
  StringPool& operator=(const StringPool&) = delete;

  /**
   * Store a string in this pool with embedded ref count.
   * Thread-safe: Multiple threads can call this concurrently.
   *
   * Memory layout created:
   * [ref_count=1][length][flags=0][padding][string data]
   *
   * @param str String to store
   * @param pool_id Pool identifier (for StringRef)
   * @return Ok(StringRef) with ref_count = 1, or Error with reason
   */
  arrow::Result<StringRef> store_string(const std::string& str,
                                        uint32_t pool_id);

  /**
   * Store a string view in this pool.
   */
  arrow::Result<StringRef> store_string(std::string_view str,
                                        uint32_t pool_id) {
    return store_string(std::string(str), pool_id);
  }

  /**
   * Mark a string for deletion (called when node field is updated).
   * The string will be deallocated when the last StringRef is destroyed.
   *
   * @param data Pointer to string data (NOT to header)
   */
  void mark_for_deletion(const char* data);

  /**
   * Deallocate a string's memory back to the FreeListArena.
   * Called by StringRef::release() AFTER it has already decremented
   * ref_count to 0 and confirmed is_marked_for_deletion().
   *
   * This method MUST NOT touch ref_count (the caller already did).
   *
   * Thread-safe: Multiple threads can call this concurrently.
   *
   * @param data Pointer to string data (NOT to header)
   */
  void release_string(const char* data);

  /**
   * Enable or disable string deduplication.
   * When enabled, identical strings share the same memory.
   */
  void enable_deduplication(bool enable = true);

  // ========================================================================
  // STATISTICS
  // ========================================================================

  size_t get_max_size() const { return max_size_; }

  /** Number of live (allocated but not yet freed) strings in this pool. */
  int64_t get_active_allocs() const {
    return active_allocs_.load(std::memory_order_relaxed);
  }

  size_t get_total_allocated() const { return arena_->get_total_allocated(); }

  size_t get_used_bytes() const;

  size_t get_string_count() const { return dedup_cache_.size(); }

  /**
   * Get total reference count across all strings (for debugging).
   */
  size_t get_total_references() const;

  /**
   * Reset the pool - clears all allocations.
   */
  void reset() {
    arena_->reset();
    dedup_cache_.clear();
  }

  /**
   * Clear all memory.
   */
  void clear() {
    arena_->clear();
    dedup_cache_.clear();
  }

 private:
  size_t max_size_;
  std::unique_ptr<FreeListArena> arena_;
  mutable std::mutex
      arena_mutex_;  // Protects arena_ (FreeListArena is NOT thread-safe)
  bool enable_deduplication_ = false;
  std::atomic<int64_t> active_allocs_{0};

  // Deduplication cache: string content -> StringRef (holds one reference)
  // Thread-safe concurrent hash map from Intel TBB
  tbb::concurrent_hash_map<std::string, StringRef> dedup_cache_;
};

/**
 * Multi-pool string arena that manages strings of different sizes.
 * Routes strings to appropriate pools based on their size and type.
 *
 * This is the main interface for string storage in TundraDB.
 *
 * Usage:
 *   StringArena arena;
 *   arena.enable_deduplication(true);
 *
 *   // Store a string
 *   StringRef name = arena.store_string("Alice", ValueType::FIXED_STRING16);
 *
 *   // Share it across nodes (copy increments ref count)
 *   node1->set_field("name", name);  // ref_count = 2
 *   node2->set_field("friend", name); // ref_count = 3
 *
 *   // Mark for deletion when updating
 *   StringRef old_name = node1->get_field_string("name");
 *   arena.mark_for_deletion(old_name);
 *
 *   // Set new value
 *   StringRef new_name = arena.store_string("Bob");
 *   node1->set_field("name", new_name);
 *
 *   // Old string is deallocated when last StringRef is destroyed
 */
class StringArena {
 public:
  /**
   * Create a multi-pool string arena with 4 pools:
   * - Pool 0: strings up to 16 bytes
   * - Pool 1: strings up to 32 bytes
   * - Pool 2: strings up to 64 bytes
   * - Pool 3: unlimited size strings
   */
  StringArena();

  /**
   * Store a string in a specific pool by ID.
   *
   * @param str String to store
   * @param pool_id Pool identifier (0-3), default is pool 3 (unlimited size)
   * @return Ok(StringRef) with ref_count = 1, or Error with reason
   */
  arrow::Result<StringRef> store_string(const std::string& str,
                                        uint32_t pool_id = 3);

  /**
   * Store a string, automatically choosing the best pool.
   * Picks the smallest pool that can fit the string.
   */
  arrow::Result<StringRef> store_string_auto(const std::string& str);

  /**
   * Mark a string for deletion.
   * The string will be deallocated when the last StringRef is destroyed.
   *
   * @param ref StringRef to mark for deletion
   */
  void mark_for_deletion(const StringRef& ref);

  /**
   * Get string content from reference (zero-copy view).
   */
  std::string_view get_string_view(const StringRef& ref) const {
    return ref.view();
  }

  /**
   * Enable or disable deduplication for all pools.
   */
  void enable_deduplication(bool enable = true);

  /** Total live (allocated but not freed) strings across all pools. */
  int64_t get_active_allocs() const;

  /**
   * Get pool by ID.
   */
  StringPool* get_pool(uint32_t pool_id) const;

  /**
   * Reset all pools.
   */
  void reset();

  /**
   * Clear all memory.
   */
  void clear();

  /**
   * Register pools with the global registry.
   * Defined below StringArenaRegistry to avoid forward-declaration issues.
   */
  void register_pools();

 private:
  std::vector<std::unique_ptr<StringPool>> pools_;
};

// ============================================================================
// Global registry for StringRef to find pools during deallocation
// ============================================================================

/**
 * Global registry that maps pool IDs to StringPool instances.
 * This allows StringRef::release() to find the correct pool for deallocation
 * without storing a pool pointer in each StringRef (saves 8 bytes per ref).
 */
class StringArenaRegistry {
 private:
  static StringArenaRegistry& instance() {
    static StringArenaRegistry registry;
    return registry;
  }

  tbb::concurrent_hash_map<uint32_t, StringPool*> pool_map_;

 public:
  /**
   * Register a pool with the global registry.
   * This is called automatically by StringArena.
   */
  static void register_pool(uint32_t pool_id, StringPool* pool) {
    typename decltype(pool_map_)::accessor acc;
    instance().pool_map_.insert(acc, pool_id);
    acc->second = pool;
  }

  static StringPool* get_pool(uint32_t pool_id);

  /**
   * Release a string reference.
   * Called by StringRef::release() to deallocate the string.
   */
  static void release_string(uint32_t pool_id, const char* data);
};

}  // namespace tundradb

#endif  // STRING_ARENA_HPP
