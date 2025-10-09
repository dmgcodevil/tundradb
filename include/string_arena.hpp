#ifndef STRING_ARENA_HPP
#define STRING_ARENA_HPP

#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "free_list_arena.hpp"
#include "memory_arena.hpp"
#include "string_ref.hpp"  // StringRef class
#include "value_type.hpp"  // ValueType enum and helpers

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
   * @return StringRef with ref_count = 1, or null ref if allocation fails
   */
  StringRef store_string(const std::string& str, uint32_t pool_id) {
    if (str.length() > max_size_) {
      return StringRef{};  // String too large for this pool
    }

    // Check deduplication cache (thread-safe via tbb::concurrent_hash_map)
    if (enable_deduplication_) {
      typename decltype(dedup_cache_)::const_accessor acc;
      if (dedup_cache_.find(acc, str)) {
        // Found existing string - return copy (increments ref count)
        return acc->second;
      }
    }

    // CRITICAL: Lock arena allocation (FreeListArena is NOT thread-safe)
    std::lock_guard<std::mutex> lock(arena_mutex_);

    // Allocate: [header 16 bytes][string data][null terminator]
    const size_t alloc_size = StringRef::HEADER_SIZE + str.length() + 1;
    void* raw_storage = arena_->allocate(alloc_size);
    if (!raw_storage) {
      return StringRef{};  // Allocation failed
    }

    // Initialize header at the beginning
    auto* header = static_cast<StringRef::StringHeader*>(raw_storage);
    header->ref_count.store(
        0, std::memory_order_relaxed);  // Will be set to 1 by StringRef ctor
    header->length = static_cast<uint32_t>(str.length());
    header->flags = 0;
    header->padding = 0;

    // Copy string data after header
    char* data = reinterpret_cast<char*>(header) + StringRef::HEADER_SIZE;
    std::memcpy(data, str.c_str(), str.length());
    data[str.length()] = '\0';  // Null-terminate for safety

    // Create StringRef (this increments ref_count to 1)
    StringRef ref(data, static_cast<uint32_t>(str.length()), pool_id);

    if (enable_deduplication_) {
      typename decltype(dedup_cache_)::accessor acc;
      dedup_cache_.insert(acc, str);
      acc->second = ref;  // This increments ref to 2 (cache holds one ref)
    }

    return ref;
  }

  /**
   * Store a string view in this pool.
   */
  StringRef store_string(std::string_view str, uint32_t pool_id) {
    return store_string(std::string(str), pool_id);
  }

  /**
   * Mark a string for deletion (called when node field is updated).
   * The string will be deallocated when the last StringRef is destroyed.
   *
   * @param data Pointer to string data (NOT to header)
   */
  void mark_for_deletion(const char* data) {
    if (!data) return;

    auto* header = reinterpret_cast<StringRef::StringHeader*>(
        const_cast<char*>(data - StringRef::HEADER_SIZE));

    header->mark_for_deletion();

    // Remove from dedup cache immediately to prevent new references
    if (enable_deduplication_) {
      std::string str(data, header->length);
      dedup_cache_.erase(str);
    }
  }

  /**
   * Release a string reference (called by StringRef::release()).
   * Decrements ref count and deallocates if this was the last reference
   * AND the string is marked for deletion.
   *
   * Thread-safe: Multiple threads can call this concurrently.
   *
   * @param data Pointer to string data (NOT to header)
   */
  void release_string(const char* data) {
    if (!data) return;

    // Get header
    auto* header = reinterpret_cast<StringRef::StringHeader*>(
        const_cast<char*>(data - StringRef::HEADER_SIZE));

    // Decrement ref count atomically
    int32_t old_count =
        header->ref_count.fetch_sub(1, std::memory_order_acq_rel);

    if (old_count == 1) {  // We were the last reference
      // Only deallocate if marked for deletion
      if (header->is_marked_for_deletion()) {
        // CORRECT: Don't remove from dedup_cache_ here!
        // It was already removed in mark_for_deletion(), or
        // a new string with the same content may now be in cache.

        // CRITICAL: Lock arena deallocation (FreeListArena is NOT thread-safe)
        std::lock_guard<std::mutex> lock(arena_mutex_);

        // Deallocate entire block (header + data + null terminator)
        size_t alloc_size = StringRef::HEADER_SIZE + header->length + 1;
        arena_->deallocate(header);
      }
    }
  }

  /**
   * Enable or disable string deduplication.
   * When enabled, identical strings share the same memory.
   */
  void enable_deduplication(bool enable = true) {
    enable_deduplication_ = enable;
    if (!enable) {
      dedup_cache_.clear();
    }
  }

  // ========================================================================
  // STATISTICS
  // ========================================================================

  size_t get_max_size() const { return max_size_; }

  size_t get_total_allocated() const { return arena_->get_total_allocated(); }

  size_t get_used_bytes() const {
    if (auto* free_list = dynamic_cast<FreeListArena*>(arena_.get())) {
      return free_list->get_used_bytes();
    }
    return 0;
  }

  size_t get_string_count() const { return dedup_cache_.size(); }

  /**
   * Get total reference count across all strings (for debugging).
   */
  size_t get_total_references() const {
    size_t total = 0;
    typename decltype(dedup_cache_)::const_accessor acc;
    for (auto it = dedup_cache_.begin(); it != dedup_cache_.end(); ++it) {
      if (dedup_cache_.find(acc, it->first)) {
        total += acc->second.get_ref_count();
      }
    }
    return total;
  }

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
  StringArena() {
    pools_.reserve(4);
    pools_.emplace_back(std::make_unique<StringPool>(16));
    pools_.emplace_back(std::make_unique<StringPool>(32));
    pools_.emplace_back(std::make_unique<StringPool>(64));
    pools_.emplace_back(std::make_unique<StringPool>(SIZE_MAX));
  }

  /**
   * Store a string in a specific pool by ID.
   *
   * @param str String to store
   * @param pool_id Pool identifier (0-3), default is pool 3 (unlimited size)
   * @return StringRef with ref_count = 1
   */
  StringRef store_string(const std::string& str, uint32_t pool_id = 3) {
    if (pool_id >= pools_.size()) {
      return StringRef{};  // Invalid pool ID
    }
    return pools_[pool_id]->store_string(str, pool_id);
  }

  /**
   * Store a string, automatically choosing the best pool.
   * Picks the smallest pool that can fit the string.
   */
  StringRef store_string_auto(const std::string& str) {
    size_t len = str.length();
    if (len <= 16) return pools_[0]->store_string(str, 0);
    if (len <= 32) return pools_[1]->store_string(str, 1);
    if (len <= 64) return pools_[2]->store_string(str, 2);
    return pools_[3]->store_string(str, 3);
  }

  /**
   * Mark a string for deletion.
   * The string will be deallocated when the last StringRef is destroyed.
   *
   * @param ref StringRef to mark for deletion
   */
  void mark_for_deletion(const StringRef& ref) {
    if (!ref.is_null()) {
      pools_[ref.pool_id()]->mark_for_deletion(ref.data());
    }
  }

  /**
   * Get string content from reference (zero-copy view).
   */
  std::string_view get_string_view(const StringRef& ref) const {
    return ref.view();
  }

  /**
   * Enable or disable deduplication for all pools.
   */
  void enable_deduplication(bool enable = true) {
    for (auto& pool : pools_) {
      pool->enable_deduplication(enable);
    }
  }

  /**
   * Get pool by ID.
   */
  StringPool* get_pool(uint32_t pool_id) const {
    if (pool_id < pools_.size()) {
      return pools_[pool_id].get();
    }
    return nullptr;
  }

  /**
   * Reset all pools.
   */
  void reset() {
    for (auto& pool : pools_) {
      pool->reset();
    }
  }

  /**
   * Clear all memory.
   */
  void clear() {
    for (auto& pool : pools_) {
      pool->clear();
    }
  }

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

  static StringPool* get_pool(uint32_t pool_id) {
    typename decltype(pool_map_)::const_accessor acc;
    if (instance().pool_map_.find(acc, pool_id)) {
      return acc->second;
    }
    return nullptr;
  }

  /**
   * Release a string reference.
   * Called by StringRef::release() to deallocate the string.
   */
  static void release_string(uint32_t pool_id, const char* data) {
    if (auto* pool = get_pool(pool_id)) {
      pool->release_string(data);
    }
  }
};

// ============================================================================
// StringRef::release() implementation
// ============================================================================

inline void StringRef::release() {
  if (data_) {
    auto* header = get_header();
    if (header) {
      // Decrement ref count atomically
      int32_t old_count =
          header->ref_count.fetch_sub(1, std::memory_order_acq_rel);

      if (old_count == 1 && header->is_marked_for_deletion()) {
        // Last reference and marked for deletion - deallocate
        StringArenaRegistry::release_string(pool_id_, data_);
      }
    }

    // Clear this reference
    data_ = nullptr;
    length_ = 0;
    pool_id_ = 0;
  }
}

}  // namespace tundradb

#endif  // STRING_ARENA_HPP
