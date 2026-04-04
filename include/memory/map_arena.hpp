#ifndef MAP_ARENA_HPP
#define MAP_ARENA_HPP

#include <arrow/api.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>

#include "common/types.hpp"
#include "common/value_type.hpp"
#include "memory/array_ref.hpp"
#include "memory/free_list_arena.hpp"
#include "memory/map_ref.hpp"
#include "memory/string_ref.hpp"

namespace tundradb {

/**
 * Arena for managing variable-length map (key-value) data.
 *
 * Memory layout per map allocation:
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │ MapHeader (24B)              │  entry data (40B x capacity)          │
 * │ [ref_count|flags|count|      │  [MapEntry0][MapEntry1][...]          │
 * │  capacity|arena*]            │                                       │
 * └──────────────────────────────────────────────────────────────────────┘
 *                                ↑
 *                           MapRef::data_ points here
 *
 * Thread safety: FreeListArena is NOT thread-safe, so all
 * allocations/deallocations are protected by arena_mutex_.
 */
class MapArena {
 public:
  static constexpr uint32_t DEFAULT_CAPACITY = 8;

  explicit MapArena(size_t initial_size = 2 * 1024 * 1024)
      : arena_(std::make_unique<FreeListArena>(initial_size, 16)) {}

  MapArena(const MapArena&) = delete;
  MapArena& operator=(const MapArena&) = delete;

  /**
   * Allocate a new empty map with the given capacity.
   * Returns a MapRef with ref_count = 1 and count = 0.
   */
  arrow::Result<MapRef> allocate(uint32_t capacity = DEFAULT_CAPACITY);

  /**
   * Create a COW copy of an existing map.
   * The new map has the same entries but independent storage.
   *
   * @param src            Source MapRef to copy
   * @param extra_capacity Additional entry slots beyond the original capacity
   */
  arrow::Result<MapRef> copy(const MapRef& src, uint32_t extra_capacity = 0);

  void mark_for_deletion(const MapRef& ref);

  /**
   * Deallocate a map's memory back to the FreeListArena.
   * Called by MapRef::release() when ref_count reaches 0 and
   * the map is marked for deletion.
   */
  void release_map(char* data);

  // ========================================================================
  // Entry-level operations
  // ========================================================================

  /**
   * Find an entry by key name (linear scan).
   * Returns the index, or -1 if not found.
   */
  static int32_t find_entry(const MapRef& ref, const std::string& key);

  /**
   * Set an entry value. If the key exists, overwrites it in place.
   * If the key doesn't exist and there's capacity, appends.
   * If full, returns OutOfMemory (caller should COW-copy with extra capacity).
   *
   * The caller is responsible for managing the StringRef key lifetime
   * (store it in string_arena before calling this).
   */
  static arrow::Status set_entry(MapRef& ref, const StringRef& key,
                                 ValueType vtype, const void* value_ptr);

  /**
   * Remove an entry by key (swap-with-last, O(1) removal).
   * Returns true if found and removed, false if not found.
   */
  static bool remove_entry(MapRef& ref, const std::string& key);

  // ========================================================================
  // Statistics
  // ========================================================================

  int64_t get_active_allocs() const {
    return active_allocs_.load(std::memory_order_relaxed);
  }

  size_t get_total_allocated() const { return arena_->get_total_allocated(); }
  size_t get_used_bytes() const { return arena_->get_used_bytes(); }
  size_t get_freed_bytes() const { return arena_->get_freed_bytes(); }

  void reset();
  void clear();

 private:
  MapRef::MapHeader* init_header(void* raw, uint32_t capacity);

  static MapRef::MapHeader* get_header(const MapRef& ref) {
    if (ref.is_null()) return nullptr;
    return reinterpret_cast<MapRef::MapHeader*>(ref.data() -
                                                MapRef::HEADER_SIZE);
  }

  static const MapRef::MapHeader* get_header_const(const MapRef& ref) {
    return get_header(ref);
  }

  static MapRef::MapHeader* get_header_mut(const MapRef& ref) {
    return get_header(ref);
  }

  static void zero_init_entries(char* data, uint32_t count);
  static void copy_init_entry(MapEntry* dst, const MapEntry* src);
  static void copy_value_into_entry(MapEntry* entry, ValueType vtype,
                                    const void* src);
  static void destruct_entry_value(MapEntry* entry);
  static void destruct_entry(MapEntry* entry);
  static void move_entry(MapEntry* dst, MapEntry* src);
  static void destruct_entries(char* data, uint32_t count);

  std::unique_ptr<FreeListArena> arena_;
  mutable std::mutex arena_mutex_;
  std::atomic<int64_t> active_allocs_{0};
};

}  // namespace tundradb

#endif  // MAP_ARENA_HPP
