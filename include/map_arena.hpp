#ifndef MAP_ARENA_HPP
#define MAP_ARENA_HPP

#include <arrow/api.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>

#include "array_ref.hpp"
#include "free_list_arena.hpp"
#include "map_ref.hpp"
#include "string_ref.hpp"
#include "types.hpp"
#include "value_type.hpp"

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
  arrow::Result<MapRef> allocate(uint32_t capacity = DEFAULT_CAPACITY) {
    if (capacity == 0) return MapRef{};

    const size_t data_bytes = sizeof(MapEntry) * capacity;
    const size_t alloc_size = MapRef::HEADER_SIZE + data_bytes;

    std::lock_guard<std::mutex> lock(arena_mutex_);
    void* raw = arena_->allocate(alloc_size);
    if (!raw) {
      return arrow::Status::OutOfMemory(
          "MapArena::allocate: arena allocation failed (requested ", alloc_size,
          " bytes)");
    }

    init_header(raw, capacity);

    char* data = static_cast<char*>(raw) + MapRef::HEADER_SIZE;
    zero_init_entries(data, capacity);

    active_allocs_.fetch_add(1, std::memory_order_relaxed);
    return MapRef{data};
  }

  /**
   * Create a COW copy of an existing map.
   * The new map has the same entries but independent storage.
   *
   * @param src            Source MapRef to copy
   * @param extra_capacity Additional entry slots beyond the original capacity
   */
  arrow::Result<MapRef> copy(const MapRef& src, uint32_t extra_capacity = 0) {
    if (src.is_null()) {
      return arrow::Status::Invalid("MapArena::copy: source MapRef is null");
    }

    const auto* header = get_header_const(src);
    if (!header) {
      return arrow::Status::Invalid(
          "MapArena::copy: invalid source MapRef (header is null)");
    }

    const uint32_t src_count = header->count;
    const uint32_t new_capacity = header->capacity + extra_capacity;

    ARROW_ASSIGN_OR_RAISE(MapRef new_ref, allocate(new_capacity));

    auto* new_header = get_header(new_ref);
    new_header->count = src_count;

    // Copy-construct entries (handles ref-counted StringRef keys/values)
    for (uint32_t i = 0; i < src_count; ++i) {
      const auto* src_entry = src.entry_ptr(i);
      auto* dst_entry = new_ref.mutable_entry_ptr(i);
      copy_init_entry(dst_entry, src_entry);
    }

    return new_ref;
  }

  void mark_for_deletion(const MapRef& ref) {
    if (ref.is_null()) return;
    if (auto* h = get_header_mut(ref)) {
      h->mark_for_deletion();
    }
  }

  /**
   * Deallocate a map's memory back to the FreeListArena.
   * Called by MapRef::release() when ref_count reaches 0 and
   * the map is marked for deletion.
   */
  void release_map(char* data) {
    if (!data) return;

    auto* header =
        reinterpret_cast<MapRef::MapHeader*>(data - MapRef::HEADER_SIZE);
    if (!header->arena) return;
    header->arena = nullptr;

    destruct_entries(data, header->count);

    active_allocs_.fetch_sub(1, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(arena_mutex_);
    arena_->deallocate(header);
  }

  // ========================================================================
  // Entry-level operations
  // ========================================================================

  /**
   * Find an entry by key name (linear scan).
   * Returns the index, or -1 if not found.
   */
  static int32_t find_entry(const MapRef& ref, const std::string& key) {
    if (ref.is_null()) return -1;
    const uint32_t n = ref.count();
    for (uint32_t i = 0; i < n; ++i) {
      const auto* entry = ref.entry_ptr(i);
      if (entry->key.view() == key) {
        return static_cast<int32_t>(i);
      }
    }
    return -1;
  }

  /**
   * Set an entry value. If the key exists, overwrites it in place.
   * If the key doesn't exist and there's capacity, appends.
   * If full, returns OutOfMemory (caller should COW-copy with extra capacity).
   *
   * The caller is responsible for managing the StringRef key lifetime
   * (store it in string_arena before calling this).
   */
  static arrow::Status set_entry(MapRef& ref, const StringRef& key,
                                 ValueType vtype, const void* value_ptr) {
    if (ref.is_null()) {
      return arrow::Status::Invalid("MapArena::set_entry: MapRef is null");
    }

    auto* header = get_header(ref);
    if (!header) {
      return arrow::Status::Invalid("MapArena::set_entry: invalid header");
    }

    // Try to find existing entry
    const std::string_view key_view = key.view();
    for (uint32_t i = 0; i < header->count; ++i) {
      auto* entry = ref.mutable_entry_ptr(i);
      if (entry->key.view() == key_view) {
        destruct_entry_value(entry);
        entry->value_type = static_cast<uint8_t>(vtype);
        copy_value_into_entry(entry, vtype, value_ptr);
        return arrow::Status::OK();
      }
    }

    // Not found — append
    if (header->count >= header->capacity) {
      return arrow::Status::CapacityError(
          "MapArena::set_entry: map is full (count=", header->count,
          ", capacity=", header->capacity, ")");
    }

    auto* entry = ref.mutable_entry_ptr(header->count);
    // Destruct the default-constructed entry before overwriting
    entry->key.~StringRef();
    new (&entry->key) StringRef(key);
    entry->value_type = static_cast<uint8_t>(vtype);
    copy_value_into_entry(entry, vtype, value_ptr);
    header->count++;
    return arrow::Status::OK();
  }

  /**
   * Remove an entry by key (swap-with-last, O(1) removal).
   * Returns true if found and removed, false if not found.
   */
  static bool remove_entry(MapRef& ref, const std::string& key) {
    if (ref.is_null()) return false;
    auto* header = get_header(ref);
    if (!header || header->count == 0) return false;

    for (uint32_t i = 0; i < header->count; ++i) {
      auto* entry = ref.mutable_entry_ptr(i);
      if (entry->key.view() == key) {
        destruct_entry(entry);
        if (i < header->count - 1) {
          auto* last = ref.mutable_entry_ptr(header->count - 1);
          move_entry(entry, last);
        }
        // Zero-init the now-vacant last slot
        auto* vacant = ref.mutable_entry_ptr(header->count - 1);
        new (vacant) MapEntry();
        header->count--;
        return true;
      }
    }
    return false;
  }

  // ========================================================================
  // Statistics
  // ========================================================================

  int64_t get_active_allocs() const {
    return active_allocs_.load(std::memory_order_relaxed);
  }

  size_t get_total_allocated() const { return arena_->get_total_allocated(); }
  size_t get_used_bytes() const { return arena_->get_used_bytes(); }
  size_t get_freed_bytes() const { return arena_->get_freed_bytes(); }

  void reset() {
    std::lock_guard<std::mutex> lock(arena_mutex_);
    arena_->reset();
  }

  void clear() {
    std::lock_guard<std::mutex> lock(arena_mutex_);
    arena_->clear();
  }

 private:
  MapRef::MapHeader* init_header(void* raw, uint32_t capacity) {
    auto* header = static_cast<MapRef::MapHeader*>(raw);
    header->ref_count.store(0, std::memory_order_relaxed);
    header->flags = 0;
    header->count = 0;
    header->capacity = capacity;
    header->arena = this;
    return header;
  }

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

  static void zero_init_entries(char* data, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
      new (data + i * sizeof(MapEntry)) MapEntry();
    }
  }

  /** Copy-construct a single entry (properly handles ref-counted key). */
  static void copy_init_entry(MapEntry* dst, const MapEntry* src) {
    dst->key.~StringRef();
    new (&dst->key) StringRef(src->key);
    dst->value_type = src->value_type;
    std::memset(dst->pad, 0, sizeof(dst->pad));
    copy_value_into_entry(dst, static_cast<ValueType>(src->value_type),
                          src->value);
  }

  /** Copy value bytes into entry, using copy-constructor for ref-counted types.
   */
  static void copy_value_into_entry(MapEntry* entry, ValueType vtype,
                                    const void* src) {
    if (is_string_type(vtype)) {
      auto* dst = reinterpret_cast<StringRef*>(entry->value);
      dst->~StringRef();
      new (dst) StringRef(*reinterpret_cast<const StringRef*>(src));
    } else if (is_array_type(vtype)) {
      auto* dst = reinterpret_cast<ArrayRef*>(entry->value);
      dst->~ArrayRef();
      new (dst) ArrayRef(*reinterpret_cast<const ArrayRef*>(src));
    } else if (is_map_type(vtype)) {
      auto* dst = reinterpret_cast<MapRef*>(entry->value);
      dst->~MapRef();
      new (dst) MapRef(*reinterpret_cast<const MapRef*>(src));
    } else {
      std::memcpy(entry->value, src, MapEntry::VALUE_SIZE);
    }
  }

  /**
   * Destruct the value in an entry.
   * Marks ref-counted values for deletion before calling destructors so that
   * release() can actually free the underlying arena memory when ref_count
   * reaches 0.
   */
  static void destruct_entry_value(MapEntry* entry) {
    auto vtype = static_cast<ValueType>(entry->value_type);
    if (is_string_type(vtype)) {
      auto* sr = reinterpret_cast<StringRef*>(entry->value);
      if (!sr->is_null()) {
        auto* hdr = reinterpret_cast<StringRef::StringHeader*>(
            const_cast<char*>(sr->data() - StringRef::HEADER_SIZE));
        hdr->mark_for_deletion();
      }
      sr->~StringRef();
      new (sr) StringRef();
    } else if (is_array_type(vtype)) {
      auto* ar = reinterpret_cast<ArrayRef*>(entry->value);
      if (!ar->is_null()) {
        auto* hdr = reinterpret_cast<ArrayRef::ArrayHeader*>(
            ar->data() - ArrayRef::HEADER_SIZE);
        hdr->mark_for_deletion();
      }
      ar->~ArrayRef();
      new (ar) ArrayRef();
    } else if (is_map_type(vtype)) {
      auto* mr = reinterpret_cast<MapRef*>(entry->value);
      if (!mr->is_null()) {
        auto* hdr = reinterpret_cast<MapRef::MapHeader*>(mr->data() -
                                                         MapRef::HEADER_SIZE);
        hdr->mark_for_deletion();
      }
      mr->~MapRef();
      new (mr) MapRef();
    }
  }

  /** Destruct an entire entry (key + value). */
  static void destruct_entry(MapEntry* entry) {
    destruct_entry_value(entry);
    if (!entry->key.is_null()) {
      auto* hdr = reinterpret_cast<StringRef::StringHeader*>(
          const_cast<char*>(entry->key.data() - StringRef::HEADER_SIZE));
      hdr->mark_for_deletion();
    }
    entry->key.~StringRef();
    new (&entry->key) StringRef();
    entry->value_type = static_cast<uint8_t>(ValueType::NA);
    std::memset(entry->value, 0, MapEntry::VALUE_SIZE);
  }

  /** Move entry src into dst (dst assumed already destructed). */
  static void move_entry(MapEntry* dst, MapEntry* src) {
    new (&dst->key) StringRef(std::move(src->key));
    dst->value_type = src->value_type;
    std::memcpy(dst->value, src->value, MapEntry::VALUE_SIZE);
    // Clear src without destructing moved-from refs
    src->value_type = static_cast<uint8_t>(ValueType::NA);
    std::memset(src->value, 0, MapEntry::VALUE_SIZE);
  }

  /** Destruct all entries in a map block (marks keys/values for deletion). */
  static void destruct_entries(char* data, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
      auto* entry = reinterpret_cast<MapEntry*>(data + i * sizeof(MapEntry));
      destruct_entry_value(entry);
      if (!entry->key.is_null()) {
        auto* hdr = reinterpret_cast<StringRef::StringHeader*>(
            const_cast<char*>(entry->key.data() - StringRef::HEADER_SIZE));
        hdr->mark_for_deletion();
      }
      entry->key.~StringRef();
    }
  }

  std::unique_ptr<FreeListArena> arena_;
  mutable std::mutex arena_mutex_;
  std::atomic<int64_t> active_allocs_{0};
};

// ============================================================================
// MapRef::release() implementation (after MapArena is fully defined)
// ============================================================================

inline void MapRef::release() {
  if (!data_) return;
  if (auto* h = get_header()) {
    assert(h->ref_count.load(std::memory_order_relaxed) > 0 &&
           "MapRef::release() called with ref_count already 0");

    const int32_t old_count =
        h->ref_count.fetch_sub(1, std::memory_order_acq_rel);
    if (old_count == 1 && h->is_marked_for_deletion() && h->arena) {
      h->arena->release_map(data_);
    }
  }
  data_ = nullptr;
}

inline Value MapRef::get_value(const std::string& key) const {
  int32_t idx = MapArena::find_entry(*this, key);
  if (idx < 0) return Value{};
  const auto* entry = entry_ptr(static_cast<uint32_t>(idx));
  return Value::read_value_from_memory(
      entry->value, static_cast<ValueType>(entry->value_type));
}

inline bool MapRef::contains(const std::string& key) const {
  return MapArena::find_entry(*this, key) >= 0;
}

}  // namespace tundradb

#endif  // MAP_ARENA_HPP
