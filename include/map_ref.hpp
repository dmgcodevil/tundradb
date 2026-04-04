#ifndef MAP_REF_HPP
#define MAP_REF_HPP

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "common/constants.hpp"
#include "string_ref.hpp"
#include "common/value_type.hpp"

namespace tundradb {

class MapArena;  // forward declaration
class Value;     // forward declaration (defined in types.hpp)

/**
 * A single key-value entry inside a MapRef's arena block.
 *
 * Layout (40 bytes):
 *   StringRef  key         16 bytes  (arena-backed key string)
 *   uint8_t    value_type   1 byte   (ValueType tag)
 *   uint8_t    pad[7]       7 bytes  (alignment)
 *   char       value[16]   16 bytes  (reinterpret_cast to actual type)
 *
 * The value slot fits any supported type: int32, int64, float, double, bool,
 * StringRef (16B), or ArrayRef (16B).
 */
struct MapEntry {
  static constexpr size_t VALUE_SIZE = 16;

  StringRef key;
  uint8_t value_type;
  uint8_t pad[7];
  char value[VALUE_SIZE];

  MapEntry() : key(), value_type(static_cast<uint8_t>(ValueType::NA)), pad{} {
    std::memset(value, 0, VALUE_SIZE);
  }
};

static_assert(sizeof(MapEntry) == 40, "MapEntry must be 40 bytes");

/**
 * Lightweight handle stored in the node's fixed-size slot (16 bytes).
 *
 * Memory layout in MapArena (FreeListArena):
 * ┌────────────┬────────┬──────────┬──────────┬──────────┬───────────────┐
 * │ ref_count  │ flags  │  count   │ capacity │  arena*  │ entry data    │
 * │   (4B)     │ (4B)   │   (4B)   │  (4B)    │  (8B)    │ (40B * cap)   │
 * └────────────┴────────┴──────────┴──────────┴──────────┴───────────────┘
 *       ↑                                                 ↑
 *    MapHeader (24 bytes)                            data_ points here
 *
 * MapRef stored in the node slot:
 * ┌──────────┬─────────┐
 * │  data_   │ padding │
 * │  (8B)    │  (8B)   │
 * └──────────┴─────────┘
 *          Total: 16 bytes
 */
class MapRef {
 public:
  struct MapHeader {
    std::atomic<int32_t> ref_count;  // 4 bytes
    uint32_t flags;                  // 4 bytes - bit 0: marked_for_deletion
    uint32_t count;                  // 4 bytes - current entry count
    uint32_t capacity;               // 4 bytes - allocated entry slots
    MapArena* arena;                 // 8 bytes - owning arena (for release)

    [[nodiscard]] bool is_marked_for_deletion() const {
      return (flags & arena_flags::kMarkedForDeletion) != 0;
    }
    void mark_for_deletion() { flags |= arena_flags::kMarkedForDeletion; }
  };

  static constexpr size_t HEADER_SIZE = sizeof(MapHeader);  // 24 bytes

  // ========================================================================
  // CONSTRUCTORS AND DESTRUCTOR
  // ========================================================================

  MapRef() : data_(nullptr), pad_(0) {}

  explicit MapRef(char* data) : data_(data), pad_(0) {
    if (data_) {
      if (auto* h = get_header()) {
        h->ref_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  MapRef(const MapRef& other) : data_(other.data_), pad_(0) {
    if (data_) {
      if (auto* h = get_header()) {
        h->ref_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  MapRef(MapRef&& other) noexcept : data_(other.data_), pad_(0) {
    other.data_ = nullptr;
  }

  MapRef& operator=(const MapRef& other) {
    if (this != &other) {
      release();
      data_ = other.data_;
      if (data_) {
        if (auto* h = get_header()) {
          h->ref_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    return *this;
  }

  MapRef& operator=(MapRef&& other) noexcept {
    if (this != &other) {
      release();
      data_ = other.data_;
      other.data_ = nullptr;
    }
    return *this;
  }

  ~MapRef() { release(); }

  // ========================================================================
  // PUBLIC INTERFACE
  // ========================================================================

  [[nodiscard]] char* data() const { return data_; }

  [[nodiscard]] uint32_t count() const {
    const auto* h = get_header();
    return h ? h->count : 0;
  }

  [[nodiscard]] uint32_t capacity() const {
    const auto* h = get_header();
    return h ? h->capacity : 0;
  }

  [[nodiscard]] bool is_null() const { return data_ == nullptr; }
  [[nodiscard]] bool empty() const { return count() == 0; }

  [[nodiscard]] const MapEntry* entry_ptr(uint32_t i) const {
    assert(i < count());
    return reinterpret_cast<const MapEntry*>(data_ + static_cast<size_t>(i) *
                                                         sizeof(MapEntry));
  }

  [[nodiscard]] MapEntry* mutable_entry_ptr(uint32_t i) const {
    assert(i < capacity());
    return reinterpret_cast<MapEntry*>(data_ + static_cast<size_t>(i) *
                                                   sizeof(MapEntry));
  }

  [[nodiscard]] bool has_capacity() const { return count() < capacity(); }

  [[nodiscard]] int32_t get_ref_count() const {
    const auto* h = get_header();
    return h ? h->ref_count.load(std::memory_order_relaxed) : 0;
  }

  [[nodiscard]] bool is_marked_for_deletion() const {
    const auto* h = get_header();
    return h && h->is_marked_for_deletion();
  }

  // ========================================================================
  // VALUE ACCESS
  // ========================================================================

  /**
   * Get a value by key. Returns Value{} (NA) if not found.
   * Implementation in map_arena.hpp (after Value is fully defined).
   */
  [[nodiscard]] Value get_value(const std::string& key) const;

  /**
   * Check if a key exists.
   */
  [[nodiscard]] bool contains(const std::string& key) const;

  // ========================================================================
  // OPERATORS
  // ========================================================================

  bool operator==(const MapRef& other) const {
    if (data_ == other.data_) return true;
    if (is_null() && other.is_null()) return true;
    if (is_null() || other.is_null()) return false;
    if (count() != other.count()) return false;
    return std::memcmp(data_, other.data_, count() * sizeof(MapEntry)) == 0;
  }

  bool operator!=(const MapRef& other) const { return !(*this == other); }

 private:
  // Implementation in map_arena.hpp to avoid circular dependency.
  void release();

  MapHeader* get_header() const {
    if (!data_) return nullptr;
    return reinterpret_cast<MapHeader*>(data_ - HEADER_SIZE);
  }

  char* data_;    // 8 bytes - pointer to entry data in MapArena
  uint64_t pad_;  // 8 bytes - padding to reach 16 bytes total

  friend class MapArena;
};

static_assert(sizeof(MapRef) == 16,
              "MapRef must be 16 bytes (same as StringRef/ArrayRef)");

static_assert(!std::is_trivially_copyable_v<MapRef>,
              "MapRef must not be trivially copyable");

}  // namespace tundradb

#endif  // MAP_REF_HPP
