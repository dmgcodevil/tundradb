#ifndef ARRAY_REF_HPP
#define ARRAY_REF_HPP

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "common/constants.hpp"
#include "common/value_type.hpp"

namespace tundradb {

class ArrayArena;  // forward declaration

/**
 * Lightweight handle stored in the node's fixed-size slot (16 bytes).
 * Same footprint as StringRef.
 *
 * Memory layout in ArrayArena (FreeListArena):
 * ┌────────────┬────────┬──────────┬──────────┬──────────┬───────────────┐
 * │ ref_count  │ flags  │  length  │ capacity │  arena*  │ element data  │
 * │   (4B)     │ (4B)   │   (4B)   │  (4B)    │  (8B)    │ (elem*cap)    │
 * └────────────┴────────┴──────────┴──────────┴──────────┴───────────────┘
 *       ↑                                                 ↑
 *    ArrayHeader (24 bytes)                          data_ points here
 *
 * The owning ArrayArena pointer is stored in the header so that
 * ArrayRef::release() can deallocate directly without a global registry.
 *
 * ArrayRef stored in the node slot:
 * ┌──────────┬────────────┬─────────┐
 * │  data_   │ elem_type_ │ padding │
 * │  (8B)    │   (4B)     │  (4B)   │
 * └──────────┴────────────┴─────────┘
 *            Total: 16 bytes
 *
 * Supported operations (v1):
 *   SET field = [values]        - replace entire array
 *   APPEND(field, value)        - add element to end
 * Random-index delete is NOT supported in v1.
 */
class ArrayRef {
 public:
  /**
   * Header stored in ArrayArena memory BEFORE the element data.
   * All mutable shared state lives here - ArrayRef delegates to it.
   * The arena pointer allows release() to deallocate without a registry.
   */
  struct ArrayHeader {
    std::atomic<int32_t> ref_count;  // 4 bytes
    uint32_t flags;                  // 4 bytes - bit 0: marked_for_deletion
    uint32_t length;                 // 4 bytes - current element count
    uint32_t capacity;               // 4 bytes - allocated element slots
    ArrayArena* arena;               // 8 bytes - owning arena (for dealloc)

    [[nodiscard]] bool is_marked_for_deletion() const {
      return (flags & arena_flags::kMarkedForDeletion) != 0;
    }
    void mark_for_deletion() { flags |= arena_flags::kMarkedForDeletion; }
  };

  static constexpr size_t HEADER_SIZE = sizeof(ArrayHeader);  // 24 bytes

  // ========================================================================
  // CONSTRUCTORS AND DESTRUCTOR
  // ========================================================================

  /** Default constructor - creates a null/empty reference. */
  ArrayRef() : data_(nullptr), elem_type_(ValueType::NA) {}

  /**
   * Internal constructor used by ArrayArena.
   * Automatically increments the reference count.
   *
   * @param data      Pointer to first element in arena (NOT to header)
   * @param elem_type Element ValueType (e.g. INT64, DOUBLE, STRING)
   */
  ArrayRef(char* data, ValueType elem_type)
      : data_(data), elem_type_(elem_type) {
    if (data_) {
      if (auto* h = get_header()) {
        h->ref_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  /** Copy constructor - increments reference count. */
  ArrayRef(const ArrayRef& other)
      : data_(other.data_), elem_type_(other.elem_type_) {
    if (data_) {
      if (auto* h = get_header()) {
        h->ref_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  /** Move constructor - transfers ownership without changing ref count. */
  ArrayRef(ArrayRef&& other) noexcept
      : data_(other.data_), elem_type_(other.elem_type_) {
    other.data_ = nullptr;
    other.elem_type_ = ValueType::NA;
  }

  /** Copy assignment - properly handles reference counting. */
  ArrayRef& operator=(const ArrayRef& other) {
    if (this != &other) {
      release();
      data_ = other.data_;
      elem_type_ = other.elem_type_;
      if (data_) {
        if (auto* h = get_header()) {
          h->ref_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    return *this;
  }

  /** Move assignment - transfers ownership. */
  ArrayRef& operator=(ArrayRef&& other) noexcept {
    if (this != &other) {
      release();
      data_ = other.data_;
      elem_type_ = other.elem_type_;
      other.data_ = nullptr;
      other.elem_type_ = ValueType::NA;
    }
    return *this;
  }

  ~ArrayRef() { release(); }

  // ========================================================================
  // PUBLIC INTERFACE - delegates to header for mutable state
  // ========================================================================

  /** Pointer to the first element in arena memory. */
  [[nodiscard]] char* data() const { return data_; }

  /** Element type (e.g. INT64, DOUBLE, STRING). */
  [[nodiscard]] ValueType elem_type() const { return elem_type_; }

  /** Current number of elements (reads from shared header). */
  [[nodiscard]] uint32_t length() const {
    const auto* h = get_header();
    return h ? h->length : 0;
  }

  /** Allocated element slots (reads from shared header). */
  [[nodiscard]] uint32_t capacity() const {
    const auto* h = get_header();
    return h ? h->capacity : 0;
  }

  /** True if this reference points to no data. */
  [[nodiscard]] bool is_null() const { return data_ == nullptr; }

  /** True if the array has no elements. */
  [[nodiscard]] bool empty() const { return length() == 0; }

  /** Size of one element in bytes. */
  [[nodiscard]] size_t elem_size() const { return get_type_size(elem_type_); }

  /** Pointer to the element at index i. */
  [[nodiscard]] const char* element_ptr(uint32_t i) const {
    assert(i < length());
    return data_ + static_cast<size_t>(i) * elem_size();
  }

  /** Mutable pointer to the element at index i. */
  [[nodiscard]] char* mutable_element_ptr(uint32_t i) const {
    assert(i < capacity());
    return data_ + static_cast<size_t>(i) * elem_size();
  }

  /** True if there's room for at least one more element. */
  [[nodiscard]] bool has_capacity() const { return length() < capacity(); }

  /** Get the current ref count (for debugging). */
  [[nodiscard]] int32_t get_ref_count() const {
    const auto* h = get_header();
    return h ? h->ref_count.load(std::memory_order_relaxed) : 0;
  }

  /** Check if this array is marked for deferred deletion. */
  [[nodiscard]] bool is_marked_for_deletion() const {
    const auto* h = get_header();
    return h && h->is_marked_for_deletion();
  }

  // ========================================================================
  // OPERATORS
  // ========================================================================

  bool operator==(const ArrayRef& other) const {
    if (data_ == other.data_) return true;
    const uint32_t len = length();
    if (len != other.length() || elem_type_ != other.elem_type_) return false;
    if (is_null() && other.is_null()) return true;
    if (is_null() || other.is_null()) return false;
    return std::memcmp(data_, other.data_, len * elem_size()) == 0;
  }

  bool operator!=(const ArrayRef& other) const { return !(*this == other); }

 private:
  /**
   * Release this reference.
   * Decrements ref count; if this was the last reference AND the array
   * is marked for deletion, deallocates via the arena stored in the header.
   * Implementation in array_arena.hpp to avoid circular dependency.
   */
  void release();

  ArrayHeader* get_header() const {
    if (!data_) return nullptr;
    return reinterpret_cast<ArrayHeader*>(data_ - HEADER_SIZE);
  }

  char* data_;           // 8 bytes - pointer to element data in ArrayArena
  ValueType elem_type_;  // 4 bytes - element type
  // 4 bytes implicit padding -> total 16 bytes (same as StringRef)

  friend class ArrayArena;
};

static_assert(sizeof(ArrayRef) == 16,
              "ArrayRef must be 16 bytes (same as StringRef)");

// ArrayRef has custom copy/move/destructor for ref counting.
// It MUST NOT be trivially copyable - using memcpy on it skips ref-count
// updates and causes use-after-free. Always use copy/move constructors.
static_assert(!std::is_trivially_copyable_v<ArrayRef>,
              "ArrayRef must not be trivially copyable");

}  // namespace tundradb

#endif  // ARRAY_REF_HPP
