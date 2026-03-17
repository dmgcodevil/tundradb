#ifndef ARRAY_ARENA_HPP
#define ARRAY_ARENA_HPP

#include <arrow/api.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>

#include "array_ref.hpp"
#include "free_list_arena.hpp"
#include "string_ref.hpp"
#include "value_type.hpp"

namespace tundradb {

/**
 * Arena for managing variable-length array data.
 *
 * Memory layout per array allocation:
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │ ArrayHeader (24B)          │  element data (elem_size x capacity)    │
 * │ [ref_count|flags|length|   │  [elem0][elem1][...][padding]           │
 * │  capacity|arena*]          │                                         │
 * └──────────────────────────────────────────────────────────────────────┘
 *                              ↑
 *                          ArrayRef::data_ points here
 *
 * The header stores a pointer to the owning ArrayArena so that
 * ArrayRef::release() can deallocate directly (no global registry).
 *
 * Thread safety:
 *   FreeListArena is NOT thread-safe, so all allocations/deallocations
 *   are protected by arena_mutex_.
 */
class ArrayArena {
 public:
  /**
   * @param initial_size  Initial arena size (default: 2 MB)
   */
  explicit ArrayArena(size_t initial_size = 2 * 1024 * 1024)
      : arena_(std::make_unique<FreeListArena>(initial_size, 16)) {}

  // Non-copyable (contains mutex)
  ArrayArena(const ArrayArena&) = delete;
  ArrayArena& operator=(const ArrayArena&) = delete;

  /**
   * Allocate a new array with the given element type and capacity.
   * Initializes header (ref_count = 0, length = 0, arena = this)
   * and zeroes element data.
   * The returned ArrayRef constructor bumps ref_count to 1.
   *
   * @param elem_type  Element ValueType (e.g. INT32, DOUBLE, STRING)
   * @param capacity   Number of element slots to allocate
   * @return Ok(ArrayRef) with ref_count = 1, or Error with reason on failure
   */
  arrow::Result<ArrayRef> allocate(ValueType elem_type, uint32_t capacity) {
    // capacity 0 is valid: return empty (null) ArrayRef for empty arrays
    if (capacity == 0) {
      return ArrayRef{};
    }

    const size_t elem_sz = get_type_size(elem_type);
    const size_t data_bytes = elem_sz * capacity;
    const size_t alloc_size = ArrayRef::HEADER_SIZE + data_bytes;

    std::lock_guard<std::mutex> lock(arena_mutex_);
    void* raw = arena_->allocate(alloc_size);
    if (!raw) {
      return arrow::Status::OutOfMemory(
          "ArrayArena::allocate: arena allocation failed (requested ",
          alloc_size, " bytes)");
    }

    init_header(raw, capacity);

    char* data = static_cast<char*>(raw) + ArrayRef::HEADER_SIZE;
    zero_init_elements(data, elem_type, capacity);

    return ArrayRef{data, elem_type};
  }

  /**
   * Allocate and populate an array from existing data.
   *
   * @param elem_type  Element ValueType
   * @param elements   Pointer to element data to copy
   * @param count      Number of elements
   * @param capacity   Capacity (must be >= count; if 0, uses count)
   * @return Ok(ArrayRef) with ref_count = 1 and length = count, or Error
   */
  arrow::Result<ArrayRef> allocate_with_data(ValueType elem_type,
                                             const void* elements,
                                             uint32_t count,
                                             uint32_t capacity = 0) {
    if (capacity < count) capacity = count;
    // capacity 0 is valid (count 0): return empty ArrayRef
    if (capacity == 0) {
      return ArrayRef{};
    }

    const size_t elem_sz = get_type_size(elem_type);
    const size_t data_bytes = elem_sz * capacity;
    const size_t alloc_size = ArrayRef::HEADER_SIZE + data_bytes;

    std::lock_guard<std::mutex> lock(arena_mutex_);
    void* raw = arena_->allocate(alloc_size);
    if (!raw) {
      return arrow::Status::OutOfMemory(
          "ArrayArena::allocate_with_data: arena allocation failed (requested ",
          alloc_size, " bytes)");
    }

    auto* header = init_header(raw, capacity);
    header->length = count;

    char* data = static_cast<char*>(raw) + ArrayRef::HEADER_SIZE;
    std::memcpy(data, elements, elem_sz * count);
    if (capacity > count) {
      zero_init_elements(data + elem_sz * count, elem_type, capacity - count);
    }

    return ArrayRef{data, elem_type};
  }

  /**
   * Append one element to an array. Two strategies:
   *
   * 1. If there's spare capacity (length < capacity), write in-place.
   *    -> O(1), no allocation.
   *
   * 2. If full, allocate a new array with 2 x capacity, copy old elements,
   *    append the new one, and mark the old array for deletion.
   *    -> Amortized O(1).
   *
   * @param ref        ArrayRef to append to (updated in-place if reallocated)
   * @param element    Pointer to the element data to append
   * @return Ok on success; Error with reason if ref is null or allocation fails
   */
  arrow::Status append(ArrayRef& ref, const void* element) {
    if (ref.is_null()) {
      return arrow::Status::Invalid(
          "ArrayArena::append: ArrayRef is null (cannot append to null ref)");
    }

    auto* header = get_header(ref);
    if (!header) {
      return arrow::Status::Invalid(
          "ArrayArena::append: invalid ArrayRef (header is null)");
    }

    const size_t elem_sz = ref.elem_size();

    if (header->length < header->capacity) {
      char* dest = ref.mutable_element_ptr(header->length);
      std::memcpy(dest, element, elem_sz);
      header->length++;
      return arrow::Status::OK();
    }

    // Full - reallocate with 2 x capacity
    const uint32_t new_cap = header->capacity * 2;
    const uint32_t old_len = header->length;

    ARROW_ASSIGN_OR_RAISE(ArrayRef new_ref, allocate(ref.elem_type(), new_cap));

    auto* new_header = get_header(new_ref);
    std::memcpy(new_ref.data(), ref.data(), elem_sz * old_len);
    std::memcpy(new_ref.data() + elem_sz * old_len, element, elem_sz);
    new_header->length = old_len + 1;

    header->mark_for_deletion();

    ref = std::move(new_ref);
    return arrow::Status::OK();
  }

  /**
   * Create a copy of an existing array (for copy-on-write / versioning).
   * The new array has the same length and capacity as the original.
   *
   * @param src  Source ArrayRef to copy
   * @return Ok(new ArrayRef) with independent data, or Error with reason
   */
  arrow::Result<ArrayRef> copy(const ArrayRef& src) {
    if (src.is_null()) {
      return arrow::Status::Invalid(
          "ArrayArena::copy: source ArrayRef is null");
    }

    const auto* header = get_header_const(src);
    if (!header) {
      return arrow::Status::Invalid(
          "ArrayArena::copy: invalid source ArrayRef (header is null)");
    }

    return allocate_with_data(src.elem_type(), src.data(), header->length,
                              header->capacity);
  }

  /**
   * Mark an array for deferred deletion.
   * The actual deallocation happens when the last ArrayRef is destroyed
   * and release() is called.
   */
  void mark_for_deletion(const ArrayRef& ref) {
    if (ref.is_null()) return;
    if (auto* h = get_header_mut(ref)) {
      h->mark_for_deletion();
    }
  }

  /**
   * Deallocate an array's memory back to the FreeListArena.
   * Called by ArrayRef::release() when ref_count reaches 0 and the
   * array is marked for deletion.
   *
   * @param data  Pointer to element data (NOT to header)
   */
  void release_array(char* data) {
    if (!data) return;

    auto* header =
        reinterpret_cast<ArrayRef::ArrayHeader*>(data - ArrayRef::HEADER_SIZE);

    std::lock_guard<std::mutex> lock(arena_mutex_);
    arena_->deallocate(header);
  }

  // ========================================================================
  // Statistics
  // ========================================================================

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
  /** Initialize a freshly allocated header block. */
  ArrayRef::ArrayHeader* init_header(void* raw, uint32_t capacity) {
    auto* header = static_cast<ArrayRef::ArrayHeader*>(raw);
    header->ref_count.store(0, std::memory_order_relaxed);
    header->flags = 0;
    header->length = 0;
    header->capacity = capacity;
    header->arena = this;
    return header;
  }

  static ArrayRef::ArrayHeader* get_header(const ArrayRef& ref) {
    if (ref.is_null()) return nullptr;
    return reinterpret_cast<ArrayRef::ArrayHeader*>(ref.data() -
                                                    ArrayRef::HEADER_SIZE);
  }

  static const ArrayRef::ArrayHeader* get_header_const(const ArrayRef& ref) {
    return get_header(ref);
  }

  static ArrayRef::ArrayHeader* get_header_mut(const ArrayRef& ref) {
    return get_header(ref);
  }

  /**
   * Initialize element memory.
   * Uses placement-new for types with non-trivial constructors
   * (StringRef, ArrayRef); memset(0) for primitives.
   */
  static void zero_init_elements(char* data, ValueType elem_type,
                                 uint32_t count) {
    if (is_string_type(elem_type)) {
      for (uint32_t i = 0; i < count; ++i) {
        new (data + i * sizeof(StringRef)) StringRef();
      }
    } else if (is_array_type(elem_type)) {
      for (uint32_t i = 0; i < count; ++i) {
        new (data + i * sizeof(ArrayRef)) ArrayRef();
      }
    } else {
      std::memset(data, 0, get_type_size(elem_type) * count);
    }
  }

  std::unique_ptr<FreeListArena> arena_;
  mutable std::mutex arena_mutex_;
};

// ============================================================================
// ArrayRef::release() implementation (after ArrayArena is fully defined)
// ============================================================================

inline void ArrayRef::release() {
  if (!data_) return;
  if (auto* h = get_header()) {
    const int32_t old_count =
        h->ref_count.fetch_sub(1, std::memory_order_acq_rel);
    if (old_count == 1 && h->is_marked_for_deletion() && h->arena) {
      h->arena->release_array(data_);
    }
  }
  data_ = nullptr;
  elem_type_ = ValueType::NA;
}

}  // namespace tundradb

#endif  // ARRAY_ARENA_HPP
