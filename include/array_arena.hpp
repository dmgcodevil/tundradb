#ifndef ARRAY_ARENA_HPP
#define ARRAY_ARENA_HPP

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
 *
 * Memory layout per array allocation:
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ ArrayHeader (16B)  │  element data (elem_size x capacity)        │
 * │ [ref_count|flags|  │  [elem0][elem1][...][elemN-1][...padding]   │
 * │  length|capacity]  │                                             │
 * └──────────────────────────────────────────────────────────────────┘
 *                      ↑
 *                  ArrayRef::data_ points here
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
   * Initializes header (ref_count = 0, length = 0) and zeroes element data.
   * The returned ArrayRef constructor bumps ref_count to 1.
   *
   * @param elem_type  Element ValueType (e.g. INT32, DOUBLE, STRING)
   * @param capacity   Number of element slots to allocate
   * @return ArrayRef with ref_count = 1, or null ref on failure
   */
  ArrayRef allocate(ValueType elem_type, uint32_t capacity) {
    if (capacity == 0) {
      return {};
    }

    const size_t elem_sz = get_type_size(elem_type);
    const size_t data_bytes = elem_sz * capacity;
    const size_t alloc_size = ArrayRef::HEADER_SIZE + data_bytes;

    std::lock_guard<std::mutex> lock(arena_mutex_);
    void* raw = arena_->allocate(alloc_size);
    if (!raw) {
      return {};  // allocation failed
    }

    // Initialize header
    auto* header = static_cast<ArrayRef::ArrayHeader*>(raw);
    header->ref_count.store(0,
                            std::memory_order_relaxed);  // ArrayRef ctor -> 1
    header->flags = 0;
    header->length = 0;
    header->capacity = capacity;

    char* data = static_cast<char*>(raw) + ArrayRef::HEADER_SIZE;
    zero_init_elements(data, elem_type, capacity);

    return {data, elem_type};
  }

  /**
   * Allocate and populate an array from existing data.
   *
   * @param elem_type  Element ValueType
   * @param elements   Pointer to element data to copy
   * @param count      Number of elements
   * @param capacity   Capacity (must be >= count; if 0, uses count)
   * @return ArrayRef with ref_count = 1 and length = count
   */
  ArrayRef allocate_with_data(ValueType elem_type, const void* elements,
                              uint32_t count, uint32_t capacity = 0) {
    if (capacity < count) capacity = count;
    if (capacity == 0) return {};

    const size_t elem_sz = get_type_size(elem_type);
    const size_t data_bytes = elem_sz * capacity;
    const size_t alloc_size = ArrayRef::HEADER_SIZE + data_bytes;

    std::lock_guard<std::mutex> lock(arena_mutex_);
    void* raw = arena_->allocate(alloc_size);
    if (!raw) return {};

    auto* header = static_cast<ArrayRef::ArrayHeader*>(raw);
    header->ref_count.store(0, std::memory_order_relaxed);
    header->flags = 0;
    header->length = count;
    header->capacity = capacity;

    char* data = static_cast<char*>(raw) + ArrayRef::HEADER_SIZE;
    std::memcpy(data, elements, elem_sz * count);
    if (capacity > count) {
      zero_init_elements(data + elem_sz * count, elem_type, capacity - count);
    }

    return {data, elem_type};
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
   * @return true on success, false on allocation failure
   */
  bool append(ArrayRef& ref, const void* element) {
    if (ref.is_null()) return false;

    auto* header = get_header(ref);
    if (!header) return false;

    const size_t elem_sz = ref.elem_size();

    if (header->length < header->capacity) {
      // In-place append - no allocation needed
      char* dest = ref.mutable_element_ptr(header->length);
      std::memcpy(dest, element, elem_sz);
      header->length++;
      return true;
    }

    // Full - reallocate with 2 x capacity
    const uint32_t new_cap = header->capacity * 2;
    const uint32_t old_len = header->length;

    ArrayRef new_ref = allocate(ref.elem_type(), new_cap);
    if (new_ref.is_null()) return false;

    // Copy old data
    auto* new_header = get_header(new_ref);
    std::memcpy(new_ref.data(), ref.data(), elem_sz * old_len);
    // Append new element
    std::memcpy(new_ref.data() + elem_sz * old_len, element, elem_sz);
    new_header->length = old_len + 1;

    // Mark old array for deletion
    header->mark_for_deletion();

    // Replace ref (ArrayRef assignment handles ref counting)
    ref = std::move(new_ref);
    return true;
  }

  /**
   * Create a copy of an existing array (for copy-on-write / versioning).
   * The new array has the same length and capacity as the original.
   *
   * @param src  Source ArrayRef to copy
   * @return New ArrayRef with independent data, or null ref on failure
   */
  ArrayRef copy(const ArrayRef& src) {
    if (src.is_null()) return {};

    const auto* header = get_header_const(src);
    if (!header) return {};

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
   * Release an array's memory.
   * Called when the last ArrayRef is destroyed AND the array is marked
   * for deletion.
   *
   * @param data  Pointer to element data (NOT to header)
   */
  void release_array(char* data) {
    if (!data) return;

    auto* header =
        reinterpret_cast<ArrayRef::ArrayHeader*>(data - ArrayRef::HEADER_SIZE);

    // Check ref count
    const int32_t old_count =
        header->ref_count.fetch_sub(1, std::memory_order_acq_rel);

    if (old_count == 1 && header->is_marked_for_deletion()) {
      std::lock_guard<std::mutex> lock(arena_mutex_);
      arena_->deallocate(header);
    }
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
  mutable std::mutex arena_mutex_;  // FreeListArena is NOT thread-safe
};

}  // namespace tundradb

#endif  // ARRAY_ARENA_HPP
