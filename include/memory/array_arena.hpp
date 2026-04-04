#ifndef ARRAY_ARENA_HPP
#define ARRAY_ARENA_HPP

#include <arrow/api.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>

#include "common/value_type.hpp"
#include "memory/array_ref.hpp"
#include "memory/free_list_arena.hpp"
#include "memory/string_ref.hpp"

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
  arrow::Result<ArrayRef> allocate(ValueType elem_type, uint32_t capacity);

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
                                             uint32_t capacity = 0);

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
   * IMPORTANT: This mutates the array through `ref`. In versioned storage,
   * the caller MUST first copy() the ArrayRef to avoid corrupting older
   * versions that share the same underlying data (copy-on-write).
   *
   * @param ref        ArrayRef to append to (updated in-place if reallocated)
   * @param element    Pointer to the element data to append
   * @return Ok on success; Error with reason if ref is null or allocation fails
   */
  arrow::Status append(ArrayRef& ref, const void* element);

  /**
   * Create a copy of an existing array (for copy-on-write / versioning).
   *
   * @param src            Source ArrayRef to copy
   * @param extra_capacity Additional element slots beyond the original
   * capacity. Use this when the caller knows it will append soon, to avoid a
   * second reallocation inside append().
   * @return Ok(new ArrayRef) with independent data, or Error with reason
   */
  arrow::Result<ArrayRef> copy(const ArrayRef& src,
                               uint32_t extra_capacity = 0);

  /**
   * Mark an array for deferred deletion.
   * The actual deallocation happens when the last ArrayRef is destroyed
   * and release() is called.
   */
  void mark_for_deletion(const ArrayRef& ref);

  /**
   * Deallocate an array's memory back to the FreeListArena.
   * Called by ArrayRef::release() when ref_count reaches 0 and the
   * array is marked for deletion.
   *
   * For arrays of non-trivial types (STRING, ARRAY), element destructors
   * are called first so that nested ref-counted objects are properly released.
   *
   * @param data      Pointer to element data (NOT to header)
   * @param elem_type Element type (NA = skip element cleanup)
   */
  void release_array(char* data, ValueType elem_type = ValueType::NA);

  // ========================================================================
  // Statistics
  // ========================================================================

  /** Number of live (allocated but not yet freed) arrays in this arena. */
  int64_t get_active_allocs() const {
    return active_allocs_.load(std::memory_order_relaxed);
  }

  size_t get_total_allocated() const { return arena_->get_total_allocated(); }

  size_t get_used_bytes() const { return arena_->get_used_bytes(); }

  size_t get_freed_bytes() const { return arena_->get_freed_bytes(); }

  void reset();
  void clear();

 private:
  ArrayRef::ArrayHeader* init_header(void* raw, uint32_t capacity);

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

  static void destruct_elements(char* data, ValueType elem_type,
                                uint32_t count);
  static void copy_init_elements(char* dst, const char* src,
                                 ValueType elem_type, uint32_t count);
  static void assign_element(char* dst, const void* src, ValueType elem_type);
  static void zero_init_elements(char* data, ValueType elem_type,
                                 uint32_t count);

  std::unique_ptr<FreeListArena> arena_;
  mutable std::mutex arena_mutex_;
  std::atomic<int64_t> active_allocs_{0};
};

}  // namespace tundradb

#endif  // ARRAY_ARENA_HPP
