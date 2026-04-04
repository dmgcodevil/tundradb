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

    active_allocs_.fetch_add(1, std::memory_order_relaxed);
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
    copy_init_elements(data, static_cast<const char*>(elements), elem_type,
                       count);
    if (capacity > count) {
      zero_init_elements(data + elem_sz * count, elem_type, capacity - count);
    }

    active_allocs_.fetch_add(1, std::memory_order_relaxed);
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
   * IMPORTANT: This mutates the array through `ref`. In versioned storage,
   * the caller MUST first copy() the ArrayRef to avoid corrupting older
   * versions that share the same underlying data (copy-on-write).
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

    if (header->length < header->capacity) {
      char* dest = ref.mutable_element_ptr(header->length);
      assign_element(dest, element, ref.elem_type());
      header->length++;
      return arrow::Status::OK();
    }

    // Full - reallocate with 2 x capacity using allocate_with_data
    // which properly handles ref-counted element types.
    const uint32_t new_cap = header->capacity * 2;
    const uint32_t old_len = header->length;

    ARROW_ASSIGN_OR_RAISE(
        ArrayRef new_ref,
        allocate_with_data(ref.elem_type(), ref.data(), old_len, new_cap));

    assign_element(new_ref.mutable_element_ptr(old_len), element,
                   ref.elem_type());
    auto* new_header = get_header(new_ref);
    new_header->length = old_len + 1;

    header->mark_for_deletion();

    ref = std::move(new_ref);
    return arrow::Status::OK();
  }

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
                               uint32_t extra_capacity = 0) {
    if (src.is_null()) {
      return arrow::Status::Invalid(
          "ArrayArena::copy: source ArrayRef is null");
    }

    const auto* header = get_header_const(src);
    if (!header) {
      return arrow::Status::Invalid(
          "ArrayArena::copy: invalid source ArrayRef (header is null)");
    }

    const uint32_t new_capacity = header->capacity + extra_capacity;
    return allocate_with_data(src.elem_type(), src.data(), header->length,
                              new_capacity);
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
   * For arrays of non-trivial types (STRING, ARRAY), element destructors
   * are called first so that nested ref-counted objects are properly released.
   *
   * @param data      Pointer to element data (NOT to header)
   * @param elem_type Element type (NA = skip element cleanup)
   */
  void release_array(char* data, ValueType elem_type = ValueType::NA) {
    if (!data) return;

    auto* header =
        reinterpret_cast<ArrayRef::ArrayHeader*>(data - ArrayRef::HEADER_SIZE);
    if (!header->arena) return;  // already released
    header->arena = nullptr;     // prevent double-free

    destruct_elements(data, elem_type, header->length);

    active_allocs_.fetch_sub(1, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(arena_mutex_);
    arena_->deallocate(header);
  }

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
   * Destruct non-trivial elements before freeing array memory.
   *
   * For STRING elements: marks each string for deletion, then calls
   * the destructor. The destructor decrements ref_count; when it hits 0
   * with the deletion flag set, the string memory is freed.
   *
   * For ARRAY elements: calls the destructor, which triggers the same
   * release_array chain recursively.
   *
   * Primitives (INT32, DOUBLE, etc.) have trivial destructors - skip them.
   */
  static void destruct_elements(char* data, ValueType elem_type,
                                uint32_t count) {
    if (count == 0) return;

    if (is_string_type(elem_type)) {
      for (uint32_t i = 0; i < count; ++i) {
        auto* sr = reinterpret_cast<StringRef*>(data + i * sizeof(StringRef));
        if (!sr->is_null()) {
          // Must mark for deletion first - StringRef::release() only
          // frees memory when BOTH ref_count==0 AND marked_for_deletion.
          auto* hdr = reinterpret_cast<StringRef::StringHeader*>(
              const_cast<char*>(sr->data() - StringRef::HEADER_SIZE));
          hdr->mark_for_deletion();
        }
        sr->~StringRef();
      }
    } else if (is_array_type(elem_type)) {
      for (uint32_t i = 0; i < count; ++i) {
        auto* ar = reinterpret_cast<ArrayRef*>(data + i * sizeof(ArrayRef));
        if (!ar->is_null()) {
          // ArrayRef destructor calls release() which calls release_array
          // recursively if this was the last reference.
          auto* hdr = reinterpret_cast<ArrayRef::ArrayHeader*>(
              ar->data() - ArrayRef::HEADER_SIZE);
          hdr->mark_for_deletion();
        }
        ar->~ArrayRef();
      }
    }
    // Primitives: trivial destructors, nothing to do.
  }

  /**
   * Copy-construct elements from src to raw (uninitialized) dst memory.
   * Uses copy constructors for ref-counted types (StringRef, ArrayRef)
   * to properly increment reference counts; memcpy for primitives.
   *
   * IMPORTANT: dst must be RAW uninitialized memory (no live objects).
   */
  static void copy_init_elements(char* dst, const char* src,
                                 ValueType elem_type, uint32_t count) {
    if (count == 0) return;
    if (is_string_type(elem_type)) {
      for (uint32_t i = 0; i < count; ++i) {
        const auto* s =
            reinterpret_cast<const StringRef*>(src + i * sizeof(StringRef));
        new (dst + i * sizeof(StringRef)) StringRef(*s);
      }
    } else if (is_array_type(elem_type)) {
      for (uint32_t i = 0; i < count; ++i) {
        const auto* a =
            reinterpret_cast<const ArrayRef*>(src + i * sizeof(ArrayRef));
        new (dst + i * sizeof(ArrayRef)) ArrayRef(*a);
      }
    } else {
      std::memcpy(dst, src, get_type_size(elem_type) * count);
    }
  }

  /**
   * Copy-assign a single element from src to an INITIALIZED dst slot.
   * Uses copy assignment for ref-counted types (properly releases old,
   * increments new); memcpy for primitives.
   */
  static void assign_element(char* dst, const void* src, ValueType elem_type) {
    if (is_string_type(elem_type)) {
      *reinterpret_cast<StringRef*>(dst) =
          *reinterpret_cast<const StringRef*>(src);
    } else if (is_array_type(elem_type)) {
      *reinterpret_cast<ArrayRef*>(dst) =
          *reinterpret_cast<const ArrayRef*>(src);
    } else {
      std::memcpy(dst, src, get_type_size(elem_type));
    }
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
  std::atomic<int64_t> active_allocs_{0};
};

// ============================================================================
// ArrayRef::release() implementation (after ArrayArena is fully defined)
// ============================================================================

inline void ArrayRef::release() {
  if (!data_) return;
  if (auto* h = get_header()) {
    assert(h->ref_count.load(std::memory_order_relaxed) > 0 &&
           "ArrayRef::release() called with ref_count already 0 — "
           "double-release or missing ref-count increment");

    const int32_t old_count =
        h->ref_count.fetch_sub(1, std::memory_order_acq_rel);
    if (old_count == 1 && h->is_marked_for_deletion() && h->arena) {
      h->arena->release_array(data_, elem_type_);
    }
  }
  data_ = nullptr;
  elem_type_ = ValueType::NA;
}

}  // namespace tundradb

#endif  // ARRAY_ARENA_HPP
