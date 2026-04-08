#include "memory/array_arena.hpp"

namespace tundradb {

// ============================================================================
// ArrayArena
// ============================================================================

arrow::Result<ArrayRef> ArrayArena::allocate(ValueType elem_type,
                                             uint32_t capacity) {
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
        "ArrayArena::allocate: arena allocation failed (requested ", alloc_size,
        " bytes)");
  }

  init_header(raw, capacity);

  char* data = static_cast<char*>(raw) + ArrayRef::HEADER_SIZE;
  zero_init_elements(data, elem_type, capacity);

  active_allocs_.fetch_add(1, std::memory_order_relaxed);
  return ArrayRef{data, elem_type};
}

arrow::Result<ArrayRef> ArrayArena::allocate_with_data(ValueType elem_type,
                                                       const void* elements,
                                                       uint32_t count,
                                                       uint32_t capacity) {
  if (capacity < count) capacity = count;
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

arrow::Status ArrayArena::append(ArrayRef& ref, const void* element) {
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

arrow::Result<ArrayRef> ArrayArena::copy(const ArrayRef& src,
                                         uint32_t extra_capacity) {
  if (src.is_null()) {
    return arrow::Status::Invalid("ArrayArena::copy: source ArrayRef is null");
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

void ArrayArena::mark_for_deletion(const ArrayRef& ref) {
  if (ref.is_null()) return;
  if (auto* h = get_header_mut(ref)) {
    h->mark_for_deletion();
  }
}

void ArrayArena::release_array(char* data, ValueType elem_type) {
  if (!data) return;

  auto* header =
      reinterpret_cast<ArrayRef::ArrayHeader*>(data - ArrayRef::HEADER_SIZE);
  if (!header->arena) return;
  header->arena = nullptr;

  destruct_elements(data, elem_type, header->length);

  active_allocs_.fetch_sub(1, std::memory_order_relaxed);

  std::lock_guard<std::mutex> lock(arena_mutex_);
  arena_->deallocate(header);
}

void ArrayArena::reset() {
  std::lock_guard<std::mutex> lock(arena_mutex_);
  arena_->reset();
}

void ArrayArena::clear() {
  std::lock_guard<std::mutex> lock(arena_mutex_);
  arena_->clear();
}

ArrayRef::ArrayHeader* ArrayArena::init_header(void* raw, uint32_t capacity) {
  auto* header = static_cast<ArrayRef::ArrayHeader*>(raw);
  header->ref_count.store(0, std::memory_order_relaxed);
  header->flags = 0;
  header->length = 0;
  header->capacity = capacity;
  header->arena = this;
  return header;
}

void ArrayArena::destruct_elements(char* data, ValueType elem_type,
                                   uint32_t count) {
  if (count == 0) return;

  if (is_string_type(elem_type)) {
    for (uint32_t i = 0; i < count; ++i) {
      auto* sr = reinterpret_cast<StringRef*>(data + i * sizeof(StringRef));
      if (!sr->is_null()) {
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
        auto* hdr = reinterpret_cast<ArrayRef::ArrayHeader*>(
            ar->data() - ArrayRef::HEADER_SIZE);
        hdr->mark_for_deletion();
      }
      ar->~ArrayRef();
    }
  }
}

void ArrayArena::copy_init_elements(char* dst, const char* src,
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

void ArrayArena::assign_element(char* dst, const void* src,
                                ValueType elem_type) {
  if (is_string_type(elem_type)) {
    *reinterpret_cast<StringRef*>(dst) =
        *reinterpret_cast<const StringRef*>(src);
  } else if (is_array_type(elem_type)) {
    *reinterpret_cast<ArrayRef*>(dst) = *reinterpret_cast<const ArrayRef*>(src);
  } else {
    std::memcpy(dst, src, get_type_size(elem_type));
  }
}

void ArrayArena::zero_init_elements(char* data, ValueType elem_type,
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

// ============================================================================
// ArrayRef::release() implementation (after ArrayArena is fully defined)
// ============================================================================

void ArrayRef::release() {
  if (!data_) return;
  if (auto* h = get_header()) {
    assert(h->ref_count.load(std::memory_order_relaxed) > 0 &&
           "ArrayRef::release() called with ref_count already 0 - "
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
