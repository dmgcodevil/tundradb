#include "memory/map_arena.hpp"

namespace tundradb {

// ============================================================================
// MapArena
// ============================================================================

arrow::Result<MapRef> MapArena::allocate(uint32_t capacity) {
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

arrow::Result<MapRef> MapArena::copy(const MapRef& src,
                                     uint32_t extra_capacity) {
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

  for (uint32_t i = 0; i < src_count; ++i) {
    const auto* src_entry = src.entry_ptr(i);
    auto* dst_entry = new_ref.mutable_entry_ptr(i);
    copy_init_entry(dst_entry, src_entry);
  }

  return new_ref;
}

void MapArena::mark_for_deletion(const MapRef& ref) {
  if (ref.is_null()) return;
  if (auto* h = get_header_mut(ref)) {
    h->mark_for_deletion();
  }
}

void MapArena::release_map(char* data) {
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

int32_t MapArena::find_entry(const MapRef& ref, const std::string& key) {
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

arrow::Status MapArena::set_entry(MapRef& ref, const StringRef& key,
                                  ValueType vtype, const void* value_ptr) {
  if (ref.is_null()) {
    return arrow::Status::Invalid("MapArena::set_entry: MapRef is null");
  }

  auto* header = get_header(ref);
  if (!header) {
    return arrow::Status::Invalid("MapArena::set_entry: invalid header");
  }

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

  if (header->count >= header->capacity) {
    return arrow::Status::CapacityError(
        "MapArena::set_entry: map is full (count=", header->count,
        ", capacity=", header->capacity, ")");
  }

  auto* entry = ref.mutable_entry_ptr(header->count);
  entry->key.~StringRef();
  new (&entry->key) StringRef(key);
  entry->value_type = static_cast<uint8_t>(vtype);
  copy_value_into_entry(entry, vtype, value_ptr);
  header->count++;
  return arrow::Status::OK();
}

bool MapArena::remove_entry(MapRef& ref, const std::string& key) {
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
      auto* vacant = ref.mutable_entry_ptr(header->count - 1);
      new (vacant) MapEntry();
      header->count--;
      return true;
    }
  }
  return false;
}

void MapArena::reset() {
  std::lock_guard<std::mutex> lock(arena_mutex_);
  arena_->reset();
}

void MapArena::clear() {
  std::lock_guard<std::mutex> lock(arena_mutex_);
  arena_->clear();
}

MapRef::MapHeader* MapArena::init_header(void* raw, uint32_t capacity) {
  auto* header = static_cast<MapRef::MapHeader*>(raw);
  header->ref_count.store(0, std::memory_order_relaxed);
  header->flags = 0;
  header->count = 0;
  header->capacity = capacity;
  header->arena = this;
  return header;
}

void MapArena::zero_init_entries(char* data, uint32_t count) {
  for (uint32_t i = 0; i < count; ++i) {
    new (data + i * sizeof(MapEntry)) MapEntry();
  }
}

void MapArena::copy_init_entry(MapEntry* dst, const MapEntry* src) {
  dst->key.~StringRef();
  new (&dst->key) StringRef(src->key);
  dst->value_type = src->value_type;
  std::memset(dst->pad, 0, sizeof(dst->pad));
  copy_value_into_entry(dst, static_cast<ValueType>(src->value_type),
                        src->value);
}

void MapArena::copy_value_into_entry(MapEntry* entry, ValueType vtype,
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
    size_t n = get_type_size(vtype);
    std::memset(entry->value, 0, MapEntry::VALUE_SIZE);
    std::memcpy(entry->value, src, n);
  }
}

void MapArena::destruct_entry_value(MapEntry* entry) {
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

void MapArena::destruct_entry(MapEntry* entry) {
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

void MapArena::move_entry(MapEntry* dst, MapEntry* src) {
  new (&dst->key) StringRef(std::move(src->key));
  dst->value_type = src->value_type;
  std::memcpy(dst->value, src->value, MapEntry::VALUE_SIZE);
  src->value_type = static_cast<uint8_t>(ValueType::NA);
  std::memset(src->value, 0, MapEntry::VALUE_SIZE);
}

void MapArena::destruct_entries(char* data, uint32_t count) {
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

// ============================================================================
// MapRef methods
// ============================================================================

void MapRef::release() {
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

Value MapRef::get_value(const std::string& key) const {
  int32_t idx = MapArena::find_entry(*this, key);
  if (idx < 0) return Value{};
  const auto* entry = entry_ptr(static_cast<uint32_t>(idx));
  return Value::read_value_from_memory(
      entry->value, static_cast<ValueType>(entry->value_type));
}

bool MapRef::contains(const std::string& key) const {
  return MapArena::find_entry(*this, key) >= 0;
}

}  // namespace tundradb
