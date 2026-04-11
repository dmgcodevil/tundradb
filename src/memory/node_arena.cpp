#include "memory/node_arena.hpp"

#include <cstring>

namespace tundradb {

// ===========================================================================
// VersionInfo
// ===========================================================================

const VersionInfo* VersionInfo::find_version_at_snapshot(
    uint64_t valid_time, uint64_t tx_time) const {
  const VersionInfo* current = this;
  while (current != nullptr) {
    if (current->is_visible_at(valid_time, tx_time)) {
      return current;
    }
    current = current->prev;
  }
  return nullptr;
}

const VersionInfo* VersionInfo::find_version_at_time(uint64_t ts) const {
  const VersionInfo* current = this;
  while (current != nullptr) {
    if (current->is_valid_at(ts)) return current;
    current = current->prev;
  }
  return nullptr;
}

size_t VersionInfo::count_versions() const {
  size_t count = 1;
  const VersionInfo* current = prev;
  while (current != nullptr) {
    count++;
    current = current->prev;
  }
  return count;
}

// ===========================================================================
// NodeHandle
// ===========================================================================

size_t NodeHandle::count_versions() const {
  if (!is_versioned()) return 1;
  return version_info_->count_versions();
}

const VersionInfo* NodeHandle::find_version_at_time(uint64_t ts) const {
  if (!is_versioned()) return nullptr;
  return version_info_->find_version_at_time(ts);
}

// ===========================================================================
// NodeArena - constructor / destructor
// ===========================================================================

NodeArena::NodeArena(std::unique_ptr<MemArena> mem_arena,
                     std::shared_ptr<LayoutRegistry> layout_registry,
                     std::unique_ptr<StringArena> string_arena,
                     bool enable_versioning)
    : mem_arena_(std::move(mem_arena)),
      layout_registry_(std::move(layout_registry)),
      string_arena_(string_arena ? std::move(string_arena)
                                 : std::make_unique<StringArena>()),
      array_arena_(std::make_unique<ArrayArena>()),
      map_arena_(std::make_unique<MapArena>()),
      versioning_enabled_(enable_versioning),
      version_counter_(0) {
  if (versioning_enabled_) {
    version_arena_ = std::make_unique<FreeListArena>(4 * 1024 * 1024);
  }
}

NodeArena::~NodeArena() {
  // VersionInfo objects are placement-new'd into version_arena_ memory.
  // Their SmallDenseMap members may heap-allocate, so we must call
  // destructors before the arena frees the underlying memory.
  for (auto* vi : version_infos_) {
    vi->~VersionInfo();
  }
}

// ===========================================================================
// NodeArena - public methods
// ===========================================================================

NodeHandle NodeArena::allocate_node(const std::string& schema_name) {
  const std::shared_ptr<SchemaLayout> layout =
      layout_registry_->get_layout(schema_name);
  if (!layout) {
    return NodeHandle{};
  }

  return allocate_node(layout);
}

NodeHandle NodeArena::allocate_node(
    const std::shared_ptr<SchemaLayout>& layout) {
  size_t node_size = layout->get_total_size_with_bitset();
  size_t alignment = layout->get_alignment();

  void* node_data = mem_arena_->allocate(node_size, alignment);
  if (!node_data) {
    return NodeHandle{};
  }

  layout->initialize_node_data(static_cast<char*>(node_data));

  if (versioning_enabled_) {
    void* version_info_memory =
        version_arena_->allocate(sizeof(VersionInfo), alignof(VersionInfo));
    if (!version_info_memory) {
      return NodeHandle{};
    }

    uint64_t now = get_current_timestamp_ns();
    auto* version_info = new (version_info_memory) VersionInfo();
    version_infos_.push_back(version_info);
    version_info->version_id = 0;
    version_info->valid_from = now;
    version_info->valid_to = std::numeric_limits<uint64_t>::max();
    version_info->prev = nullptr;

    return {node_data, node_size, layout->get_schema_name(), 1, version_info};
  }
  return {node_data, node_size, layout->get_schema_name()};
}

const char* NodeArena::get_value_ptr(
    const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const std::shared_ptr<Field>& field) {
  if (handle.is_null()) {
    return nullptr;
  }

  return layout->get_value_ptr(static_cast<const char*>(handle.ptr), field);
}

Value NodeArena::get_value(const NodeHandle& handle,
                           const std::shared_ptr<SchemaLayout>& layout,
                           const std::shared_ptr<Field>& field) {
  if (handle.is_null()) {
    return Value{};
  }

  if (handle.is_versioned()) {
    const FieldLayout* field_layout = layout->get_field_layout(field);
    if (!field_layout) {
      return Value{};
    }

    uint16_t field_idx = field_layout->index;

    const VersionInfo* current = handle.version_info_;
    while (current != nullptr) {
      auto it = current->updated_fields.find(field_idx);
      if (it != current->updated_fields.end()) {
        if (it->second == nullptr) {
          return Value{};
        }
        return Value::read_value_from_memory(it->second, field_layout->type);
      }
      current = current->prev;
    }

    return layout->get_value(static_cast<const char*>(handle.ptr), field);
  }

  return layout->get_value(static_cast<const char*>(handle.ptr), field);
}

arrow::Result<Value> NodeArena::prepare_append_value(
    const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const FieldLayout& field_layout, const Value& new_value) {
  if (!is_array_type(field_layout.type)) {
    return arrow::Status::TypeError(
        "APPEND is only valid for array fields, got: ",
        tundradb::to_string(field_layout.type));
  }

  ArrayRef current_ref;
  if (handle.is_versioned()) {
    auto [found, ptr] = get_field_ptr_from_version_chain(handle.version_info_,
                                                         field_layout.index);
    if (found && ptr) {
      current_ref = *reinterpret_cast<const ArrayRef*>(ptr);
    } else if (!found) {
      const char* base_ptr = layout->get_value_ptr(
          static_cast<const char*>(handle.ptr), field_layout.index);
      if (base_ptr) {
        current_ref = *reinterpret_cast<const ArrayRef*>(base_ptr);
      }
    }
  }

  if (new_value.holds_raw_array()) {
    const auto& elems = new_value.as_raw_array();
    if (elems.empty()) {
      if (current_ref.is_null()) return Value{ArrayRef{}};
      ARROW_ASSIGN_OR_RAISE(ArrayRef copy, array_arena_->copy(current_ref));
      return Value{std::move(copy)};
    }
    if (current_ref.is_null()) {
      ARROW_ASSIGN_OR_RAISE(ArrayRef arr_ref,
                            store_raw_array(field_layout.type_desc, elems));
      return Value{std::move(arr_ref)};
    }
    const auto n = static_cast<uint32_t>(elems.size());
    ARROW_ASSIGN_OR_RAISE(
        ArrayRef new_ref,
        array_arena_->copy(current_ref, grow_for_append(current_ref, n)));
    for (const auto& elem : elems) {
      ARROW_RETURN_NOT_OK(
          append_single_element(new_ref, field_layout.type_desc, elem));
    }
    return Value{std::move(new_ref)};
  }

  if (current_ref.is_null()) {
    const std::vector<Value> elems = {new_value};
    ARROW_ASSIGN_OR_RAISE(ArrayRef arr_ref,
                          store_raw_array(field_layout.type_desc, elems));
    return Value{std::move(arr_ref)};
  }
  ARROW_ASSIGN_OR_RAISE(
      ArrayRef new_ref,
      array_arena_->copy(current_ref, grow_for_append(current_ref, 1)));
  ARROW_RETURN_NOT_OK(
      append_single_element(new_ref, field_layout.type_desc, new_value));
  return Value{std::move(new_ref)};
}

arrow::Status NodeArena::set_field_value_v0(
    NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const std::shared_ptr<Field>& field, const Value& value) {
  assert(!handle.is_null());

  const FieldLayout* field_layout = layout->get_field_layout(field);
  if (!field_layout) {
    return arrow::Status::Invalid(
        "set_field_value_v0: field not found in layout");
  }

  return set_field_value_internal(handle.ptr, layout, field_layout, value);
}

arrow::Result<bool> NodeArena::apply_updates(
    NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const std::vector<FieldUpdate>& updates) {
  ARROW_ASSIGN_OR_RAISE(auto schema_updates,
                        resolve_field_indices(layout, updates));

  if (!versioning_enabled_ || !handle.is_versioned()) {
    ARROW_RETURN_NOT_OK(
        apply_non_versioned_schema_updates(handle, layout, schema_updates));
    return true;
  }

  if (schema_updates.empty()) {
    return true;
  }

  const uint64_t now = get_current_timestamp_ns();
  ARROW_ASSIGN_OR_RAISE(auto* new_vi, allocate_version(handle, now));

  ARROW_RETURN_NOT_OK(materialize_versioned_schema_fields(
      handle, layout, schema_updates, new_vi));

  handle.version_info_->valid_to = now;
  handle.version_info_ = new_vi;
  return true;
}

const char* NodeArena::get_value_ptr_at_version(
    const NodeHandle& handle, const VersionInfo* version,
    const std::shared_ptr<SchemaLayout>& layout,
    const std::shared_ptr<Field>& field) {
  const FieldLayout* field_layout = layout->get_field_layout(field);
  if (!field_layout) {
    return nullptr;
  }

  auto [found, field_ptr] =
      get_field_ptr_from_version_chain(version, field_layout->index);

  if (found) {
    return field_ptr;
  }

  return layout->get_value_ptr(static_cast<const char*>(handle.ptr),
                               field_layout->index);
}

arrow::Result<Value> NodeArena::get_value_at_version(
    const NodeHandle& handle, const VersionInfo* version,
    const std::shared_ptr<SchemaLayout>& layout,
    const std::shared_ptr<Field>& field) {
  const FieldLayout* field_layout = layout->get_field_layout(field);
  if (!field_layout) {
    return arrow::Status::KeyError("Field not found in layout");
  }

  auto [found, field_ptr] =
      get_field_ptr_from_version_chain(version, field_layout->index);

  if (found) {
    if (field_ptr == nullptr) {
      return Value{};
    }
    return layout->get_value_from_ptr(field_ptr, *field_layout);
  }

  return layout->get_value(static_cast<const char*>(handle.ptr), *field_layout);
}

// ===========================================================================
// NodeArena - private methods
// ===========================================================================

uint64_t NodeArena::get_current_timestamp_ns() {
  return Clock::instance().now_nanos();
}

arrow::Result<std::vector<IndexedFieldUpdate>> NodeArena::resolve_field_indices(
    const std::shared_ptr<SchemaLayout>& layout,
    const std::vector<FieldUpdate>& updates) {
  std::vector<IndexedFieldUpdate> result;
  result.reserve(updates.size());
  for (const auto& upd : updates) {
    const FieldLayout* fl = layout->get_field_layout(upd.field);
    if (!fl) {
      return arrow::Status::Invalid("Invalid field in apply_updates: ",
                                    upd.field->name());
    }
    result.push_back(
        {static_cast<uint16_t>(fl->index), upd.value, upd.op, upd.nested_path});
  }
  return result;
}

arrow::Status NodeArena::apply_non_versioned_schema_updates(
    NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const std::vector<IndexedFieldUpdate>& schema_updates) {
  for (const auto& upd : schema_updates) {
    if (upd.field_idx >= layout->get_fields().size()) {
      return arrow::Status::IndexError("Field index out of bounds");
    }
    const FieldLayout& fl = layout->get_fields()[upd.field_idx];

    if (!upd.nested_path.empty()) {
      ARROW_RETURN_NOT_OK(apply_nested_path_update_non_versioned(
          handle.ptr, layout, &fl, upd.nested_path, upd.value));
      continue;
    }

    ARROW_RETURN_NOT_OK(
        set_field_value_internal(handle.ptr, layout, &fl, upd.value, upd.op));
  }
  return arrow::Status::OK();
}

arrow::Result<VersionInfo*> NodeArena::allocate_version(
    const NodeHandle& handle, const uint64_t now) {
  void* vi_mem =
      version_arena_->allocate(sizeof(VersionInfo), alignof(VersionInfo));
  if (!vi_mem) {
    return arrow::Status::OutOfMemory("Failed to allocate VersionInfo");
  }
  const uint64_t vid =
      version_counter_.fetch_add(1, std::memory_order_relaxed) + 1;
  auto* new_vi = new (vi_mem) VersionInfo(vid, now, handle.version_info_);
  version_infos_.push_back(new_vi);
  return new_vi;
}

arrow::Status NodeArena::materialize_versioned_schema_fields(
    NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const std::vector<IndexedFieldUpdate>& schema_updates,
    VersionInfo* target_vi) {
  size_t total_size = 0;
  size_t max_alignment = 1;
  for (const auto& upd : schema_updates) {
    const FieldLayout& fl = layout->get_fields()[upd.field_idx];
    if (upd.op == UpdateType::APPEND || !upd.value.is_null()) {
      total_size += fl.size;
      max_alignment = std::max(max_alignment, fl.alignment);
    }
  }

  char* batch_memory = nullptr;
  if (total_size > 0) {
    batch_memory =
        static_cast<char*>(version_arena_->allocate(total_size, max_alignment));
    if (!batch_memory) {
      return arrow::Status::OutOfMemory(
          "Failed to batch allocate field storage");
    }
    std::memset(batch_memory, 0, total_size);
  }

  size_t offset = 0;
  for (const auto& upd : schema_updates) {
    const FieldLayout& fl = layout->get_fields()[upd.field_idx];

    if (!upd.nested_path.empty()) {
      ARROW_ASSIGN_OR_RAISE(
          Value map_val, apply_nested_path_update_versioned(
                             handle, layout, fl, upd.nested_path, upd.value));
      assert(batch_memory != nullptr);
      char* field_storage = batch_memory + offset;
      offset += fl.size;
      if (!write_value_to_memory(field_storage, fl.type, map_val)) {
        return arrow::Status::TypeError("Type mismatch writing MAP field");
      }
      target_vi->updated_fields[upd.field_idx] = field_storage;
      continue;
    }

    if (upd.op == UpdateType::SET && upd.value.is_null()) {
      target_vi->updated_fields[upd.field_idx] = nullptr;
      continue;
    }

    assert(batch_memory != nullptr);
    Value storage_value = upd.value;

    if (upd.op == UpdateType::APPEND) {
      ARROW_ASSIGN_OR_RAISE(
          storage_value, prepare_append_value(handle, layout, fl, upd.value));
    } else {
      if (upd.value.type() == ValueType::STRING &&
          upd.value.holds_std_string()) {
        ARROW_ASSIGN_OR_RAISE(
            StringRef str_ref,
            string_arena_->store_string_auto(upd.value.as_string()));
        storage_value = Value{str_ref, fl.type};
      } else if (upd.value.type() == ValueType::ARRAY &&
                 upd.value.holds_raw_array()) {
        ARROW_ASSIGN_OR_RAISE(
            ArrayRef arr_ref,
            store_raw_array(fl.type_desc, upd.value.as_raw_array()));
        storage_value = Value{std::move(arr_ref)};
      } else if (upd.value.type() == ValueType::MAP &&
                 upd.value.holds_raw_map()) {
        ARROW_ASSIGN_OR_RAISE(MapRef map_ref,
                              store_raw_map(upd.value.as_raw_map()));
        storage_value = Value{std::move(map_ref)};
      }
    }

    char* field_storage = batch_memory + offset;
    offset += fl.size;

    if (!write_value_to_memory(field_storage, fl.type, storage_value)) {
      return arrow::Status::TypeError("Type mismatch writing field value");
    }
    target_vi->updated_fields[upd.field_idx] = field_storage;
  }
  return arrow::Status::OK();
}

arrow::Status NodeArena::set_field_value_internal(
    void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
    const FieldLayout* field_layout, const Value& value,
    UpdateType update_type) {
  if (update_type == UpdateType::APPEND) {
    return append_to_array_field(node_ptr, layout, field_layout, value);
  }

  if (is_string_type(field_layout->type) &&
      is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
    Value old_value =
        layout->get_value(static_cast<char*>(node_ptr), *field_layout);
    if (!old_value.is_null() && old_value.type() != ValueType::NA) {
      try {
        const StringRef& old_str_ref = old_value.as_string_ref();
        if (!old_str_ref.is_null()) {
          string_arena_->mark_for_deletion(old_str_ref);
        }
      } catch (...) {
      }
    }
  }

  if (is_array_type(field_layout->type) &&
      is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
    Value old_value =
        layout->get_value(static_cast<char*>(node_ptr), *field_layout);
    if (!old_value.is_null() && old_value.holds_array_ref()) {
      const ArrayRef& old_arr_ref = old_value.as_array_ref();
      if (!old_arr_ref.is_null()) {
        array_arena_->mark_for_deletion(old_arr_ref);
      }
    }
  }

  if (is_map_type(field_layout->type) &&
      is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
    Value old_value =
        layout->get_value(static_cast<char*>(node_ptr), *field_layout);
    if (!old_value.is_null() && old_value.holds_map_ref()) {
      const MapRef& old_map_ref = old_value.as_map_ref();
      if (!old_map_ref.is_null()) {
        map_arena_->mark_for_deletion(old_map_ref);
      }
    }
  }

  if (value.type() == ValueType::STRING && value.holds_std_string()) {
    const std::string& str_content = value.as_string();
    ARROW_ASSIGN_OR_RAISE(StringRef str_ref,
                          string_arena_->store_string_auto(str_content));
    if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                 Value{str_ref, field_layout->type})) {
      return arrow::Status::Invalid("Failed to write string field value");
    }
    return arrow::Status::OK();
  }

  if (value.type() == ValueType::ARRAY && value.holds_raw_array()) {
    ARROW_ASSIGN_OR_RAISE(
        ArrayRef arr_ref,
        store_raw_array(field_layout->type_desc, value.as_raw_array()));
    if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                 Value{std::move(arr_ref)})) {
      return arrow::Status::Invalid("Failed to write array field value");
    }
    return arrow::Status::OK();
  }

  if (value.type() == ValueType::MAP && value.holds_raw_map()) {
    ARROW_ASSIGN_OR_RAISE(MapRef map_ref, store_raw_map(value.as_raw_map()));
    if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                 Value{std::move(map_ref)})) {
      return arrow::Status::Invalid("Failed to write map field value");
    }
    return arrow::Status::OK();
  }

  if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                               value)) {
    return arrow::Status::Invalid("Failed to write field value");
  }
  return arrow::Status::OK();
}

arrow::Result<Value> NodeArena::materialise_map_value(const Value& value) {
  if (value.type() == ValueType::STRING && value.holds_std_string()) {
    ARROW_ASSIGN_OR_RAISE(StringRef sr,
                          string_arena_->store_string_auto(value.as_string()));
    return Value{sr, ValueType::STRING};
  }
  return value;
}

arrow::Status NodeArena::set_nested_map_key(MapRef& ref, const std::string& key,
                                            const Value& value) {
  if (ref.is_null()) {
    ARROW_ASSIGN_OR_RAISE(ref, map_arena_->allocate());
  }

  ARROW_ASSIGN_OR_RAISE(Value mat, materialise_map_value(value));
  ARROW_ASSIGN_OR_RAISE(StringRef key_ref,
                        string_arena_->store_string_auto(key));

  ValueType vtype = mat.type();
  if (is_string_type(vtype)) vtype = ValueType::STRING;

  const void* vptr = nullptr;
  int32_t i32;
  int64_t i64;
  double d;
  float f;
  bool b;
  StringRef sr;
  ArrayRef ar;
  MapRef mr;

  switch (vtype) {
    case ValueType::INT32:
      i32 = mat.as_int32();
      vptr = &i32;
      break;
    case ValueType::INT64:
      i64 = mat.as_int64();
      vptr = &i64;
      break;
    case ValueType::DOUBLE:
      d = mat.as_double();
      vptr = &d;
      break;
    case ValueType::FLOAT:
      f = mat.as_float();
      vptr = &f;
      break;
    case ValueType::BOOL:
      b = mat.as_bool();
      vptr = &b;
      break;
    case ValueType::STRING:
      sr = mat.as_string_ref();
      vptr = &sr;
      break;
    case ValueType::ARRAY:
      if (!mat.holds_array_ref())
        return arrow::Status::Invalid(
            "nested_path update: raw arrays not supported");
      ar = mat.as_array_ref();
      vptr = &ar;
      break;
    case ValueType::MAP:
      if (!mat.holds_map_ref())
        return arrow::Status::Invalid(
            "nested_path update: raw maps not supported");
      mr = mat.as_map_ref();
      vptr = &mr;
      break;
    default:
      return arrow::Status::Invalid(
          "nested_path update: unsupported value type");
  }

  auto status = MapArena::set_entry(ref, key_ref, vtype, vptr);
  if (status.IsCapacityError()) {
    ARROW_ASSIGN_OR_RAISE(MapRef grown, map_arena_->copy(ref, ref.capacity()));
    map_arena_->mark_for_deletion(ref);
    ref = std::move(grown);
    return MapArena::set_entry(ref, key_ref, vtype, vptr);
  }
  return status;
}

arrow::Status NodeArena::apply_nested_path_update_non_versioned(
    void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
    const FieldLayout* fl, const std::vector<std::string>& nested_path,
    const Value& value) {
  if (nested_path.empty()) {
    return arrow::Status::Invalid(
        "nested_path update requires at least one path segment");
  }
  if (nested_path.size() > 1) {
    return arrow::Status::NotImplemented(
        "nested_path update depth > 1 is not implemented yet");
  }
  const std::string& key = nested_path.front();
  if (!is_map_type(fl->type)) {
    return arrow::Status::TypeError("nested_path update on non-map field: ",
                                    tundradb::to_string(fl->type));
  }

  auto* base = static_cast<char*>(node_ptr);
  MapRef current;
  if (is_field_set(base, fl->index)) {
    Value old = layout->get_value(base, *fl);
    if (!old.is_null() && old.holds_map_ref()) current = old.as_map_ref();
  }

  MapRef copy;
  if (current.is_null()) {
    ARROW_ASSIGN_OR_RAISE(copy, map_arena_->allocate());
  } else {
    ARROW_ASSIGN_OR_RAISE(copy, map_arena_->copy(current));
    map_arena_->mark_for_deletion(current);
  }

  ARROW_RETURN_NOT_OK(set_nested_map_key(copy, key, value));

  if (!layout->set_field_value(base, *fl, Value{std::move(copy)})) {
    return arrow::Status::Invalid(
        "Failed to write map field after nested_path update");
  }
  return arrow::Status::OK();
}

arrow::Result<Value> NodeArena::apply_nested_path_update_versioned(
    const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
    const FieldLayout& fl, const std::vector<std::string>& nested_path,
    const Value& value) {
  if (nested_path.empty()) {
    return arrow::Status::Invalid(
        "nested_path update requires at least one path segment");
  }
  if (nested_path.size() > 1) {
    return arrow::Status::NotImplemented(
        "nested_path update depth > 1 is not implemented yet");
  }
  const std::string& key = nested_path.front();
  if (!is_map_type(fl.type)) {
    return arrow::Status::TypeError("nested_path update on non-map field: ",
                                    tundradb::to_string(fl.type));
  }

  MapRef current;
  if (handle.is_versioned()) {
    auto [found, ptr] =
        get_field_ptr_from_version_chain(handle.version_info_, fl.index);
    if (found && ptr) {
      current = *reinterpret_cast<const MapRef*>(ptr);
    } else if (!found) {
      const char* base_ptr =
          layout->get_value_ptr(static_cast<const char*>(handle.ptr), fl.index);
      if (base_ptr) {
        current = *reinterpret_cast<const MapRef*>(base_ptr);
      }
    }
  }

  MapRef copy;
  if (current.is_null()) {
    ARROW_ASSIGN_OR_RAISE(copy, map_arena_->allocate());
  } else {
    ARROW_ASSIGN_OR_RAISE(copy, map_arena_->copy(current));
  }

  ARROW_RETURN_NOT_OK(set_nested_map_key(copy, key, value));
  return Value{std::move(copy)};
}

arrow::Status NodeArena::append_to_array_field(
    void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
    const FieldLayout* field_layout, const Value& value) {
  if (!is_array_type(field_layout->type)) {
    return arrow::Status::TypeError(
        "APPEND is only valid for array fields, got: ",
        tundradb::to_string(field_layout->type));
  }

  auto* base = static_cast<char*>(node_ptr);
  const bool field_is_set = is_field_set(base, field_layout->index);

  ArrayRef current_ref;
  if (field_is_set) {
    Value old_value = layout->get_value(base, *field_layout);
    if (!old_value.is_null() && old_value.holds_array_ref()) {
      current_ref = old_value.as_array_ref();
    }
  }

  if (value.holds_raw_array()) {
    const auto& elems = value.as_raw_array();
    if (elems.empty()) return arrow::Status::OK();

    ArrayRef new_ref;
    if (current_ref.is_null()) {
      ARROW_ASSIGN_OR_RAISE(new_ref,
                            store_raw_array(field_layout->type_desc, elems));
    } else {
      const auto n = static_cast<uint32_t>(elems.size());
      ARROW_ASSIGN_OR_RAISE(
          new_ref,
          array_arena_->copy(current_ref, grow_for_append(current_ref, n)));
      for (const auto& elem : elems) {
        ARROW_RETURN_NOT_OK(
            append_single_element(new_ref, field_layout->type_desc, elem));
      }
      array_arena_->mark_for_deletion(current_ref);
    }

    if (!layout->set_field_value(base, *field_layout,
                                 Value{std::move(new_ref)})) {
      return arrow::Status::Invalid("Failed to write array field after APPEND");
    }
    return arrow::Status::OK();
  }

  if (current_ref.is_null()) {
    const std::vector<Value> elems = {value};
    ARROW_ASSIGN_OR_RAISE(ArrayRef new_ref,
                          store_raw_array(field_layout->type_desc, elems));
    if (!layout->set_field_value(base, *field_layout,
                                 Value{std::move(new_ref)})) {
      return arrow::Status::Invalid("Failed to write array field after APPEND");
    }
    return arrow::Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(
      ArrayRef new_ref,
      array_arena_->copy(current_ref, grow_for_append(current_ref, 1)));
  ARROW_RETURN_NOT_OK(
      append_single_element(new_ref, field_layout->type_desc, value));
  array_arena_->mark_for_deletion(current_ref);

  if (!layout->set_field_value(base, *field_layout,
                               Value{std::move(new_ref)})) {
    return arrow::Status::Invalid("Failed to write array field after APPEND");
  }
  return arrow::Status::OK();
}

uint32_t NodeArena::grow_for_append(const ArrayRef& ref, uint32_t n) {
  const uint32_t spare = ref.capacity() - ref.length();
  if (spare >= n) return 0;
  return n - spare;
}

arrow::Status NodeArena::append_single_element(ArrayRef& ref,
                                               const TypeDescriptor& type_desc,
                                               const Value& elem) {
  switch (type_desc.element_type) {
    case ValueType::INT32: {
      int32_t v = elem.as_int32();
      return array_arena_->append(ref, &v);
    }
    case ValueType::INT64: {
      int64_t v = elem.as_int64();
      return array_arena_->append(ref, &v);
    }
    case ValueType::DOUBLE: {
      double v = elem.as_double();
      return array_arena_->append(ref, &v);
    }
    case ValueType::BOOL: {
      bool v = elem.as_bool();
      return array_arena_->append(ref, &v);
    }
    case ValueType::STRING: {
      ARROW_ASSIGN_OR_RAISE(StringRef sr,
                            string_arena_->store_string_auto(elem.as_string()));
      return array_arena_->append(ref, &sr);
    }
    default:
      return arrow::Status::NotImplemented(
          "APPEND: unsupported element type: ",
          tundradb::to_string(type_desc.element_type));
  }
}

std::pair<bool, const char*> NodeArena::get_field_ptr_from_version_chain(
    const VersionInfo* version_info, uint16_t field_idx) {
  const VersionInfo* current = version_info;
  while (current != nullptr) {
    if (auto it = current->updated_fields.find(field_idx);
        it != current->updated_fields.end()) {
      return {true, it->second};
    }
    current = current->prev;
  }
  return {false, nullptr};
}

bool NodeArena::write_value_to_memory(char* ptr, ValueType type,
                                      const Value& value) {
  switch (type) {
    case ValueType::INT64:
      if (value.type() != ValueType::INT64) return false;
      *reinterpret_cast<int64_t*>(ptr) = value.as_int64();
      return true;

    case ValueType::INT32:
      if (value.type() != ValueType::INT32) return false;
      *reinterpret_cast<int32_t*>(ptr) = value.as_int32();
      return true;

    case ValueType::DOUBLE:
      if (value.type() != ValueType::DOUBLE) return false;
      *reinterpret_cast<double*>(ptr) = value.as_double();
      return true;

    case ValueType::BOOL:
      if (value.type() != ValueType::BOOL) return false;
      *reinterpret_cast<bool*>(ptr) = value.as_bool();
      return true;

    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      if (!is_string_type(value.type())) return false;
      *reinterpret_cast<StringRef*>(ptr) = value.as_string_ref();
      return true;

    case ValueType::ARRAY:
      if (value.type() != ValueType::ARRAY) return false;
      *reinterpret_cast<ArrayRef*>(ptr) = value.as_array_ref();
      return true;

    case ValueType::MAP:
      if (value.type() != ValueType::MAP) return false;
      *reinterpret_cast<MapRef*>(ptr) = value.as_map_ref();
      return true;

    default:
      return false;
  }
}

arrow::Result<ArrayRef> NodeArena::store_raw_array(
    const TypeDescriptor& type_desc, const std::vector<Value>& elements) {
  const ValueType elem_type = type_desc.element_type;
  const auto count = static_cast<uint32_t>(elements.size());

  uint32_t capacity = count;
  if (type_desc.is_fixed_size_array() && type_desc.fixed_size > count) {
    capacity = type_desc.fixed_size;
  }

  ARROW_ASSIGN_OR_RAISE(ArrayRef ref,
                        array_arena_->allocate(elem_type, capacity));

  if (ref.is_null()) {
    return ref;
  }

  const size_t elem_sz = get_type_size(elem_type);
  auto* header = reinterpret_cast<ArrayRef::ArrayHeader*>(
      ref.data() - ArrayRef::HEADER_SIZE);

  for (uint32_t i = 0; i < count; ++i) {
    char* dest = ref.mutable_element_ptr(i);
    const Value& elem = elements[i];

    if (is_string_type(elem_type) && elem.holds_std_string()) {
      ARROW_ASSIGN_OR_RAISE(StringRef str_ref,
                            string_arena_->store_string_auto(elem.as_string()));
      *reinterpret_cast<StringRef*>(dest) = std::move(str_ref);
    } else {
      write_value_to_memory(dest, elem_type, elem);
    }
  }

  header->length = count;
  return ref;
}

arrow::Result<MapRef> NodeArena::store_raw_map(
    const std::map<std::string, Value>& entries) {
  ARROW_ASSIGN_OR_RAISE(
      MapRef ref, map_arena_->allocate(static_cast<uint32_t>(entries.size())));
  for (const auto& [key, val] : entries) {
    ARROW_RETURN_NOT_OK(set_nested_map_key(ref, key, val));
  }
  return ref;
}

}  // namespace tundradb
