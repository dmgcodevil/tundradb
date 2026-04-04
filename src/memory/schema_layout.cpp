#include "memory/schema_layout.hpp"

namespace tundradb {

// ============================================================================
// SchemaLayout
// ============================================================================

SchemaLayout::SchemaLayout(const std::shared_ptr<Schema>& schema)
    : schema_name_(std::move(schema->name())), total_size_(0), alignment_(8) {
  fields_.reserve(schema->num_fields());
  for (auto field : schema->fields()) {
    add_field(field);
  }
  finalize();
}

const char* SchemaLayout::get_value_ptr(const char* node_data,
                                        const size_t field_index) const {
  const FieldLayout& field_layout = fields_[field_index];
  if (!is_field_set(node_data, field_layout.index)) {
    return nullptr;
  }

  const char* data_start = node_data + data_offset_;
  const char* field_ptr = data_start + field_layout.offset;
  return field_ptr;
}

Value SchemaLayout::get_value_from_ptr(const char* field_ptr,
                                       const FieldLayout& field_layout) const {
  if (field_ptr == nullptr) {
    return Value{};
  }
  return Value::read_value_from_memory(field_ptr, field_layout.type);
}

bool SchemaLayout::set_field_value(char* node_data,
                                   const FieldLayout& field_layout,
                                   const Value& value) {
  set_field_bit(node_data, field_layout.index, !value.is_null());

  if (value.is_null()) {
    return true;
  }

  char* data_start = node_data + data_offset_;
  char* field_ptr = data_start + field_layout.offset;
  return write_value_to_memory(field_ptr, field_layout.type, value);
}

void SchemaLayout::initialize_node_data(char* node_data) const {
  const size_t bitset_size = get_bitset_size();
  std::memset(node_data, 0, bitset_size);

  char* data_start = node_data + data_offset_;
  std::memset(data_start, 0, total_size_);

  for (const auto& field : fields_) {
    char* field_ptr = data_start + field.offset;
    initialize_field_memory(field_ptr, field.type);
  }
}

const FieldLayout* SchemaLayout::get_field_layout(
    const std::shared_ptr<Field>& field) const {
  if (!field) {
    return nullptr;
  }
  if (field->index_ >= fields_.size()) {
    return nullptr;
  }
  return &fields_[field->index_];
}

void SchemaLayout::add_field(const std::shared_ptr<Field>& field) {
  assert(field != nullptr);
  const auto& td = field->type_descriptor();
  size_t field_size = td.storage_size();
  size_t field_alignment = td.storage_alignment();

  alignment_ = std::max(alignment_, field_alignment);

  size_t aligned_offset = align_up(total_size_, field_alignment);
  field->index_ = fields_.size();
  fields_.emplace_back(field->index_, field->name(), field->type(), td,
                       aligned_offset, field_size, field_alignment,
                       field->nullable());

  total_size_ = aligned_offset + field_size;
}

bool SchemaLayout::write_value_to_memory(char* ptr, const ValueType type,
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
    case ValueType::FIXED_STRING64: {
      if (!is_string_type(value.type())) return false;
      *reinterpret_cast<StringRef*>(ptr) = value.as_string_ref();
      return true;
    }
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

void SchemaLayout::initialize_field_memory(char* ptr, const ValueType type) {
  switch (type) {
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      new (ptr) StringRef();
      break;
    case ValueType::ARRAY:
      new (ptr) ArrayRef();
      break;
    case ValueType::MAP:
      new (ptr) MapRef();
      break;
    default:
      break;
  }
}

// ============================================================================
// LayoutRegistry
// ============================================================================

void LayoutRegistry::register_layout(std::shared_ptr<SchemaLayout> layout) {
  if (!layout->is_finalized()) {
    layout->finalize();
  }
  layouts_[layout->get_schema_name()] = std::move(layout);
}

std::shared_ptr<SchemaLayout> LayoutRegistry::get_layout(
    const std::string& schema_name) {
  const auto it = layouts_.find(schema_name);
  return it != layouts_.end() ? it->second : nullptr;
}

std::vector<std::string> LayoutRegistry::get_schema_names() const {
  std::vector<std::string> names;
  names.reserve(layouts_.size());
  for (auto const& entry : layouts_) {
    names.push_back(entry.first().str());
  }
  return names;
}

}  // namespace tundradb
