#ifndef SCHEMA_LAYOUT_HPP
#define SCHEMA_LAYOUT_HPP

#include <arrow/api.h>

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mem_utils.hpp"
#include "schema.hpp"
#include "types.hpp"

namespace tundradb {

/**
 * Helper functions for bit set manipulation to track which fields are set
 */
inline size_t get_bitset_size_bytes(const size_t num_fields) {
  return (num_fields + 7) / 8;  // Round up to nearest byte
}

inline bool is_field_set(const char* bitset, const size_t field_index) {
  const size_t byte_index = field_index / 8;
  const size_t bit_index = field_index % 8;
  return (bitset[byte_index] & (1 << bit_index)) != 0;
}

inline void set_field_bit(char* bitset, const size_t field_index,
                          const bool is_set) {
  const size_t byte_index = field_index / 8;
  const size_t bit_index = field_index % 8;
  if (is_set) {
    bitset[byte_index] |= (1 << bit_index);
  } else {
    bitset[byte_index] &= ~(1 << bit_index);
  }
}

/**
 * Describes the layout of a single field within a schema
 */
struct FieldLayout {
  std::string name;
  ValueType type;
  size_t offset;     // Byte offset from start of node data
  size_t size;       // Size in bytes
  size_t alignment;  // Required alignment
  bool nullable;     // Whether field can be null

  FieldLayout(std::string field_name, const ValueType field_type,
              const size_t field_offset, const size_t field_size,
              const size_t field_alignment, const bool is_nullable = true)
      : name(std::move(field_name)),
        type(field_type),
        offset(field_offset),
        size(field_size),
        alignment(field_alignment),
        nullable(is_nullable) {}
};

/**
 * Schema layout for fixed-size data
 * All strings stored as StringRef (12 bytes), actual content in StringArena
 */
class SchemaLayout {
 public:
  explicit SchemaLayout(std::string schema_name)
      : schema_name_(std::move(schema_name)), total_size_(0), alignment_(8) {}

  /**
   * Get the size of the bit set in bytes
   */
  size_t get_bitset_size() const {
    return get_bitset_size_bytes(fields_.size());
  }

  /**
   * Get the offset where actual field data starts (after bit set + alignment)
   */
  size_t get_data_offset() const {
    const size_t bitset_size = get_bitset_size();
    return align_up(bitset_size, alignment_);
  }

  /**
   * Add a field to the schema layout
   * Fields are automatically aligned and packed efficiently
   */
  void add_field(const std::string& name, ValueType type,
                 bool nullable = true) {
    size_t field_size = get_type_size(type);
    size_t field_alignment = get_type_alignment(type);

    alignment_ = std::max(alignment_, field_alignment);

    // Calculate field offset (relative to start of data, after bit set)
    size_t aligned_offset = align_up(total_size_, field_alignment);

    // Create field layout (offset is relative to data start, not absolute)
    field_index_[name] = fields_.size();
    fields_.emplace_back(name, type, aligned_offset, field_size,
                         field_alignment, nullable);

    // Update total size (size of data portion only)
    total_size_ = aligned_offset + field_size;
  }

  /**
   * Finalize the layout - adds padding to ensure proper alignment
   * Must be called after all fields are added
   */
  void finalize() {
    // Add padding at the end to ensure array alignment
    total_size_ = align_up(total_size_, alignment_);
    finalized_ = true;
  }

  /**
   * Get the total size including bit set and data
   */
  size_t get_total_size_with_bitset() const {
    return get_data_offset() + total_size_;
  }

  /**
   * Get field value from node data
   */
  Value get_field_value(const char* node_data,
                        const std::string& field_name) const {
    const auto it = field_index_.find(field_name);
    if (it == field_index_.end()) {
      return Value();  // null value for missing field
    }

    const size_t field_index = it->second;
    const FieldLayout& field = fields_[field_index];

    // Check if this field has been set using the bit set
    if (!is_field_set(node_data, field_index)) {
      return Value();  // null value for unset field
    }

    // Field has been set, read it from memory
    const char* data_start = node_data + get_data_offset();
    const char* field_ptr = data_start + field.offset;

    return read_value_from_memory(field_ptr, field.type);
  }

  /**
   * Set field value in node data
   */
  bool set_field_value(char* node_data, const std::string& field_name,
                       const Value& value) {
    const auto it = field_index_.find(field_name);
    if (it == field_index_.end()) {
      return false;  // field not found
    }

    const size_t field_index = it->second;
    const FieldLayout& field = fields_[field_index];

    // Update the bit set to indicate this field has been set
    set_field_bit(node_data, field_index, !value.is_null());

    // If the value is null, we don't need to write it to memory
    if (value.is_null()) {
      return true;  // Successfully "set" to null
    }

    // Write the actual value to memory
    char* data_start = node_data + get_data_offset();
    char* field_ptr = data_start + field.offset;

    return write_value_to_memory(field_ptr, field.type, value);
  }

  /**
   * Initialize node data with default values
   */
  void initialize_node_data(char* node_data) const {
    // Clear the bit set (all fields initially unset)
    const size_t bitset_size = get_bitset_size();
    std::memset(node_data, 0, bitset_size);

    // Zero out all data memory
    char* data_start = node_data + get_data_offset();
    std::memset(data_start, 0, total_size_);

    // Set any non-zero default values if needed
    for (const auto& field : fields_) {
      char* field_ptr = data_start + field.offset;
      initialize_field_memory(field_ptr, field.type);
    }
  }

  // Getters
  const std::string& get_schema_name() const { return schema_name_; }
  size_t get_total_size() const { return total_size_; }
  size_t get_alignment() const { return alignment_; }
  bool is_finalized() const { return finalized_; }

  bool has_field(const std::string& name) const {
    return field_index_.contains(name);
  }

  const FieldLayout* get_field_layout(const std::string& name) const {
    const auto it = field_index_.find(name);
    return it != field_index_.end() ? &fields_[it->second] : nullptr;
  }

  const std::vector<FieldLayout>& get_fields() const { return fields_; }

 private:
  static Value read_value_from_memory(const char* ptr, const ValueType type) {
    switch (type) {
      case ValueType::INT64:
        return Value{*reinterpret_cast<const int64_t*>(ptr)};
      case ValueType::INT32:
        return Value{*reinterpret_cast<const int32_t*>(ptr)};
      case ValueType::DOUBLE:
        return Value{*reinterpret_cast<const double*>(ptr)};
      case ValueType::BOOL:
        return Value{*reinterpret_cast<const bool*>(ptr)};
      case ValueType::STRING:
      case ValueType::FIXED_STRING16:
      case ValueType::FIXED_STRING32:
      case ValueType::FIXED_STRING64:
        // All string types stored as StringRef, but preserve the field's
        // declared type
        return Value{*reinterpret_cast<const StringRef*>(ptr), type};
      case ValueType::NA:
      default:
        return Value{};
    }
  }

  static bool write_value_to_memory(char* ptr, const ValueType type,
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
        // All string types expect StringRef
        if (!is_string_type(value.type())) return false;
        // Value should contain StringRef (created by NodeArena)
        *reinterpret_cast<StringRef*>(ptr) = value.as_string_ref();
        return true;
      }
      default:
        return false;
    }
  }

  static void initialize_field_memory(char* ptr, const ValueType type) {
    switch (type) {
      case ValueType::STRING:
      case ValueType::FIXED_STRING16:
      case ValueType::FIXED_STRING32:
      case ValueType::FIXED_STRING64:
        // Initialize StringRef to null/empty
        new (ptr) StringRef();
        break;
      default:
        // Zero initialization is fine for numeric types and bools
        break;
    }
  }

  std::string schema_name_;
  std::vector<FieldLayout> fields_;
  std::unordered_map<std::string, size_t> field_index_;
  size_t total_size_;
  size_t alignment_;
  bool finalized_ = false;
};

/**
 * Registry for managing schema layouts
 */
class LayoutRegistry {
 public:
  /**
   * Register a manually created layout
   */
  void register_layout(std::shared_ptr<SchemaLayout> layout) {
    if (!layout->is_finalized()) {
      layout->finalize();
    }
    layouts_[layout->get_schema_name()] = std::move(layout);
  }

  /**
   * Get layout for a schema, returns nullptr if not found
   */
  std::shared_ptr<SchemaLayout> get_layout(const std::string& schema_name) {
    const auto it = layouts_.find(schema_name);
    return it != layouts_.end() ? it->second : nullptr;
  }

  bool exists(const std::string& schema_name) const {
    return layouts_.contains(schema_name);
  }

  /**
   * Create and register a layout from an Arrow schema
   */
  std::shared_ptr<SchemaLayout> create_layout_from_arrow_schema(
      const std::string& schema_name,
      const std::shared_ptr<arrow::Schema>& arrow_schema) {
    auto layout = std::make_shared<SchemaLayout>(schema_name);

    // Add fields (all strings stored as StringRef)
    for (const auto& field : arrow_schema->fields()) {
      const ValueType value_type = arrow_type_to_value_type(field->type());
      // String types are stored as StringRef in node layout
      layout->add_field(field->name(), value_type, field->nullable());
    }
    layout->finalize();
    layouts_[schema_name] = layout;
    return layout;
  }

  std::shared_ptr<SchemaLayout> create_layout(
      const std::shared_ptr<Schema>& schema) {
    auto layout = std::make_shared<SchemaLayout>(schema->name());

    // Add fields (all strings stored as StringRef)
    for (const auto& field : schema->fields()) {
      // String types are stored as StringRef in node layout
      layout->add_field(field->name(), field->type(), field->nullable());
    }

    layout->finalize();

    layouts_[schema->name()] = layout;
    // Logger::get_instance().debug("created schema layout");
    return layout;
  }

  bool remove_layout(const std::string& schema_name) {
    return layouts_.erase(schema_name) > 0;
  }

  std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(layouts_.size());
    for (const auto& name : layouts_ | std::views::keys) {
      names.push_back(name);
    }
    return names;
  }

  size_t size() const { return layouts_.size(); }
  bool empty() const { return layouts_.empty(); }
  void clear() { layouts_.clear(); }

 private:
  std::unordered_map<std::string, std::shared_ptr<SchemaLayout>> layouts_;

  // Helper function to convert Arrow types to ValueTypes
  static ValueType arrow_type_to_value_type(
      const std::shared_ptr<arrow::DataType>& arrow_type) {
    switch (arrow_type->id()) {
      case arrow::Type::INT32:
        return ValueType::INT32;
      case arrow::Type::INT64:
        return ValueType::INT64;
      case arrow::Type::DOUBLE:
        return ValueType::DOUBLE;
      case arrow::Type::BOOL:
        return ValueType::BOOL;
      case arrow::Type::STRING:
        return ValueType::STRING;  // Will be stored as StringRef
      default:
        return ValueType::NA;
    }
  }
};

}  // namespace tundradb

#endif  // SCHEMA_LAYOUT_HPP