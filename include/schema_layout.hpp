#ifndef SCHEMA_LAYOUT_HPP
#define SCHEMA_LAYOUT_HPP

#include <arrow/api.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mem_utils.hpp"
#include "types.hpp"

namespace tundradb {

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

  FieldLayout(std::string field_name, ValueType field_type, size_t field_offset,
              size_t field_size, size_t field_alignment,
              bool is_nullable = true)
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
   * Add a field to the schema layout
   * Fields are automatically aligned and packed efficiently
   */
  void add_field(const std::string& name, ValueType type,
                 bool nullable = true) {
    size_t field_size = get_type_size(type);
    size_t field_alignment = get_type_alignment(type);

    // Update overall alignment requirement
    alignment_ = std::max(alignment_, field_alignment);

    // Align current position for this field
    size_t aligned_offset = align_up(total_size_, field_alignment);

    // Create field layout
    field_index_[name] = fields_.size();
    fields_.emplace_back(name, type, aligned_offset, field_size,
                         field_alignment, nullable);

    // Update total size
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
   * Get field value from node data
   */
  Value get_field_value(const char* node_data,
                        const std::string& field_name) const {
    auto it = field_index_.find(field_name);
    if (it == field_index_.end()) {
      return Value();  // null value for missing field
    }

    const FieldLayout& field = fields_[it->second];
    const char* field_ptr = node_data + field.offset;

    return read_value_from_memory(field_ptr, field.type);
  }

  /**
   * Set field value in node data
   */
  bool set_field_value(char* node_data, const std::string& field_name,
                       const Value& value) {
    auto it = field_index_.find(field_name);
    if (it == field_index_.end()) {
      return false;  // field not found
    }

    const FieldLayout& field = fields_[it->second];
    char* field_ptr = node_data + field.offset;

    return write_value_to_memory(field_ptr, field.type, value);
  }

  /**
   * Initialize node data with default values
   */
  void initialize_node_data(char* node_data) const {
    // Zero out all memory first
    std::memset(node_data, 0, total_size_);

    // Set any non-zero default values if needed
    for (const auto& field : fields_) {
      char* field_ptr = node_data + field.offset;
      initialize_field_memory(field_ptr, field.type);
    }
  }

  // Getters
  const std::string& get_schema_name() const { return schema_name_; }
  size_t get_total_size() const { return total_size_; }
  size_t get_alignment() const { return alignment_; }
  bool is_finalized() const { return finalized_; }

  bool has_field(const std::string& name) const {
    return field_index_.find(name) != field_index_.end();
  }

  const FieldLayout* get_field_layout(const std::string& name) const {
    auto it = field_index_.find(name);
    return it != field_index_.end() ? &fields_[it->second] : nullptr;
  }

  const std::vector<FieldLayout>& get_fields() const { return fields_; }

 private:
  Value read_value_from_memory(const char* ptr, ValueType type) const {
    switch (type) {
      case ValueType::Int64:
        return Value{*reinterpret_cast<const int64_t*>(ptr)};
      case ValueType::Int32:
        return Value{*reinterpret_cast<const int32_t*>(ptr)};
      case ValueType::Double:
        return Value{*reinterpret_cast<const double*>(ptr)};
      case ValueType::Bool:
        return Value{*reinterpret_cast<const bool*>(ptr)};
      case ValueType::String:
      case ValueType::FixedString16:
      case ValueType::FixedString32:
      case ValueType::FixedString64:
        // All string types stored as StringRef, but preserve the field's
        // declared type
        return Value{*reinterpret_cast<const StringRef*>(ptr), type};
      case ValueType::Null:
      default:
        return Value{};
    }
  }

  bool write_value_to_memory(char* ptr, ValueType type, const Value& value) {
    switch (type) {
      case ValueType::Int64:
        if (value.type() != ValueType::Int64) return false;
        *reinterpret_cast<int64_t*>(ptr) = value.as_int64();
        return true;
      case ValueType::Int32:
        if (value.type() != ValueType::Int32) return false;
        *reinterpret_cast<int32_t*>(ptr) = value.as_int32();
        return true;
      case ValueType::Double:
        if (value.type() != ValueType::Double) return false;
        *reinterpret_cast<double*>(ptr) = value.as_double();
        return true;
      case ValueType::Bool:
        if (value.type() != ValueType::Bool) return false;
        *reinterpret_cast<bool*>(ptr) = value.as_bool();
        return true;
      case ValueType::String:
      case ValueType::FixedString16:
      case ValueType::FixedString32:
      case ValueType::FixedString64: {
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

  void initialize_field_memory(char* ptr, ValueType type) const {
    switch (type) {
      case ValueType::String:
      case ValueType::FixedString16:
      case ValueType::FixedString32:
      case ValueType::FixedString64:
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
  void register_layout(std::unique_ptr<SchemaLayout> layout) {
    if (!layout->is_finalized()) {
      layout->finalize();
    }
    layouts_[layout->get_schema_name()] = std::move(layout);
  }

  /**
   * Get layout for a schema, returns nullptr if not found
   */
  SchemaLayout* get_layout(const std::string& schema_name) {
    auto it = layouts_.find(schema_name);
    return it != layouts_.end() ? it->second.get() : nullptr;
  }

  const SchemaLayout* get_layout(const std::string& schema_name) const {
    auto it = layouts_.find(schema_name);
    return it != layouts_.end() ? it->second.get() : nullptr;
  }

  /**
   * Create and register a layout from an Arrow schema
   */
  SchemaLayout* create_layout_from_arrow_schema(
      const std::string& schema_name,
      const std::shared_ptr<arrow::Schema>& arrow_schema) {
    auto layout = std::make_unique<SchemaLayout>(schema_name);

    // Add fields (all strings stored as StringRef)
    for (const auto& field : arrow_schema->fields()) {
      ValueType value_type = arrow_type_to_value_type(field->type());
      // String types are stored as StringRef in node layout
      layout->add_field(field->name(), value_type, field->nullable());
    }

    layout->finalize();

    SchemaLayout* result = layout.get();
    layouts_[schema_name] = std::move(layout);
    return result;
  }

  bool remove_layout(const std::string& schema_name) {
    return layouts_.erase(schema_name) > 0;
  }

  std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(layouts_.size());
    for (const auto& [name, layout] : layouts_) {
      names.push_back(name);
    }
    return names;
  }

  size_t size() const { return layouts_.size(); }
  bool empty() const { return layouts_.empty(); }
  void clear() { layouts_.clear(); }

 private:
  std::unordered_map<std::string, std::unique_ptr<SchemaLayout>> layouts_;

  // Helper function to convert Arrow types to ValueTypes
  ValueType arrow_type_to_value_type(
      const std::shared_ptr<arrow::DataType>& arrow_type) const {
    switch (arrow_type->id()) {
      case arrow::Type::INT32:
        return ValueType::Int32;
      case arrow::Type::INT64:
        return ValueType::Int64;
      case arrow::Type::DOUBLE:
        return ValueType::Double;
      case arrow::Type::BOOL:
        return ValueType::Bool;
      case arrow::Type::STRING:
        return ValueType::String;  // Will be stored as StringRef
      default:
        return ValueType::Null;
    }
  }
};

}  // namespace tundradb

#endif  // SCHEMA_LAYOUT_HPP