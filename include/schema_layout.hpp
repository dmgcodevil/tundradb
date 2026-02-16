#ifndef SCHEMA_LAYOUT_HPP
#define SCHEMA_LAYOUT_HPP

#include <arrow/api.h>

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "llvm/ADT/StringMap.h"
#include "mem_utils.hpp"
#include "schema.hpp"
#include "types.hpp"

namespace tundradb {

/**
 * Helper functions for bit set manipulation to track which fields are set
 */
inline size_t get_bitset_size_bytes(const size_t num_fields) {
  size_t bit_words = (num_fields + 63) / 64;
  size_t bitset_bytes = bit_words * sizeof(uint64_t);
  return bitset_bytes;
}

inline bool is_field_set(const char* base, const size_t idx) {
  auto words = reinterpret_cast<const uint64_t*>(base);
  return (words[idx >> 6] >> (idx & 63)) & 1ULL;
}

inline void set_field_bit(char* base, size_t idx, bool is_set) {
  auto words = reinterpret_cast<uint64_t*>(base);
  uint64_t mask = 1ULL << (idx & 63);
  uint64_t& w = words[idx >> 6];
  if (is_set) {
    w |= mask;
  } else {
    w &= ~mask;
  }
}

/**
 * Describes the layout of a single field within a schema
 */
struct FieldLayout {
  const size_t index;
  std::string name;
  ValueType type;
  size_t offset;     // Byte offset from start of node data
  size_t size;       // Size in bytes
  size_t alignment;  // Required alignment
  bool nullable;     // Whether field can be null

  FieldLayout(const size_t index, std::string field_name,
              const ValueType field_type, const size_t field_offset,
              const size_t field_size, const size_t field_alignment,
              const bool is_nullable = true)
      : index(index),
        name(std::move(field_name)),
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
  explicit SchemaLayout(const std::shared_ptr<Schema>& schema)
      : schema_name_(std::move(schema->name())), total_size_(0), alignment_(8) {
    fields_.reserve(schema->num_fields());
    for (auto field : schema->fields()) {
      add_field(field);
    }
    finalize();
  }

  /**
   * Get the size of the bit set in bytes
   */
  [[nodiscard]] size_t get_bitset_size() const {
    return get_bitset_size_bytes(fields_.size());
  }

  /**
   * Get the offset where actual field data starts (after bit set + alignment)
   */
  [[nodiscard]] size_t get_data_offset() const { return data_offset_; }

  /**
   * Finalize the layout - adds padding to ensure proper alignment
   * Must be called after all fields are added
   */
  void finalize() {
    // Add padding at the end to ensure array alignment
    total_size_ = align_up(total_size_, alignment_);
    data_offset_ = align_up(get_bitset_size(), alignment_);
    finalized_ = true;
  }

  /**
   * Get the total size including bit set and data
   */
  [[nodiscard]] size_t get_total_size_with_bitset() const {
    return data_offset_ + total_size_;
  }

  const char* get_field_value_ptr(const char* node_data,
                                  const size_t field_index) const {
    const FieldLayout& field_layout = fields_[field_index];
    // Check if this field has been set using the bit set
    if (!is_field_set(node_data, field_layout.index)) {
      return nullptr;  // null value for unset field
    }

    // Field has been set, read it from memory
    const char* data_start = node_data + data_offset_;
    const char* field_ptr = data_start + field_layout.offset;
    return field_ptr;
  }

  // gets a pointer to field value
  const char* get_field_value_ptr(const char* node_data,
                                  const std::shared_ptr<Field>& field) const {
    return get_field_value_ptr(node_data, field->index_);
  }

  Value get_field_value(const char* node_data, const size_t field_index) const {
    const FieldLayout& field_layout = fields_[field_index];
    return Value::read_value_from_memory(
        get_field_value_ptr(node_data, field_index), field_layout.type);
  }

  Value get_field_value(const char* node_data,
                        const FieldLayout& field_layout) const {
    return Value::read_value_from_memory(
        get_field_value_ptr(node_data, field_layout.index), field_layout.type);
  }

  Value get_field_value(const char* node_data,
                        const std::shared_ptr<Field>& field) const {
    return get_field_value(node_data, field->index_);
  }

  /**
   * Get field value directly from field pointer (no address math).
   * Used by versioning when we already have the exact field data pointer.
   *
   * @param field_ptr Direct pointer to field data (from updated_fields)
   * @param field_layout Field layout for type information
   * @return Value read from field_ptr
   */
  Value get_field_value_from_ptr(const char* field_ptr,
                                 const FieldLayout& field_layout) const {
    if (field_ptr == nullptr) {
      return Value{};  // Explicit NULL
    }
    return Value::read_value_from_memory(field_ptr, field_layout.type);
  }

  /**
   * Set field value in node data
   */
  bool set_field_value(char* node_data, const std::shared_ptr<Field>& field,
                       const Value& value) {
    return set_field_value(node_data, fields_[field->index_], value);
  }

  bool set_field_value(char* node_data, const FieldLayout& field_layout,
                       const Value& value) {
    // Update the bit set to indicate this field has been set
    set_field_bit(node_data, field_layout.index, !value.is_null());

    // If the value is null, we don't need to write it to memory just set bit
    if (value.is_null()) {
      return true;
    }

    // Write the actual value to memory
    char* data_start = node_data + data_offset_;
    char* field_ptr = data_start + field_layout.offset;
    return write_value_to_memory(field_ptr, field_layout.type, value);
  }

  /**
   * Initialize node data with default values
   */
  void initialize_node_data(char* node_data) const {
    // Clear the bit set (all fields initially unset)
    const size_t bitset_size = get_bitset_size();
    std::memset(node_data, 0, bitset_size);

    // Zero out all data memory
    char* data_start = node_data + data_offset_;  // get_data_offset();
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
  const std::vector<FieldLayout>& get_fields() const { return fields_; }

  const FieldLayout* get_field_layout(
      const std::shared_ptr<Field>& field) const {
    if (!field) {
      // log_error("get_field_layout: field is null");
      return nullptr;
    }
    if (field->index_ >= fields_.size()) {
      // log_error("get_field_layout: field index {} >= fields size {}",
      //           field->index_, fields_.size());
      return nullptr;
    }
    return &fields_[field->index_];
  }

 private:
  /**
   * Add a field to the schema layout
   * Fields are automatically aligned and packed efficiently
   */
  void add_field(const std::shared_ptr<Field>& field) {
    assert(field != nullptr);
    size_t field_size = get_type_size(field->type());
    size_t field_alignment = get_type_alignment(field->type());

    alignment_ = std::max(alignment_, field_alignment);

    // Calculate field offset (relative to start of data, after bit set)
    size_t aligned_offset = align_up(total_size_, field_alignment);
    field->index_ = fields_.size();
    fields_.emplace_back(field->index_, field->name(), field->type(),
                         aligned_offset, field_size, field_alignment,
                         field->nullable());

    // Update total size (size of data portion only)
    total_size_ = aligned_offset + field_size;
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
  size_t total_size_;
  size_t alignment_;
  bool finalized_ = false;
  size_t data_offset_;
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

  std::shared_ptr<SchemaLayout> create_layout(
      const std::shared_ptr<Schema>& schema) {
    auto layout = std::make_shared<SchemaLayout>(schema);
    layouts_[schema->name()] = layout;
    // Logger::get_instance().debug("created schema layout");
    return layout;
  }

  bool remove_layout(const std::string& schema_name) {
    return layouts_.erase(schema_name) > 0;
  }

  [[nodiscard]] std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(layouts_.size());
    for (auto const& entry : layouts_) {
      names.push_back(entry.first().str());
    }
    return names;
  }

  size_t size() const { return layouts_.size(); }
  bool empty() const { return layouts_.empty(); }
  void clear() { layouts_.clear(); }

 private:
  llvm::StringMap<std::shared_ptr<SchemaLayout>> layouts_;
};

}  // namespace tundradb

#endif  // SCHEMA_LAYOUT_HPP