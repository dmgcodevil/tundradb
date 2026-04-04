#ifndef SCHEMA_LAYOUT_HPP
#define SCHEMA_LAYOUT_HPP

#include <arrow/api.h>

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "llvm/ADT/StringMap.h"
#include "memory/array_ref.hpp"
#include "memory/map_ref.hpp"
#include "memory/mem_utils.hpp"
#include "schema/schema.hpp"
#include "schema/type_descriptor.hpp"

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
  ValueType type;            // base type for fast dispatch
  TypeDescriptor type_desc;  // full type descriptor (carries array params etc.)
  size_t offset;             // Byte offset from start of node data
  size_t size;               // Size in bytes
  size_t alignment;          // Required alignment
  bool nullable;             // Whether field can be null

  FieldLayout(const size_t index, std::string field_name,
              const ValueType field_type, const TypeDescriptor& field_type_desc,
              const size_t field_offset, const size_t field_size,
              const size_t field_alignment, const bool is_nullable = true)
      : index(index),
        name(std::move(field_name)),
        type(field_type),
        type_desc(field_type_desc),
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
  explicit SchemaLayout(const std::shared_ptr<Schema>& schema);

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

  /// Pointer to field storage, or nullptr if the field bit is unset.
  const char* get_value_ptr(const char* node_data,
                            const size_t field_index) const;

  /// Pointer to field storage for \p field, or nullptr if unset.
  const char* get_value_ptr(const char* node_data,
                            const std::shared_ptr<Field>& field) const {
    return get_value_ptr(node_data, field->index_);
  }

  /// Reads the field at \p field_index from node memory (unset reads as
  /// default/null).
  Value get_value(const char* node_data, const size_t field_index) const {
    const FieldLayout& field_layout = fields_[field_index];
    return Value::read_value_from_memory(get_value_ptr(node_data, field_index),
                                         field_layout.type);
  }

  /// Reads \p field_layout from node memory using its index and type.
  Value get_value(const char* node_data,
                  const FieldLayout& field_layout) const {
    return Value::read_value_from_memory(
        get_value_ptr(node_data, field_layout.index), field_layout.type);
  }

  /// Reads the field identified by \p field from node memory.
  Value get_value(const char* node_data,
                  const std::shared_ptr<Field>& field) const {
    return get_value(node_data, field->index_);
  }

  /**
   * Get field value directly from field pointer (no address math).
   * Used by versioning when we already have the exact field data pointer.
   *
   * @param field_ptr Direct pointer to field data (from updated_fields)
   * @param field_layout Field layout for type information
   * @return Value read from field_ptr
   */
  Value get_value_from_ptr(const char* field_ptr,
                           const FieldLayout& field_layout) const;

  /**
   * Set field value in node data
   */
  bool set_field_value(char* node_data, const std::shared_ptr<Field>& field,
                       const Value& value) {
    return set_field_value(node_data, fields_[field->index_], value);
  }

  bool set_field_value(char* node_data, const FieldLayout& field_layout,
                       const Value& value);

  /**
   * Initialize node data with default values
   */
  void initialize_node_data(char* node_data) const;

  // Getters
  /// Schema name this layout was built for.
  const std::string& get_schema_name() const { return schema_name_; }
  /// Size of the data region after the bitset (excludes bitset and padding to
  /// alignment).
  size_t get_total_size() const { return total_size_; }
  /// Maximum alignment required by any field in this layout.
  size_t get_alignment() const { return alignment_; }
  /// True after finalize() (or implicit finalize from construction).
  bool is_finalized() const { return finalized_; }
  /// Ordered field layouts (indices match schema field order).
  const std::vector<FieldLayout>& get_fields() const { return fields_; }

  /// Layout entry for \p field, or nullptr if index is out of range or field is
  /// null.
  const FieldLayout* get_field_layout(
      const std::shared_ptr<Field>& field) const;

 private:
  /**
   * Add a field to the schema layout
   * Fields are automatically aligned and packed efficiently
   */
  void add_field(const std::shared_ptr<Field>& field);

  static bool write_value_to_memory(char* ptr, const ValueType type,
                                    const Value& value);

  static void initialize_field_memory(char* ptr, const ValueType type);

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
  void register_layout(std::shared_ptr<SchemaLayout> layout);

  /**
   * Get layout for a schema, returns nullptr if not found
   */
  std::shared_ptr<SchemaLayout> get_layout(const std::string& schema_name);

  /// True if a layout is registered for \p schema_name.
  bool exists(const std::string& schema_name) const {
    return layouts_.contains(schema_name);
  }

  /// Builds a SchemaLayout from \p schema and stores it under the schema name.
  std::shared_ptr<SchemaLayout> create_layout(
      const std::shared_ptr<Schema>& schema) {
    auto layout = std::make_shared<SchemaLayout>(schema);
    layouts_[schema->name()] = layout;
    return layout;
  }

  /// Erases the layout for \p schema_name; returns whether an entry existed.
  bool remove_layout(const std::string& schema_name) {
    return layouts_.erase(schema_name) > 0;
  }

  /// All registered schema names (order unspecified).
  [[nodiscard]] std::vector<std::string> get_schema_names() const;

  /// Number of registered layouts.
  size_t size() const { return layouts_.size(); }
  /// True if no layouts are registered.
  bool empty() const { return layouts_.empty(); }
  /// Removes all layouts from the registry.
  void clear() { layouts_.clear(); }

 private:
  llvm::StringMap<std::shared_ptr<SchemaLayout>> layouts_;
};

}  // namespace tundradb

#endif  // SCHEMA_LAYOUT_HPP
