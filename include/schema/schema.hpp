#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include <arrow/api.h>
#include <llvm/ADT/StringMap.h>

#include <iostream>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logger.hpp"
#include "common/types.hpp"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"
#include "schema/type_descriptor.hpp"

namespace tundradb {

/// Named column in a schema: identifier, full type (including arrays/maps), and
/// nullability.
struct Field {
 private:
  uint32_t index_;
  std::string name_;
  TypeDescriptor type_desc_;
  bool nullable_ = true;

  friend struct SchemaLayout;
  friend class SchemaLayout;

 public:
  /// Construct from a TypeDescriptor (preferred)
  Field(std::string name, TypeDescriptor type_desc, bool nullable = true)
      : index_(0),
        name_(std::move(name)),
        type_desc_(type_desc),
        nullable_(nullable) {}

  /// Legacy constructor from ValueType (backwards-compatible)
  Field(std::string name, const ValueType type, bool nullable = true)
      : index_(0),
        name_(std::move(name)),
        type_desc_(TypeDescriptor::from_value_type(type)),
        nullable_(nullable) {}

  /// Column name as stored in the schema and field map.
  [[nodiscard]] const std::string &name() const { return name_; }

  /// Returns the base ValueType for switch-based dispatch.
  [[nodiscard]] ValueType type() const { return type_desc_.base_type; }

  /// Returns the full TypeDescriptor (for parameterized types like ARRAY).
  [[nodiscard]] const TypeDescriptor &type_descriptor() const {
    return type_desc_;
  }

  /// Whether values in this column may be absent (SQL NULL / Arrow null).
  [[nodiscard]] bool nullable() const { return nullable_; }

  /**
   * @brief Convert an Arrow field to Field
   *
   * @param field The Arrow field to convert
   * @return arrow::Result<Field> The resulting field
   */
  static arrow::Result<Field> from_arrow(
      const std::shared_ptr<arrow::Field> &field);

  /// Builds the equivalent Arrow field (name, type, nullability); fails if the
  /// type cannot be mapped.
  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Field>> to_arrow() const;
};

/// Versioned collection of columns with a matching Arrow schema and fast lookup
/// by name.
struct Schema {
 private:
  std::string name_;
  uint32_t version_;
  llvm::SmallVector<std::shared_ptr<Field>, 4> fields_;
  size_t num_fields_;
  llvm::StringMap<std::shared_ptr<Field>> field_map_;
  std::shared_ptr<arrow::Schema> arrow_schema_;

 public:
  /// Builds a schema with the given fields and a caller-supplied matching Arrow
  /// schema.
  Schema(std::string name, uint32_t version,
         llvm::SmallVector<std::shared_ptr<Field>, 4> fields,
         std::shared_ptr<arrow::Schema> arrow_schema)
      : name_(std::move(name)),
        version_(version),
        fields_(std::move(fields)),
        arrow_schema_(std::move(arrow_schema)) {
    num_fields_ = fields_.size();
    for (auto const &field : fields_) {
      field_map_.try_emplace(field->name(), field);
    }
  }

  /// Same as the four-argument constructor, but derives the Arrow schema from
  /// the fields.
  Schema(std::string name, uint32_t version,
         llvm::SmallVector<std::shared_ptr<Field>, 4> fields)
      : Schema(name, version, fields, crete_arrow_schema(fields)) {}

  /// Lookup table from field name to shared Field (same entries as fields(),
  /// different index).
  const llvm::StringMap<std::shared_ptr<Field>> &fields_map() {
    return field_map_;
  }

  /// Creates an Arrow schema by converting each Field to an Arrow field (see
  /// Field::to_arrow).
  static std::shared_ptr<arrow::Schema> crete_arrow_schema(
      const llvm::SmallVector<std::shared_ptr<Field>, 4> &fields);

  /// Converts a Tundra schema to its Arrow representation (one shared Arrow
  /// schema object).
  static std::shared_ptr<arrow::Schema> to_arrow(
      const std::shared_ptr<Schema> &schema);

  /**
   * @brief Convert an Arrow schema to Schema
   *
   * @param schema_name shema name
   * @param schema The Arrow schema to convert
   * @return arrow::Result<Schema> The resulting schema
   */
  static arrow::Result<std::shared_ptr<Schema>> from_arrow(
      const std::string &schema_name,
      const std::shared_ptr<arrow::Schema> &schema);

  /// Fields in column order (indices match field(int)).
  [[nodiscard]] const llvm::SmallVector<std::shared_ptr<Field>, 4> &fields()
      const;
  /// Logical schema name (e.g. table or graph label set identifier).
  [[nodiscard]] const std::string &name() const;
  /// Monotonic schema revision for evolution / compatibility checks.
  [[nodiscard]] uint32_t version() const;
  /// Number of columns; same as fields().size().
  [[nodiscard]] size_t num_fields() const;
  /// Field at column index i; index must be valid (same range as num_fields()).
  [[nodiscard]] std::shared_ptr<Field> field(int i) const;
  /// Field by name, or nullptr if no column with that name exists.
  [[nodiscard]] std::shared_ptr<Field> get_field(const std::string &name) const;
  /// True when there are no columns.
  [[nodiscard]] bool empty() const;
  /// Cached Arrow schema kept in sync with this schema’s fields.
  [[nodiscard]] std::shared_ptr<arrow::Schema> arrow() const;
};

/// Named catalog of schemas: each entry keeps both the Arrow schema and the
/// parsed Tundra Schema.
class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<Schema>> schemas_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>>
      arrow_schemas_;

 public:
  SchemaRegistry() = default;

  /// Registers under name after prepending a synthetic id column to the Arrow
  /// schema, then add_arrow.
  arrow::Result<bool> create(const std::string &name,
                             const std::shared_ptr<arrow::Schema> &schema);

  /// Stores the Arrow schema and builds the Tundra Schema from it; fails if
  /// conversion fails.
  arrow::Result<bool> add_arrow(const std::string &name,
                                std::shared_ptr<arrow::Schema> schema);

  /// Arrow schema for name, or not-found if never registered.
  arrow::Result<std::shared_ptr<arrow::Schema>> get_arrow(
      const std::string &name) const;

  /// Tundra Schema for name, or not-found if absent.
  [[nodiscard]] arrow::Result<std::shared_ptr<Schema>> get(
      const std::string &name) const;

  /// True if a schema was successfully registered under name (native Schema
  /// map).
  [[nodiscard]] bool exists(const std::string &name) const;

  /// Names of all registered schemas (keys of the native Schema map).
  [[nodiscard]] std::vector<std::string> get_schema_names() const;
};
}  // namespace tundradb

#endif  // SCHEMA_HPP
