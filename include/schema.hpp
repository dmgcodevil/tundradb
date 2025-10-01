#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include <arrow/api.h>
#include <llvm/ADT/StringMap.h>

#include <iostream>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"
#include "logger.hpp"
#include "types.hpp"

namespace tundradb {

struct Field {
 private:
  uint32_t index_;
  std::string name_;
  ValueType type_;
  bool nullable_ = true;

  friend struct SchemaLayout;

 public:
  Field(std::string name, const ValueType type, bool nullable = true)
      : index_(0), name_(std::move(name)), type_(type), nullable_(nullable) {}

  [[nodiscard]] const std::string &name() const { return name_; }

  [[nodiscard]] const ValueType &type() const { return type_; }

  [[nodiscard]] bool nullable() const { return nullable_; }

  /**
   * @brief Convert an Arrow field to Field
   *
   * @param field The Arrow field to convert
   * @return arrow::Result<Field> The resulting field
   */
  static arrow::Result<Field> from_arrow(
      const std::shared_ptr<arrow::Field> &field);

  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Field>> to_arrow() const;
};

struct Schema {
 private:
  std::string name_;
  uint32_t version_;
  llvm::SmallVector<std::shared_ptr<Field>, 4> fields_;
  size_t num_fields_;
  llvm::StringMap<std::shared_ptr<Field>> field_map_;
  std::shared_ptr<arrow::Schema> arrow_schema_;

 public:
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

  Schema(std::string name, uint32_t version,
         llvm::SmallVector<std::shared_ptr<Field>, 4> fields)
      : Schema(name, version, fields, crete_arrow_schema(fields)) {}

  const llvm::StringMap<std::shared_ptr<Field>> &fields_map() {
    return field_map_;
  }

  static std::shared_ptr<arrow::Schema> crete_arrow_schema(
      const llvm::SmallVector<std::shared_ptr<Field>, 4> &fields);

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

  [[nodiscard]] const llvm::SmallVector<std::shared_ptr<Field>, 4> &fields()
      const;
  [[nodiscard]] const std::string &name() const;
  [[nodiscard]] uint32_t version() const;
  [[nodiscard]] size_t num_fields() const;
  [[nodiscard]] std::shared_ptr<Field> field(int i) const;
  [[nodiscard]] std::shared_ptr<Field> get_field(const std::string &name) const;
  [[nodiscard]] bool empty() const;
  [[nodiscard]] std::shared_ptr<arrow::Schema> arrow() const;
};

class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<Schema>> schemas_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>>
      arrow_schemas_;

 public:
  SchemaRegistry() = default;

  arrow::Result<bool> create(const std::string &name,
                             const std::shared_ptr<arrow::Schema> &schema);

  arrow::Result<bool> add_arrow(const std::string &name,
                                std::shared_ptr<arrow::Schema> schema);

  arrow::Result<std::shared_ptr<arrow::Schema>> get_arrow(
      const std::string &name) const;

  [[nodiscard]] arrow::Result<std::shared_ptr<Schema>> get(
      const std::string &name) const;

  [[nodiscard]] bool exists(const std::string &name) const;

  [[nodiscard]] std::vector<std::string> get_schema_names() const;
};
}  // namespace tundradb

#endif  // SCHEMA_HPP
