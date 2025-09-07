#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include <arrow/api.h>

#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow_utils.hpp"
#include "logger.hpp"
#include "types.hpp"

namespace tundradb {

struct Field {
 private:
  std::string name_;
  ValueType type_;
  bool nullable_ = true;

 public:
  Field(std::string name, ValueType type, bool nullable = true)
      : name_(std::move(name)), type_(type), nullable_(nullable) {}

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

  [[nodiscard]] arrow::Result<arrow::Field> to_arrow() const;
};

struct Schema {
 private:
  std::string name_;
  uint32_t version_;
  std::vector<std::shared_ptr<Field>> fields_;

 public:
  Schema(std::string name, uint32_t version,
         std::vector<std::shared_ptr<Field>> fields)
      : name_(std::move(name)), version_(version), fields_(std::move(fields)) {}

  /**
   * @brief Convert an Arrow schema to Schema
   *
   * @param schema_name shema name
   * @param schema The Arrow schema to convert
   * @return arrow::Result<Schema> The resulting schema
   */
  static arrow::Result<Schema> from_arrow(
      const std::string &schema_name,
      const std::shared_ptr<arrow::Schema> &schema);

  [[nodiscard]] arrow::Result<arrow::Schema> to_arrow() const;

  [[nodiscard]] const std::vector<std::shared_ptr<Field>> &fields() const;
  [[nodiscard]] const std::string &name() const;
  [[nodiscard]] uint32_t version() const;
  [[nodiscard]] std::shared_ptr<Field> get_field(const std::string &name) const;

  [[nodiscard]] bool empty() const;
};

class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<Schema>> schemas_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>>
      arrow_schemas_;

 public:
  SchemaRegistry() = default;

  arrow::Result<bool> create(const std::string &name,
                             std::shared_ptr<arrow::Schema> schema);

  arrow::Result<bool> add_arrow(const std::string &name,
                                std::shared_ptr<arrow::Schema> schema);

  arrow::Result<std::shared_ptr<arrow::Schema>> get_arrow(
      const std::string &name) const;

  // arrow::Result<bool> create(const std::string &name,
  //                            std::shared_ptr<arrow::Schema> schema) {
  //   // todo rename "id" to "_id"
  //   auto normalized_schema = prepend_id_field(schema);
  //   log_debug("add schema '{}': {}", name, normalized_schema->ToString());
  //   return add_arrow(name, normalized_schema);
  // }

  // arrow::Result<bool> add_arrow(const std::string &name,
  //                               const std::shared_ptr<arrow::Schema> &schema)
  //                               {
  //   log_debug("add schema '{}': {}", name, schema->ToString());
  //   arrow_schemas_.emplace(name, schema);
  //   auto result = Schema::from_arrow(name, schema);
  //   if (!result.ok()) {
  //     return result.status();
  //   }
  //
  //   schemas_.emplace(name,
  //                    std::make_shared<Schema>(std::move(result.ValueOrDie())));
  //
  //   return {true};
  // }

  [[nodiscard]] arrow::Result<std::shared_ptr<Schema>> get(
      const std::string &name) const;

  [[nodiscard]] bool exists(const std::string &name) const;

  [[nodiscard]] std::vector<std::string> get_schema_names() const;
};
}  // namespace tundradb

#endif  // SCHEMA_HPP
