#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include <arrow/api.h>

#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

namespace tundradb {
class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schemas_;

 public:
  SchemaRegistry() = default;

  static std::shared_ptr<arrow::Schema> prepend_id_field(
      const std::shared_ptr<arrow::Schema> &original_schema) {
    // Create the ID field (typically an int64)
    const auto id_field = arrow::field("id", arrow::int64());

    // Get all existing fields
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(original_schema->num_fields() + 1);

    // Add ID field first
    fields.push_back(id_field);

    // Add all existing fields
    for (const auto &field : original_schema->fields()) {
      fields.push_back(field);
    }

    return arrow::schema(fields);
  }

  arrow::Result<bool> create(const std::string &name,
                             std::shared_ptr<arrow::Schema> schema) {
    auto normalized_schema = prepend_id_field(schema);
    log_debug("add schema '{}': {}", name, normalized_schema->ToString());
    schemas_.insert(std::make_pair(name, normalized_schema));
    return {true};
  }

  arrow::Result<bool> add(const std::string &name,
                          std::shared_ptr<arrow::Schema> schema) {
    log_debug("add schema '{}': {}", name, schema->ToString());
    schemas_.insert(std::make_pair(name, schema));
    return {true};
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> get(
      const std::string &name) const {
    const auto it = schemas_.find(name);
    if (it == schemas_.end()) {
      return arrow::Status::KeyError("Schema not found: ", name);
    }
    return it->second;
  }

  [[nodiscard]] bool exists(const std::string &name) const {
    return schemas_.contains(name);
  }

  [[nodiscard]] std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(schemas_.size());
    for (const auto &key : schemas_ | std::views::keys) {
      names.push_back(key);
    }
    return names;
  }
};
}  // namespace tundradb

#endif  // SCHEMA_HPP
