#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace tundradb {
class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schemas;

 public:
  SchemaRegistry() = default;

  // Add a schema with the given name
  arrow::Result<bool> add(const std::string &name,
                          std::shared_ptr<arrow::Schema> schema) {
    schemas.insert(std::make_pair(name, schema));
    return {true};
  }

  // Get a schema by name, returning an error if not found
  arrow::Result<std::shared_ptr<arrow::Schema>> get(
      const std::string &name) const {
    auto it = schemas.find(name);
    if (it == schemas.end()) {
      return arrow::Status::KeyError("Schema not found: ", name);
    }
    return it->second;
  }

  // Check if a schema exists
  [[nodiscard]] bool exists(const std::string &name) const {
    return schemas.find(name) != schemas.end();
  }

  // Get all schema names
  [[nodiscard]] std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(schemas.size());
    for (const auto &schema_pair : schemas) {
      names.push_back(schema_pair.first);
    }
    return names;
  }
};
}  // namespace tundradb

#endif  // SCHEMA_HPP
