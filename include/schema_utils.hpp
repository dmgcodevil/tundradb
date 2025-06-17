#ifndef SCHEMA_UTILS_HPP
#define SCHEMA_UTILS_HPP

#include <arrow/api.h>

#include <memory>

namespace tundradb {

/**
 * Creates a new schema by prepending an "id" field of type Int64 to the target
 * schema.
 *
 * @param target_schema The original schema to modify
 * @return A new schema with "id" field prepended
 */
inline std::shared_ptr<arrow::Schema> prepend_id_field(
    const std::shared_ptr<arrow::Schema>& target_schema) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(target_schema->num_fields() + 1);

  fields.push_back(arrow::field("id", arrow::int64()));

  for (int i = 0; i < target_schema->num_fields(); ++i) {
    fields.push_back(target_schema->field(i));
  }

  return arrow::schema(fields);
}

}  // namespace tundradb

#endif  // SCHEMA_UTILS_HPP