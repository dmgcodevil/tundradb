#include "schema.hpp"

#include <iostream>

#include "arrow_utils.hpp"

namespace tundradb {

arrow::Result<Field> Field::from_arrow(
    const std::shared_ptr<arrow::Field> &field) {
  ValueType type;

  switch (field->type()->id()) {
    case arrow::Type::BOOL:
      type = ValueType::BOOL;
      break;
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
      type = ValueType::INT32;
      break;
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      type = ValueType::INT64;
      break;
    case arrow::Type::FLOAT:
      type = ValueType::FLOAT;
      break;
    case arrow::Type::DOUBLE:
      type = ValueType::DOUBLE;
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      type = ValueType::STRING;
      break;
    default:
      return arrow::Status::NotImplemented("Unsupported Arrow type: ",
                                           field->type()->ToString());
  }

  return Field(field->name(), type, field->nullable());
}

[[nodiscard]] arrow::Result<std::shared_ptr<arrow::Field>> Field::to_arrow()
    const {
  switch (type_) {
    case ValueType::BOOL:
      return arrow::field(name_, arrow::boolean());
    case ValueType::INT32:
      return arrow::field(name_, arrow::int32());
    case ValueType::INT64:
      return arrow::field(name_, arrow::int64());
    case ValueType::FLOAT:
      return arrow::field(name_, arrow::float32());
    case ValueType::DOUBLE:
      return arrow::field(name_, arrow::float64());
    case ValueType::STRING:
      return arrow::field(name_, arrow::utf8());
    default:
      return arrow::Status::NotImplemented("Unsupported ValueType: ",
                                           static_cast<int>(type_));
  }
}

std::shared_ptr<arrow::Schema> Schema::crete_arrow_schema(
    const llvm::SmallVector<std::shared_ptr<Field>, 4> &fields) {
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (auto const &field : fields) {
    arrow_fields.push_back(field->to_arrow().ValueOrDie());
  }
  return arrow::schema(std::move(arrow_fields));
}

std::shared_ptr<arrow::Schema> Schema::to_arrow(
    const std::shared_ptr<Schema> &schema) {
  return crete_arrow_schema(schema->fields());
}

arrow::Result<std::shared_ptr<Schema>> Schema::from_arrow(
    const std::string &schema_name,
    const std::shared_ptr<arrow::Schema> &arrow_schema) {
  llvm::SmallVector<std::shared_ptr<Field>, 4> fields;

  for (const auto &arrow_field : arrow_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto field, Field::from_arrow(arrow_field));
    fields.push_back(std::make_shared<Field>(field));
  }
  return std::make_shared<Schema>(schema_name, 0, fields, arrow_schema);
}

[[nodiscard]] const llvm::SmallVector<std::shared_ptr<Field>, 4> &
Schema::fields() const {
  return fields_;
}
[[nodiscard]] const std::string &Schema::name() const { return name_; }
[[nodiscard]] uint32_t Schema::version() const { return version_; }
[[nodiscard]] size_t Schema::num_fields() const { return num_fields_; }
[[nodiscard]] std::shared_ptr<Field> Schema::field(const int i) const {
  return fields_[i];
}
[[nodiscard]] std::shared_ptr<Field> Schema::get_field(
    const std::string &name) const {
  auto it = field_map_.find(name);
  if (it != field_map_.end()) {
    return it->second;
  }
  return nullptr;
}

bool Schema::empty() const { return fields_.empty(); }

[[nodiscard]] std::shared_ptr<arrow::Schema> Schema::arrow() const {
  return arrow_schema_;
}

arrow::Result<bool> SchemaRegistry::create(
    const std::string &name, const std::shared_ptr<arrow::Schema> &schema) {
  const auto normalized_schema = prepend_id_field(schema);
  return add_arrow(name, normalized_schema);
}

arrow::Result<bool> SchemaRegistry::add_arrow(
    const std::string &name, std::shared_ptr<arrow::Schema> schema) {
  log_debug("add schema '{}': {}", name, schema->ToString());
  arrow_schemas_.emplace(name, schema);
  auto result = Schema::from_arrow(name, schema);
  if (!result.ok()) {
    return result.status();
  }
  log_debug("add native schema");
  schemas_.emplace(name, result.ValueOrDie());

  return {true};
}

arrow::Result<std::shared_ptr<Schema>> SchemaRegistry::get(
    const std::string &name) const {
  const auto it = schemas_.find(name);
  if (it == schemas_.end()) {
    return arrow::Status::KeyError("Schema not found: ", name);
  }
  return it->second;
}

[[deprecated]]
arrow::Result<std::shared_ptr<arrow::Schema>> SchemaRegistry::get_arrow(
    const std::string &name) const {
  const auto it = arrow_schemas_.find(name);
  if (it == arrow_schemas_.end()) {
    return arrow::Status::KeyError("Schema not found: ", name);
  }
  return it->second;
}

[[nodiscard]] bool SchemaRegistry::exists(const std::string &name) const {
  return schemas_.contains(name);
}

[[nodiscard]] std::vector<std::string> SchemaRegistry::get_schema_names()
    const {
  std::vector<std::string> names;
  names.reserve(schemas_.size());
  for (const auto &key : schemas_ | std::views::keys) {
    names.push_back(key);
  }
  return names;
}

}  // namespace tundradb