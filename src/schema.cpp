#include "schema.hpp"
namespace tundradb {

arrow::Result<Field> Field::from_arrow(
    const std::shared_ptr<arrow::Field> &field) {
  // Field result;
  // result.name = field->name();
  // result.nullable = field->nullable();

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

[[nodiscard]] arrow::Result<arrow::Field> Field::to_arrow() const {}

arrow::Result<Schema> Schema::from_arrow(
    const std::string &schema_name,
    const std::shared_ptr<arrow::Schema> &schema) {
  // Schema result;
  // result.name = schema_name;
  // result.version = 0;

  std::vector<std::shared_ptr<Field>> fields;

  for (const auto &arrow_field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto field, Field::from_arrow(arrow_field));
    fields.push_back(std::make_shared<Field>(field));
  }

  return Schema(schema_name, 0, fields);
}

[[nodiscard]] const std::vector<std::shared_ptr<Field>> &Schema::fields()
    const {
  return fields_;
}
[[nodiscard]] const std::string &Schema::name() const { return name_; }
[[nodiscard]] uint32_t Schema::version() const { return version_; }

bool Schema::empty() const { return fields_.empty(); }

arrow::Result<bool> SchemaRegistry::create(
    const std::string &name, std::shared_ptr<arrow::Schema> schema) {
  auto normalized_schema = prepend_id_field(schema);
  return add_arrow(name, normalized_schema);
  // schemas_.insert(std::make_pair(name, normalized_schema));
  // return {true};
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
  schemas_.emplace(name,
                   std::make_shared<Schema>(std::move(result.ValueOrDie())));

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