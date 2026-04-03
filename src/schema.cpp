#include "schema.hpp"

#include <iostream>

#include "arrow_map_union_types.hpp"
#include "arrow_utils.hpp"

namespace tundradb {

/// Helper: convert an Arrow element DataType to our ValueType.
static arrow::Result<ValueType> arrow_elem_to_value_type(
    const std::shared_ptr<arrow::DataType> &dt) {
  switch (dt->id()) {
    case arrow::Type::BOOL:
      return ValueType::BOOL;
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
      return ValueType::INT32;
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      return ValueType::INT64;
    case arrow::Type::FLOAT:
      return ValueType::FLOAT;
    case arrow::Type::DOUBLE:
      return ValueType::DOUBLE;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return ValueType::STRING;
    default:
      return arrow::Status::NotImplemented("Unsupported Arrow element type: ",
                                           dt->ToString());
  }
}

arrow::Result<Field> Field::from_arrow(
    const std::shared_ptr<arrow::Field> &field) {
  const auto &dt = field->type();

  // Handle list / fixed_size_list -> ARRAY
  if (dt->id() == arrow::Type::LIST) {
    auto list_type = std::static_pointer_cast<arrow::ListType>(dt);
    ARROW_ASSIGN_OR_RAISE(auto elem_vt,
                          arrow_elem_to_value_type(list_type->value_type()));
    return Field(field->name(), TypeDescriptor::array(elem_vt),
                 field->nullable());
  }
  if (dt->id() == arrow::Type::FIXED_SIZE_LIST) {
    auto fsl_type = std::static_pointer_cast<arrow::FixedSizeListType>(dt);
    ARROW_ASSIGN_OR_RAISE(auto elem_vt,
                          arrow_elem_to_value_type(fsl_type->value_type()));
    return Field(field->name(),
                 TypeDescriptor::array(elem_vt, fsl_type->list_size()),
                 field->nullable());
  }

  // Handle map -> MAP
  if (dt->id() == arrow::Type::MAP) {
    return Field(field->name(), TypeDescriptor::properties(),
                 field->nullable());
  }

  // Scalar types
  ARROW_ASSIGN_OR_RAISE(auto vt, arrow_elem_to_value_type(dt));
  return Field(field->name(), vt, field->nullable());
}

[[nodiscard]] arrow::Result<std::shared_ptr<arrow::Field>> Field::to_arrow()
    const {
  const auto base = type_desc_.base_type;
  switch (base) {
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
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      return arrow::field(name_, arrow::utf8());
    case ValueType::ARRAY: {
      // Convert element type to Arrow, then wrap in list
      auto elem_arrow = TypeDescriptor{{type_desc_.element_type}};
      // Map element ValueType to arrow type
      std::shared_ptr<arrow::DataType> elem_dt;
      switch (type_desc_.element_type) {
        case ValueType::INT32:
          elem_dt = arrow::int32();
          break;
        case ValueType::INT64:
          elem_dt = arrow::int64();
          break;
        case ValueType::FLOAT:
          elem_dt = arrow::float32();
          break;
        case ValueType::DOUBLE:
          elem_dt = arrow::float64();
          break;
        case ValueType::BOOL:
          elem_dt = arrow::boolean();
          break;
        case ValueType::STRING:
          elem_dt = arrow::utf8();
          break;
        default:
          return arrow::Status::NotImplemented(
              "Unsupported array element type: ",
              static_cast<int>(type_desc_.element_type));
      }
      if (type_desc_.fixed_size > 0) {
        return arrow::field(name_,
                            arrow::fixed_size_list(
                                arrow::field("item", elem_dt),
                                static_cast<int32_t>(type_desc_.fixed_size)));
      }
      return arrow::field(name_, arrow::list(arrow::field("item", elem_dt)));
    }
    case ValueType::MAP:
      // MAP (properties) -> Arrow map<utf8, dense_union<...>> for portable
      // typed values.
      return arrow::field(
          name_, arrow::map(arrow::utf8(), map_union_value_type()), nullable_);
    default:
      return arrow::Status::NotImplemented("Unsupported ValueType: ",
                                           static_cast<int>(base));
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