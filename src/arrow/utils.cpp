#include "arrow/utils.hpp"

#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/datum.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "arrow/map_union_types.hpp"
#include "common/logger.hpp"
#include "core/node.hpp"
#include "query/query.hpp"
#include "schema/schema.hpp"

namespace tundradb {

namespace {}  // namespace

arrow::Result<llvm::DenseSet<int64_t>> get_ids_from_table(
    const std::shared_ptr<arrow::Table>& table) {
  log_debug("Extracting IDs from table with {} rows", table->num_rows());

  const auto id_idx = table->schema()->GetFieldIndex("id");
  if (id_idx == -1) {
    log_error("Table does not have an 'id' column");
    return arrow::Status::Invalid("table does not have an 'id' column");
  }

  const auto id_column = table->column(id_idx);
  llvm::DenseSet<int64_t> result_ids;
  result_ids.reserve(table->num_rows());

  for (int chunk_idx = 0; chunk_idx < id_column->num_chunks(); chunk_idx++) {
    const auto chunk = std::static_pointer_cast<arrow::Int64Array>(
        id_column->chunk(chunk_idx));
    log_debug("Processing chunk {} with {} rows", chunk_idx, chunk->length());
    for (int i = 0; i < chunk->length(); i++) {
      result_ids.insert(chunk->Value(i));
    }
  }

  log_debug("Extracted {} unique IDs from table", result_ids.size());
  return result_ids;
}

// Initialize Arrow Compute module - should be called once at startup
bool initialize_arrow_compute() {
  static bool initialized = false;
  if (!initialized) {
    try {
      // Initialize Arrow core
      const arrow::GlobalOptions options;
      if (const auto init_status = arrow::Initialize(options);
          !init_status.ok()) {
        log_error("Failed to initialize Arrow: {}", init_status.ToString());
        return false;
      }

      // Initialize Arrow Compute module (required for Arrow 21.0.0+)
      // This registers all compute functions including string operations
      if (const auto compute_init_status = arrow::compute::Initialize();
          !compute_init_status.ok()) {
        log_error("Failed to initialize Arrow Compute: {}",
                  compute_init_status.ToString());
        return false;
      }

      const auto registry = arrow::compute::GetFunctionRegistry();
      if (!registry) {
        log_error("Failed to get Arrow Compute function registry");
        return false;
      }

      auto function_names = registry->GetFunctionNames();
      log_debug("Arrow Compute initialized with {} functions",
                function_names.size());

      // Check for essential functions
      const bool has_equal =
          std::ranges::find(function_names, "equal") != function_names.end();
      const bool has_string_funcs =
          std::ranges::find(function_names, "starts_with") !=
          function_names.end();

      if (!has_equal) {
        log_warn("Arrow Compute comparison functions not found");
      }

      if (!has_string_funcs) {
        log_warn("Arrow Compute string functions not found");
      }

      initialized = true;
    } catch (const std::exception& e) {
      log_error("Exception during Arrow Compute initialization: {}", e.what());
      return false;
    }
  }
  return initialized;
}

std::shared_ptr<arrow::Schema> prepend_field(
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Schema>& target_schema) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(target_schema->num_fields() + 1);

  fields.push_back(field);

  for (int i = 0; i < target_schema->num_fields(); ++i) {
    fields.push_back(target_schema->field(i));
  }

  return arrow::schema(fields);
}

std::shared_ptr<arrow::Schema> prepend_id_field(
    const std::shared_ptr<arrow::Schema>& target_schema) {
  return prepend_field(arrow::field("id", arrow::int64()), target_schema);
}

arrow::Result<std::shared_ptr<arrow::Scalar>> value_ptr_to_arrow_scalar(
    const char* ptr, const ValueType type) {
  switch (type) {
    case ValueType::INT32:
      return arrow::MakeScalar(*reinterpret_cast<const int32_t*>(ptr));
    case ValueType::INT64:
      return arrow::MakeScalar(*reinterpret_cast<const int64_t*>(ptr));
    case ValueType::DOUBLE:
      return arrow::MakeScalar(*reinterpret_cast<const double*>(ptr));
    case ValueType::STRING: {
      auto str_ref = *reinterpret_cast<const StringRef*>(ptr);
      return arrow::MakeScalar(str_ref.to_string());
    }
    case ValueType::BOOL:
      return arrow::MakeScalar(*reinterpret_cast<const bool*>(ptr));
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          tundradb::to_string(type));
  }
}

arrow::Result<std::optional<Value>> map_item_to_value(
    const std::shared_ptr<arrow::Array>& items, const int64_t idx) {
  auto dense = std::dynamic_pointer_cast<arrow::DenseUnionArray>(items);
  if (!dense) {
    return arrow::Status::Invalid(
        "MAP item array is not DenseUnionArray for union conversion");
  }
  if (dense->IsNull(idx)) {
    return std::optional<Value>{};
  }
  const int8_t type_code = dense->type_code(idx);
  const int child_id = dense->child_id(idx);
  const int64_t value_offset = dense->value_offset(idx);
  auto child = dense->field(child_id);

  switch (type_code) {
    case kMapUnionInt32: {
      auto arr = std::dynamic_pointer_cast<arrow::Int32Array>(child);
      if (!arr || arr->IsNull(value_offset)) return std::optional<Value>{};
      return Value(arr->Value(value_offset));
    }
    case kMapUnionInt64: {
      auto arr = std::dynamic_pointer_cast<arrow::Int64Array>(child);
      if (!arr || arr->IsNull(value_offset)) return std::optional<Value>{};
      return Value(arr->Value(value_offset));
    }
    case kMapUnionFloat: {
      auto arr = std::dynamic_pointer_cast<arrow::FloatArray>(child);
      if (!arr || arr->IsNull(value_offset)) return std::optional<Value>{};
      return Value(arr->Value(value_offset));
    }
    case kMapUnionDouble: {
      auto arr = std::dynamic_pointer_cast<arrow::DoubleArray>(child);
      if (!arr || arr->IsNull(value_offset)) return std::optional<Value>{};
      return Value(arr->Value(value_offset));
    }
    case kMapUnionBool: {
      auto arr = std::dynamic_pointer_cast<arrow::BooleanArray>(child);
      if (!arr || arr->IsNull(value_offset)) return std::optional<Value>{};
      return Value(arr->Value(value_offset));
    }
    case kMapUnionString: {
      auto arr = std::dynamic_pointer_cast<arrow::StringArray>(child);
      if (!arr || arr->IsNull(value_offset)) return std::optional<Value>{};
      return Value(arr->GetString(value_offset));
    }
    default:
      return arrow::Status::Invalid("Unsupported MAP union type code: ",
                                    static_cast<int>(type_code));
  }
}

arrow::Result<Value> array_element_to_value(
    const std::shared_ptr<arrow::Array>& values, const int64_t idx) {
  if (values->IsNull(idx)) {
    return Value{};
  }

  switch (values->type_id()) {
    case arrow::Type::INT32:
      return Value(
          std::static_pointer_cast<arrow::Int32Array>(values)->Value(idx));
    case arrow::Type::INT64:
      return Value(
          std::static_pointer_cast<arrow::Int64Array>(values)->Value(idx));
    case arrow::Type::FLOAT:
      return Value(
          std::static_pointer_cast<arrow::FloatArray>(values)->Value(idx));
    case arrow::Type::DOUBLE:
      return Value(
          std::static_pointer_cast<arrow::DoubleArray>(values)->Value(idx));
    case arrow::Type::BOOL:
      return Value(
          std::static_pointer_cast<arrow::BooleanArray>(values)->Value(idx));
    case arrow::Type::STRING:
      return Value(
          std::static_pointer_cast<arrow::StringArray>(values)->GetString(idx));
    default:
      return arrow::Status::NotImplemented("Unsupported ARRAY element type: ",
                                           values->type()->ToString());
  }
}

arrow::Status append_value_to_builder(const ValueRef& value,
                                      arrow::ArrayBuilder* builder) {
  if (value.data == nullptr) {
    return builder->AppendNull();
  }
  switch (value.type) {
    case ValueType::INT32:
      return static_cast<arrow::Int32Builder*>(builder)->Append(
          value.as_int32());
    case ValueType::INT64:
      return static_cast<arrow::Int64Builder*>(builder)->Append(
          value.as_int64());
    case ValueType::FLOAT:
      return static_cast<arrow::FloatBuilder*>(builder)->Append(
          value.as_float());
    case ValueType::DOUBLE:
      return static_cast<arrow::DoubleBuilder*>(builder)->Append(
          value.as_double());
    case ValueType::BOOL:
      return static_cast<arrow::BooleanBuilder*>(builder)->Append(
          value.as_bool());
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64: {
      const auto& s = value.as_string_ref();
      return static_cast<arrow::StringBuilder*>(builder)->Append(s.data(),
                                                                 s.length());
    }
    case ValueType::ARRAY: {
      auto* lb = dynamic_cast<arrow::ListBuilder*>(builder);
      if (!lb) return arrow::Status::Invalid("Expected ListBuilder for ARRAY");
      return append_array_to_list_builder(value.as_array_ref(), lb);
    }
    case ValueType::MAP: {
      auto* mb = dynamic_cast<arrow::MapBuilder*>(builder);
      if (!mb) return arrow::Status::Invalid("Expected MapBuilder for MAP");
      return append_map_to_map_builder(value.as_map_ref(), mb);
    }
    default:
      return builder->AppendNull();
  }
}

arrow::Status append_array_to_list_builder(const ArrayRef& arr_ref,
                                           arrow::ListBuilder* list_builder) {
  if (arr_ref.is_null()) {
    return list_builder->AppendNull();
  }
  ARROW_RETURN_NOT_OK(list_builder->Append());
  auto* vb = list_builder->value_builder();
  for (uint32_t j = 0; j < arr_ref.length(); ++j) {
    const char* ep = arr_ref.element_ptr(j);
    switch (arr_ref.elem_type()) {
      case ValueType::INT32:
        ARROW_RETURN_NOT_OK(static_cast<arrow::Int32Builder*>(vb)->Append(
            *reinterpret_cast<const int32_t*>(ep)));
        break;
      case ValueType::INT64:
        ARROW_RETURN_NOT_OK(static_cast<arrow::Int64Builder*>(vb)->Append(
            *reinterpret_cast<const int64_t*>(ep)));
        break;
      case ValueType::DOUBLE:
        ARROW_RETURN_NOT_OK(static_cast<arrow::DoubleBuilder*>(vb)->Append(
            *reinterpret_cast<const double*>(ep)));
        break;
      case ValueType::BOOL:
        ARROW_RETURN_NOT_OK(static_cast<arrow::BooleanBuilder*>(vb)->Append(
            *reinterpret_cast<const bool*>(ep)));
        break;
      case ValueType::STRING: {
        const auto& sr = *reinterpret_cast<const StringRef*>(ep);
        ARROW_RETURN_NOT_OK(static_cast<arrow::StringBuilder*>(vb)->Append(
            sr.data(), sr.length()));
        break;
      }
      default:
        return arrow::Status::NotImplemented(
            "Unsupported array element type: ",
            tundradb::to_string(arr_ref.elem_type()));
    }
  }
  return arrow::Status::OK();
}

arrow::Status append_map_to_map_builder(const MapRef& map_ref,
                                        arrow::MapBuilder* map_builder) {
  if (map_ref.is_null()) {
    return map_builder->AppendNull();
  }

  auto* key_builder =
      dynamic_cast<arrow::StringBuilder*>(map_builder->key_builder());
  auto* union_builder =
      dynamic_cast<arrow::DenseUnionBuilder*>(map_builder->item_builder());
  if (!key_builder || !union_builder) {
    return arrow::Status::Invalid(
        "MapBuilder does not have expected key/dense-union item builders");
  }

  ARROW_RETURN_NOT_OK(map_builder->Append());
  for (uint32_t i = 0; i < map_ref.count(); ++i) {
    const auto* entry = map_ref.entry_ptr(i);
    const auto& key = entry->key;
    ARROW_RETURN_NOT_OK(
        key_builder->Append(key.data(), static_cast<int32_t>(key.length())));

    const auto value_type = static_cast<ValueType>(entry->value_type);
    switch (value_type) {
      case ValueType::INT32: {
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionInt32));
        auto* b = dynamic_cast<arrow::Int32Builder*>(
            union_builder->child_builder(0).get());
        if (!b)
          return arrow::Status::Invalid("MAP union child[0] is not int32");
        ARROW_RETURN_NOT_OK(
            b->Append(*reinterpret_cast<const int32_t*>(entry->value)));
        break;
      }
      case ValueType::INT64: {
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionInt64));
        auto* b = dynamic_cast<arrow::Int64Builder*>(
            union_builder->child_builder(1).get());
        if (!b)
          return arrow::Status::Invalid("MAP union child[1] is not int64");
        ARROW_RETURN_NOT_OK(
            b->Append(*reinterpret_cast<const int64_t*>(entry->value)));
        break;
      }
      case ValueType::FLOAT: {
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionFloat));
        auto* b = dynamic_cast<arrow::FloatBuilder*>(
            union_builder->child_builder(2).get());
        if (!b)
          return arrow::Status::Invalid("MAP union child[2] is not float");
        ARROW_RETURN_NOT_OK(
            b->Append(*reinterpret_cast<const float*>(entry->value)));
        break;
      }
      case ValueType::DOUBLE: {
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionDouble));
        auto* b = dynamic_cast<arrow::DoubleBuilder*>(
            union_builder->child_builder(3).get());
        if (!b)
          return arrow::Status::Invalid("MAP union child[3] is not double");
        ARROW_RETURN_NOT_OK(
            b->Append(*reinterpret_cast<const double*>(entry->value)));
        break;
      }
      case ValueType::BOOL: {
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionBool));
        auto* b = dynamic_cast<arrow::BooleanBuilder*>(
            union_builder->child_builder(4).get());
        if (!b) return arrow::Status::Invalid("MAP union child[4] is not bool");
        ARROW_RETURN_NOT_OK(
            b->Append(*reinterpret_cast<const bool*>(entry->value)));
        break;
      }
      case ValueType::STRING:
      case ValueType::FIXED_STRING16:
      case ValueType::FIXED_STRING32:
      case ValueType::FIXED_STRING64: {
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionString));
        auto* b = dynamic_cast<arrow::StringBuilder*>(
            union_builder->child_builder(5).get());
        if (!b)
          return arrow::Status::Invalid("MAP union child[5] is not string");
        const auto& sr = *reinterpret_cast<const StringRef*>(entry->value);
        ARROW_RETURN_NOT_OK(
            b->Append(sr.data(), static_cast<int32_t>(sr.length())));
        break;
      }
      case ValueType::ARRAY:
      case ValueType::MAP:
      case ValueType::NA:
      default: {
        // Fallback complex map values to their string representation.
        ARROW_RETURN_NOT_OK(union_builder->Append(kMapUnionString));
        auto* b = dynamic_cast<arrow::StringBuilder*>(
            union_builder->child_builder(5).get());
        if (!b)
          return arrow::Status::Invalid("MAP union child[5] is not string");
        ARROW_RETURN_NOT_OK(
            b->Append(Value::read_value_from_memory(entry->value, value_type)
                          .to_string()));
        break;
      }
    }
  }

  return arrow::Status::OK();
}

arrow::compute::Expression where_condition_to_expression(
    const WhereExpr& condition, bool strip_var) {
  return condition.to_arrow_expression(strip_var);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_nodes(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes) {
  auto arrow_schema = schema->arrow();
  IF_DEBUG_ENABLED {
    log_debug("Creating table from {} nodes with schema '{}'", nodes.size(),
              arrow_schema->ToString());
  }

  // Create builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : arrow_schema->fields()) {
    IF_DEBUG_ENABLED {
      log_debug("Creating builder for field '{}' with type {}", field->name(),
                field->type()->ToString());
    }
    auto builder_result = arrow::MakeBuilder(field->type());
    if (!builder_result.ok()) {
      log_error("Failed to create builder for field '{}': {}", field->name(),
                builder_result.status().ToString());
      return builder_result.status();
    }
    builders.push_back(std::move(builder_result.ValueOrDie()));
  }

  // Populate builders with data from each node
  IF_DEBUG_ENABLED {
    log_debug("Adding data from {} nodes to builders", nodes.size());
  }
  for (const auto& node : nodes) {
    auto view = node->view(nullptr);
    for (int i = 0; i < schema->num_fields(); i++) {
      auto field = schema->field(i);
      auto res = view.get_value_ptr(field);
      ValueRef vr;
      if (res.ok() && res.ValueOrDie() != nullptr) {
        vr = ValueRef(res.ValueOrDie(), field->type());
      }
      ARROW_RETURN_NOT_OK(append_value_to_builder(vr, builders[i].get()));
    }
  }

  // Finish building arrays
  IF_DEBUG_ENABLED { log_debug("Finalizing arrays from builders"); }
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());
  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    auto status = builder->Finish(&array);
    if (!status.ok()) {
      log_error("Failed to finish array builder: {}", status.ToString());
      return status;
    }
    arrays.push_back(array);
  }

  IF_DEBUG_ENABLED {
    log_debug("Creating table with {} rows and {} columns",
              arrays.empty() ? 0 : arrays[0]->length(), arrays.size());
  }
  return arrow::Table::Make(arrow_schema, arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_empty_table(
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<std::shared_ptr<arrow::Array>> empty_arrays;

  for (const auto& field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    std::shared_ptr<arrow::Array> empty_array;
    ARROW_RETURN_NOT_OK(builder->Finish(&empty_array));
    empty_arrays.push_back(empty_array);
  }

  return arrow::Table::Make(schema, empty_arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> filter(
    const std::shared_ptr<arrow::Table>& table, const WhereExpr& condition,
    const bool strip_var) {
  IF_DEBUG_ENABLED {
    log_debug("Filtering table with WhereCondition: {}", condition.toString());
  }

  try {
    const auto filter_expr =
        where_condition_to_expression(condition, strip_var);

    IF_DEBUG_ENABLED {
      log_debug("Creating in-memory dataset from table with {} rows",
                table->num_rows());
    }
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);

    IF_DEBUG_ENABLED { log_debug("Creating scanner builder"); }
    auto scan_builder_result = dataset->NewScan();
    if (!scan_builder_result.ok()) {
      log_error("Failed to create scanner builder: {}",
                scan_builder_result.status().ToString());
      return scan_builder_result.status();
    }
    const auto& scan_builder = scan_builder_result.ValueOrDie();

    IF_DEBUG_ENABLED {
      log_debug("Applying compound filter to scanner builder");
    }
    auto filter_status = scan_builder->Filter(filter_expr);
    if (!filter_status.ok()) {
      log_error("Failed to apply filter: {}", filter_status.ToString());
      return filter_status;
    }

    IF_DEBUG_ENABLED { log_debug("Finishing scanner"); }
    auto scanner_result = scan_builder->Finish();
    if (!scanner_result.ok()) {
      log_error("Failed to finish scanner: {}",
                scanner_result.status().ToString());
      return scanner_result.status();
    }
    const auto& scanner = scanner_result.ValueOrDie();

    IF_DEBUG_ENABLED { log_debug("Executing scan to table"); }
    auto table_result = scanner->ToTable();
    if (!table_result.ok()) {
      log_error("Failed to convert scan results to table: {}",
                table_result.status().ToString());
      return table_result.status();
    }

    auto result_table = table_result.ValueOrDie();
    IF_DEBUG_ENABLED {
      log_debug("Filter completed: {} rows in, {} rows out", table->num_rows(),
                result_table->num_rows());
    }
    return result_table;

  } catch (const std::exception& e) {
    log_error("Failed to convert WhereCondition to Arrow expression: {}",
              e.what());
    return arrow::Status::Invalid("Failed to convert WHERE condition: ",
                                  e.what());
  }
}

}  // namespace tundradb