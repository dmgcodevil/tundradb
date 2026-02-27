#include "arrow_utils.hpp"

#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/datum.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>

#include <algorithm>

#include "logger.hpp"
#include "node.hpp"
#include "query.hpp"
#include "schema.hpp"

namespace tundradb {

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

// Arrow utility function implementations
arrow::Result<std::shared_ptr<arrow::Array>> create_int64_array(
    const int64_t value) {
  arrow::Int64Builder int64_builder;
  ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
  ARROW_RETURN_NOT_OK(int64_builder.Append(value));
  std::shared_ptr<arrow::Array> int64_array;
  ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
  return int64_array;
}

arrow::Result<std::shared_ptr<arrow::Array>> create_str_array(
    const std::string& value) {
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(value));
  std::shared_ptr<arrow::Array> string_arr;
  ARROW_RETURN_NOT_OK(builder.Finish(&string_arr));
  return string_arr;
}

arrow::Result<std::shared_ptr<arrow::Array>> create_null_array(
    const std::shared_ptr<arrow::DataType>& type) {
  switch (type->id()) {
    case arrow::Type::INT64: {
      arrow::Int64Builder builder;
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      return array;
    }
    case arrow::Type::STRING: {
      arrow::StringBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      return array;
    }
    default:
      return arrow::Status::NotImplemented("Unsupported type: ",
                                           type->ToString());
  }
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

arrow::Result<std::shared_ptr<arrow::Scalar>> value_to_arrow_scalar(
    const Value& value) {
  switch (value.type()) {
    case ValueType::INT32:
      return arrow::MakeScalar(value.as_int32());
    case ValueType::INT64:
      return arrow::MakeScalar(value.as_int64());
    case ValueType::DOUBLE:
      return arrow::MakeScalar(value.as_double());
    case ValueType::STRING:
      return arrow::MakeScalar(value.as_string());
    case ValueType::BOOL:
      return arrow::MakeScalar(value.as_bool());
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          tundradb::to_string(value.type()));
  }
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
      const auto& field_name = field->name();

      auto res = view.get_value_ptr(field);
      if (res.ok()) {
        auto value = res.ValueOrDie();
        if (value) {
          auto scalar_result = value_ptr_to_arrow_scalar(value, field->type());
          if (!scalar_result.ok()) {
            log_error("Failed to convert value to scalar for field '{}': {}",
                      field_name, scalar_result.status().ToString());
            return scalar_result.status();
          }

          const auto& scalar = scalar_result.ValueOrDie();
          auto status = builders[i]->AppendScalar(*scalar);
          if (!status.ok()) {
            log_error("Failed to append scalar for field '{}': {}", field_name,
                      status.ToString());
            return status;
          }
        } else {
          IF_DEBUG_ENABLED {
            log_debug("Null value for field '{}', appending null", field_name);
          }
          auto status = builders[i]->AppendNull();
          if (!status.ok()) {
            log_error("Failed to append null for field '{}': {}", field_name,
                      status.ToString());
            return status;
          }
        }
      } else {
        IF_DEBUG_ENABLED {
          log_debug("Field '{}' not found in node, appending null", field_name);
        }
        auto status = builders[i]->AppendNull();
        if (!status.ok()) {
          log_error("Failed to append null for field '{}': {}", field_name,
                    status.ToString());
          return status;
        }
      }
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
    std::shared_ptr<arrow::Array> empty_array;

    switch (field->type()->id()) {
      case arrow::Type::INT64: {
        arrow::Int64Builder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::STRING: {
        arrow::StringBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::DOUBLE: {
        arrow::DoubleBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::BOOL: {
        arrow::BooleanBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      default:
        empty_array = std::make_shared<arrow::NullArray>(0);
    }

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