#include "../include/arrow_utils.hpp"

#include <algorithm>

#include "../include/logger.hpp"

namespace tundradb {

// Initialize Arrow Compute module - should be called once at startup
bool initialize_arrow_compute() {
  static bool initialized = false;
  if (!initialized) {
    try {
      // Initialize Arrow core
      arrow::GlobalOptions options;
      auto init_status = arrow::Initialize(options);
      if (!init_status.ok()) {
        log_error("Failed to initialize Arrow: {}", init_status.ToString());
        return false;
      }

      // Initialize Arrow Compute module (required for Arrow 21.0.0+)
      // This registers all compute functions including string operations
      auto compute_init_status = arrow::compute::Initialize();
      if (!compute_init_status.ok()) {
        log_error("Failed to initialize Arrow Compute: {}",
                  compute_init_status.ToString());
        return false;
      }

      auto registry = arrow::compute::GetFunctionRegistry();
      if (!registry) {
        log_error("Failed to get Arrow Compute function registry");
        return false;
      }

      auto function_names = registry->GetFunctionNames();
      log_info("Arrow Compute initialized with {} functions",
               function_names.size());

      // Check for essential functions
      bool has_equal = std::find(function_names.begin(), function_names.end(),
                                 "equal") != function_names.end();
      bool has_string_funcs =
          std::find(function_names.begin(), function_names.end(),
                    "starts_with") != function_names.end();

      if (has_equal) {
        log_info(
            "Arrow Compute comparison functions available - using native "
            "implementation");
      } else {
        log_warn("Arrow Compute comparison functions not found");
      }

      if (has_string_funcs) {
        log_info("Arrow Compute string functions available");
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

}  // namespace tundradb