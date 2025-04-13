#ifndef UTILS_HPP
#define UTILS_HPP

#include <arrow/result.h>
#include <arrow/table.h>
#include <uuid/uuid.h>

#include <chrono>
#include <random>
#include <source_location>
#include <string>

#include "logger.hpp"
#include "node.hpp"
#include "schema_utils.hpp"

namespace tundradb {
static std::string generate_uuid() {
  uuid_t uuid;
  uuid_generate(uuid);
  char uuid_str[37];
  uuid_unparse_lower(uuid, uuid_str);
  return uuid_str;
}

static int64_t now_millis() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

// Create a table from a schema and a list of nodes
static arrow::Result<std::shared_ptr<arrow::Table>> create_table(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes, size_t chunk_size) {
  auto final_schema = prepend_id_field(schema);
  if (nodes.empty()) {
    // Return empty table with the given schema
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(final_schema->num_fields());
    for (int i = 0; i < final_schema->num_fields(); i++) {
      empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(
          std::vector<std::shared_ptr<arrow::Array>>{}));
    }
    return arrow::Table::Make(final_schema, empty_columns);
  }

  // Create builders for each field in the schema
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : final_schema->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::INT64:
        builders.push_back(std::make_unique<arrow::Int64Builder>());
        break;
      case arrow::Type::STRING:
        builders.push_back(std::make_unique<arrow::StringBuilder>());
        break;
      default:
        return arrow::Status::NotImplemented("Unsupported type: ",
                                             field->type()->ToString());
    }
  }

  // Process nodes in chunks
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> chunks_per_field(
      final_schema->num_fields());
  size_t nodes_in_current_chunk = 0;

  for (const auto& node : nodes) {
    // explicitly add id field
    ARROW_RETURN_NOT_OK(
        static_cast<arrow::Int64Builder*>(builders[0].get())->Append(node->id));
    // For each field, extract the value from the node and append to the builder
    // start from 1 to skip "id" field
    for (int i = 1; i < final_schema->num_fields(); i++) {
      const auto& field = final_schema->field(i);
      auto field_result = node->get_field(field->name());
      if (!field_result.ok()) {
        // Field not present, append null
        ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
      } else {
        auto array = field_result.ValueOrDie();
        if (array->length() == 0 || array->IsNull(0)) {
          ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
        } else {
          // Append the value based on the field type
          switch (field->type()->id()) {
            case arrow::Type::INT64: {
              auto int_array =
                  std::static_pointer_cast<arrow::Int64Array>(array);
              ARROW_RETURN_NOT_OK(
                  static_cast<arrow::Int64Builder*>(builders[i].get())
                      ->Append(int_array->Value(0)));
              break;
            }
            case arrow::Type::STRING: {
              auto str_array =
                  std::static_pointer_cast<arrow::StringArray>(array);
              if (str_array->length() > 0 && !str_array->IsNull(0)) {
                ARROW_RETURN_NOT_OK(
                    static_cast<arrow::StringBuilder*>(builders[i].get())
                        ->Append(str_array->GetString(0)));
              } else {
                ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
              }
              break;
            }
            default:
              return arrow::Status::NotImplemented("Unsupported type: ",
                                                   field->type()->ToString());
          }
        }
      }
    }

    nodes_in_current_chunk++;

    // If we've reached the chunk size, finalize the arrays and reset the
    // builders
    if (nodes_in_current_chunk >= chunk_size) {
      for (int i = 0; i < final_schema->num_fields(); i++) {
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builders[i]->Finish(&array));
        chunks_per_field[i].push_back(array);
        builders[i]->Reset();
      }
      nodes_in_current_chunk = 0;
    }
  }

  // Finalize any remaining data
  if (nodes_in_current_chunk > 0) {
    for (int i = 0; i < final_schema->num_fields(); i++) {
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&array));
      chunks_per_field[i].push_back(array);
    }
  }

  // Create chunked arrays for each field
  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
  chunked_arrays.reserve(final_schema->num_fields());
  for (int i = 0; i < final_schema->num_fields(); i++) {
    chunked_arrays.push_back(
        std::make_shared<arrow::ChunkedArray>(chunks_per_field[i]));
  }

  // Create and return the table
  return arrow::Table::Make(final_schema, chunked_arrays);
}

// Contextual ValueOrDie helper that logs and provides context on errors
template <typename T>
T ValueOrDieWithContext(
    const arrow::Result<T>& result, const std::string& context,
    const std::source_location& location = std::source_location::current()) {
  if (!result.ok()) {
    // Extract filename from path (remove directory)
    std::string_view path(location.file_name());
    size_t pos = path.find_last_of("/\\");
    std::string_view filename =
        (pos == std::string_view::npos) ? path : path.substr(pos + 1);

    // Log failure with context and location
    std::string error_msg = "Operation failed [" + context + "] - " +
                            result.status().ToString() + " [at " +
                            std::string(filename) + ":" +
                            std::to_string(location.line()) + "]";
    log_error(error_msg);

    // Still use the original ValueOrDie behavior
    return result.ValueOrDie();
  }
  return result.ValueOrDie();
}

// Macro for easier usage that automatically includes function name
#define VALUE_OR_DIE_CTX(result, context) \
  ValueOrDieWithContext((result), (context))
}  // namespace tundradb

#endif  // UTILS_HPP
