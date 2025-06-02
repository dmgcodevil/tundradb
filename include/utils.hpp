#ifndef UTILS_HPP
#define UTILS_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <uuid/uuid.h>

#include <iostream>
#include <set>
#include <source_location>
#include <string>
#include <unordered_set>

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

template <typename SetType>
static arrow::Result<std::shared_ptr<arrow::Table>> filter_table_by_id(
    const std::shared_ptr<arrow::Table>& table, const SetType& filter_ids) {
  std::shared_ptr<arrow::ChunkedArray> id_chunked_array =
      table->GetColumnByName("id");
  if (!id_chunked_array) {
    return arrow::Status::Invalid("Column 'id' not found");
  }

  arrow::BooleanBuilder filter_builder;

  for (const auto& chunk : id_chunked_array->chunks()) {
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(chunk);

    for (int64_t i = 0; i < id_array->length(); ++i) {
      filter_builder.Append(filter_ids.count(id_array->Value(i)) > 0);
    }
  }

  std::shared_ptr<arrow::Array> filter_array;
  ARROW_RETURN_NOT_OK(filter_builder.Finish(&filter_array));

  // Flatten table into a single chunk to match the filter
  ARROW_ASSIGN_OR_RAISE(auto combined_table,
                        table->CombineChunks(arrow::default_memory_pool()));

  // Apply filter
  ARROW_ASSIGN_OR_RAISE(
      auto filtered_table,
      arrow::compute::Filter(combined_table, arrow::Datum(filter_array)));

  return filtered_table.table();
}

static arrow::Result<std::set<int64_t>> get_ids_from_table(
    std::shared_ptr<arrow::Table> table) {
  log_debug("Extracting IDs from table with {} rows", table->num_rows());

  auto id_idx = table->schema()->GetFieldIndex("id");
  if (id_idx == -1) {
    log_error("Table does not have an 'id' column");
    return arrow::Status::Invalid("table does not have an 'id' column");
  }

  auto id_column = table->column(id_idx);
  std::set<int64_t> result_ids;

  for (int chunk_idx = 0; chunk_idx < id_column->num_chunks(); chunk_idx++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(
        id_column->chunk(chunk_idx));
    log_debug("Processing chunk {} with {} rows", chunk_idx, chunk->length());
    for (int i = 0; i < chunk->length(); i++) {
      result_ids.insert(chunk->Value(i));
    }
  }

  log_debug("Extracted {} unique IDs from table", result_ids.size());
  return result_ids;
}

// Create a table from a schema and a list of nodes
static arrow::Result<std::shared_ptr<arrow::Table>> create_table(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes, size_t chunk_size) {
  auto final_schema = schema;  // prepend_id_field(schema);
  // log_debug("Creating table. schema: {}", schema->ToString());
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
    // ARROW_RETURN_NOT_OK(
    //     static_cast<arrow::Int64Builder*>(builders[0].get())->Append(node->id));
    // For each field, extract the value from the node and append to the builder
    // start from 1 to skip "id" field
    for (int i = 0; i < final_schema->num_fields(); i++) {
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

// Helper function to print a single row
static void print_row(const std::shared_ptr<arrow::Table>& table,
                      int64_t row_index) {
  for (int j = 0; j < table->num_columns(); ++j) {
    auto column = table->column(j);
    if (!column || column->num_chunks() == 0) {
      std::cout << "NULL\t";
      continue;
    }

    // Find the chunk containing this row
    int64_t accumulated_length = 0;
    std::shared_ptr<arrow::Array> chunk;
    int64_t chunk_offset = row_index;

    for (int c = 0; c < column->num_chunks(); ++c) {
      chunk = column->chunk(c);
      if (accumulated_length + chunk->length() > row_index) {
        chunk_offset = row_index - accumulated_length;
        break;
      }
      accumulated_length += chunk->length();
    }

    if (!chunk || chunk_offset >= chunk->length() ||
        chunk->IsNull(chunk_offset)) {
      std::cout << "NULL\t";
      continue;
    }

    switch (chunk->type_id()) {
      case arrow::Type::INT32:
        std::cout << std::static_pointer_cast<arrow::Int32Array>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::INT64:
        std::cout << std::static_pointer_cast<arrow::Int64Array>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::FLOAT:
        std::cout << std::static_pointer_cast<arrow::FloatArray>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::DOUBLE:
        std::cout << std::static_pointer_cast<arrow::DoubleArray>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::BOOL:
        std::cout << (std::static_pointer_cast<arrow::BooleanArray>(chunk)
                              ->Value(chunk_offset)
                          ? "true"
                          : "false");
        break;
      case arrow::Type::STRING:
        try {
          auto str_array = std::static_pointer_cast<arrow::StringArray>(chunk);
          if (chunk_offset < str_array->length()) {
            std::cout << str_array->GetString(chunk_offset);
          } else {
            std::cout << "NULL";
          }
        } catch (const std::exception& e) {
          std::cout << "ERROR";
        }
        break;
      case arrow::Type::TIMESTAMP:
        std::cout << std::static_pointer_cast<arrow::TimestampArray>(chunk)
                         ->Value(chunk_offset);
        break;
      default:
        std::cout << "Unsupported";
    }
    std::cout << "\t";
  }
  std::cout << std::endl;
}

static void print_table(const std::shared_ptr<arrow::Table>& table,
                        int64_t max_rows = 100) {
  if (!table) {
    std::cout << "Null table" << std::endl;
    return;
  }

  std::cout << "Table Schema:" << std::endl;
  std::cout << table->schema()->ToString() << std::endl;

  // Print chunk information
  std::cout << "\nChunk Information:" << std::endl;
  for (int j = 0; j < table->num_columns(); ++j) {
    auto column = table->column(j);
    std::cout << "Column '" << table->schema()->field(j)->name()
              << "': " << column->num_chunks();

    if (column->num_chunks() > 0) {
      std::cout << " chunk sizes = [ ";
      for (int c = 0; c < column->num_chunks(); c++) {
        std::cout << column->chunk(c)->length();
        if (c < column->num_chunks() - 1) std::cout << ", ";
      }
      std::cout << " ]";
    }
    std::cout << std::endl;
  }

  const int64_t total_rows = table->num_rows();
  std::cout << "\nTable Data (" << total_rows << " rows):" << std::endl;

  try {
    // Determine how many rows to print
    bool use_ellipsis = max_rows > 0 && total_rows > max_rows;
    int64_t rows_to_print = use_ellipsis ? max_rows / 2 : total_rows;

    std::cout << "First " << rows_to_print << " rows:" << std::endl;
    // Print first half of rows
    for (int64_t i = 0; i < rows_to_print && i < total_rows; ++i) {
      print_row(table, i);
    }

    // Print ellipsis and last half of rows if needed
    if (use_ellipsis) {
      std::cout << "....\n" << std::endl;

      std::cout << "Last " << rows_to_print << " rows:" << std::endl;
      for (int64_t i = total_rows - rows_to_print; i < total_rows; ++i) {
        print_row(table, i);
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Error while printing table: " << e.what() << std::endl;
  }
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

/**
 * Convert an Arrow scalar value to a string representation
 */
std::string stringifyArrowScalar(
    const std::shared_ptr<arrow::ChunkedArray>& column, int64_t row_idx);
}  // namespace tundradb

#endif  // UTILS_HPP
