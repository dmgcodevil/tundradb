#ifndef UTILS_HPP
#define UTILS_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseSet.h>
#include <uuid/uuid.h>

#include <iostream>
#include <set>
#include <source_location>
#include <string>
#include <unordered_set>

#include "logger.hpp"
#include "node.hpp"
#include "query.hpp"
#include "types.hpp"

namespace tundradb {

template <class SetA, class SetB, class OutSet>
void dense_intersection(const SetA& a, const SetB& b, OutSet& out) {
  const auto& small = a.size() < b.size() ? a : b;
  const auto& large = a.size() < b.size() ? b : a;
  out.clear();
  out.reserve(std::min(a.size(), b.size()));
  for (const auto& x : small) {
    if (large.contains(x)) {
      out.insert(x);
    }
  }
}

template <class SetA, class SetB, class OutSet>
void dense_difference(const SetA& a, const SetB& b, OutSet& out) {
  out.clear();
  out.reserve(a.size());
  for (const auto& x : a) {
    if (!b.contains(x)) {
      out.insert(x);
    }
  }
}

static std::string generate_uuid() {
  uuid_t uuid;
  uuid_generate(uuid);
  char uuid_str[37];
  uuid_unparse_lower(uuid, uuid_str);
  return uuid_str;
}

static int64_t generate_unique_snapshot_id() {
  static std::atomic<int32_t> counter{0};
  auto now = std::chrono::system_clock::now();
  int64_t timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             now.time_since_epoch())
                             .count();

  // Use upper 32 bits for timestamp, lower 32 bits for counter
  // This gives us ~49 days before timestamp rollover in the upper bits
  int32_t count = counter.fetch_add(1);
  return (timestamp_ms << 32) | (count & 0xFFFFFFFF);
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
      auto status =
          filter_builder.Append(filter_ids.count(id_array->Value(i)) > 0);
      if (!status.ok()) {
        return status;
      }
    }
  }

  std::shared_ptr<arrow::Array> filter_array;
  ARROW_RETURN_NOT_OK(filter_builder.Finish(&filter_array));

  ARROW_ASSIGN_OR_RAISE(auto combined_table,
                        table->CombineChunks(arrow::default_memory_pool()));

  ARROW_ASSIGN_OR_RAISE(
      auto filtered_table,
      arrow::compute::Filter(combined_table, arrow::Datum(filter_array)));

  return filtered_table.table();
}

static arrow::Result<std::shared_ptr<arrow::Table>> create_table(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes, size_t chunk_size,
    TemporalContext* temporal_context) {
  auto arrow_schema = schema->arrow();
  if (nodes.empty()) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(arrow_schema->num_fields());
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(
          std::vector<std::shared_ptr<arrow::Array>>{}));
    }
    return arrow::Table::Make(arrow_schema, empty_columns);
  }

  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : arrow_schema->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::INT32:
        builders.push_back(std::make_unique<arrow::Int32Builder>());
        break;
      case arrow::Type::INT64:
        builders.push_back(std::make_unique<arrow::Int64Builder>());
        break;
      case arrow::Type::FLOAT:
        builders.push_back(std::make_unique<arrow::FloatBuilder>());
        break;
      case arrow::Type::DOUBLE:
        builders.push_back(std::make_unique<arrow::DoubleBuilder>());
        break;
      case arrow::Type::STRING:
        builders.push_back(std::make_unique<arrow::StringBuilder>());
        break;
      case arrow::Type::BOOL:
        builders.push_back(std::make_unique<arrow::BooleanBuilder>());
        break;
      default:
        return arrow::Status::NotImplemented("Unsupported type: ",
                                             field->type()->ToString());
    }
  }

  std::vector<std::vector<std::shared_ptr<arrow::Array>>> chunks_per_field(
      arrow_schema->num_fields());
  size_t nodes_in_current_chunk = 0;

  for (const auto& node : nodes) {
    // Use NodeView to read from version chain (supports temporal queries)
    auto view = node->view(temporal_context);

    // Skip nodes that are not visible at this temporal snapshot
    if (!view.is_visible()) {
      continue;  // Node doesn't exist at queried time
    }

    for (int i = 0; i < schema->num_fields(); i++) {
      const auto& field = schema->field(i);
      auto field_result = view.get_value_ptr(field);  // ✅ Use NodeView!
      if (!field_result.ok()) {
        ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
      } else {
        const auto value_ptr = field_result.ValueOrDie();
        if (value_ptr == nullptr) {
          ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
        } else {
          switch (field->type()) {
            case ValueType::INT32: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::Int32Builder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const int32_t*>(value_ptr)));
              break;
            }
            case ValueType::INT64: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::Int64Builder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const int64_t*>(value_ptr)));
              break;
            }
            case ValueType::FLOAT: {
              //         return Value{*reinterpret_cast<const double*>(ptr)};
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::FloatBuilder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const float*>(value_ptr)));
              break;
            }
            case ValueType::DOUBLE: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::DoubleBuilder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const double*>(value_ptr)));
              break;
            }
            case ValueType::BOOL: {
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::BooleanBuilder*>(builders[i].get())
                      ->Append(*reinterpret_cast<const bool*>(value_ptr)));
              break;
            }
            case ValueType::STRING: {
              auto str_ref = *reinterpret_cast<const StringRef*>(value_ptr);

              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::StringBuilder*>(builders[i].get())
                      ->Append(str_ref.to_string()));
              break;
            }
            default:
              return arrow::Status::NotImplemented("Unsupported type: ",
                                                   to_string(field->type()));
          }
        }
      }
    }

    nodes_in_current_chunk++;

    if (nodes_in_current_chunk >= chunk_size) {
      for (int i = 0; i < arrow_schema->num_fields(); i++) {
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builders[i]->Finish(&array));
        chunks_per_field[i].push_back(array);
        builders[i]->Reset();
      }
      nodes_in_current_chunk = 0;
    }
  }

  if (nodes_in_current_chunk > 0) {
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&array));
      chunks_per_field[i].push_back(array);
    }
  }

  // Handle case where all nodes were filtered out by temporal visibility
  // Need to create empty arrays for each field
  if (chunks_per_field[0].empty()) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(arrow_schema->num_fields());
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      // Create empty array of correct type
      std::shared_ptr<arrow::Array> empty_array;
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&empty_array));
      empty_columns.push_back(
          std::make_shared<arrow::ChunkedArray>(empty_array));
    }
    return arrow::Table::Make(arrow_schema, empty_columns);
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
  chunked_arrays.reserve(arrow_schema->num_fields());
  for (int i = 0; i < arrow_schema->num_fields(); i++) {
    chunked_arrays.push_back(
        std::make_shared<arrow::ChunkedArray>(chunks_per_field[i]));
  }

  return arrow::Table::Make(arrow_schema, chunked_arrays);
}

static void print_row(const std::shared_ptr<arrow::Table>& table,
                      const int64_t row_index) {
  for (int j = 0; j < table->num_columns(); ++j) {
    const auto column = table->column(j);
    if (!column || column->num_chunks() == 0) {
      std::cout << "NULL\t";
      continue;
    }

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
        } catch ([[maybe_unused]] const std::exception& e) {
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
    bool use_ellipsis = max_rows > 0 && total_rows > max_rows;
    int64_t rows_to_print = use_ellipsis ? max_rows / 2 : total_rows;

    std::cout << "First " << rows_to_print << " rows:" << std::endl;
    for (int64_t i = 0; i < rows_to_print && i < total_rows; ++i) {
      print_row(table, i);
    }

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

template <typename T>
T ValueOrDieWithContext(
    const arrow::Result<T>& result, const std::string& context,
    const std::source_location& location = std::source_location::current()) {
  if (!result.ok()) {
    std::string_view path(location.file_name());
    size_t pos = path.find_last_of("/\\");
    std::string_view filename =
        (pos == std::string_view::npos) ? path : path.substr(pos + 1);
    std::string error_msg = "Operation failed [" + context + "] - " +
                            result.status().ToString() + " [at " +
                            std::string(filename) + ":" +
                            std::to_string(location.line()) + "]";
    log_error(error_msg);

    return result.ValueOrDie();
  }
  return result.ValueOrDie();
}

#define VALUE_OR_DIE_CTX(result, context) \
  ValueOrDieWithContext((result), (context))

std::string stringify_arrow_scalar(
    const std::shared_ptr<arrow::ChunkedArray>& column, int64_t row_idx);

template <typename T>
arrow::Result<std::vector<T>> get_column_values(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& column_name) {
  const auto column = table->GetColumnByName(column_name);
  if (!column) {
    return arrow::Status::Invalid("Column '", column_name, "' not found");
  }

  std::vector<T> values;
  values.reserve(table->num_rows());

  for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
    auto chunk = column->chunk(chunk_idx);

    if constexpr (std::is_same_v<T, int32_t>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::Int32Array>(chunk);
      for (int32_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    } else if constexpr (std::is_same_v<T, int64_t>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::Int64Array>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::StringArray>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->GetString(i));
        }
      }
    } else if constexpr (std::is_same_v<T, double>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::DoubleArray>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    } else if constexpr (std::is_same_v<T, bool>) {
      const auto typed_array =
          std::static_pointer_cast<arrow::BooleanArray>(chunk);
      for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (!typed_array->IsNull(i)) {
          values.push_back(typed_array->Value(i));
        }
      }
    }
  }

  return values;
}

inline arrow::Result<std::shared_ptr<arrow::Array>> get_column_as_array(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& column_name) {
  const auto column = table->GetColumnByName(column_name);
  if (!column) {
    return arrow::Status::Invalid("Column '", column_name, "' not found");
  }

  ARROW_ASSIGN_OR_RAISE(auto combined_array,
                        arrow::Concatenate(column->chunks()));

  return combined_array;
}

template <typename T>
arrow::Result<T> get_first_value_from_array(
    const std::shared_ptr<arrow::Array>& array) {
  if (!array || array->length() == 0) {
    return arrow::Status::Invalid("Array is null or empty");
  }

  if (array->IsNull(0)) {
    return arrow::Status::Invalid("First value is null");
  }

  if constexpr (std::is_same_v<T, int64_t>) {
    if (array->type_id() != arrow::Type::INT64) {
      return arrow::Status::Invalid("Expected Int64 array, got: ",
                                    array->type()->ToString());
    }
    const auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
    return typed_array->Value(0);
  } else if constexpr (std::is_same_v<T, std::string>) {
    if (array->type_id() != arrow::Type::STRING) {
      return arrow::Status::Invalid("Expected String array, got: ",
                                    array->type()->ToString());
    }
    const auto typed_array =
        std::static_pointer_cast<arrow::StringArray>(array);
    return typed_array->GetString(0);
  } else if constexpr (std::is_same_v<T, double>) {
    if (array->type_id() != arrow::Type::DOUBLE) {
      return arrow::Status::Invalid("Expected Double array, got: ",
                                    array->type()->ToString());
    }
    const auto typed_array =
        std::static_pointer_cast<arrow::DoubleArray>(array);
    return typed_array->Value(0);
  } else if constexpr (std::is_same_v<T, bool>) {
    if (array->type_id() != arrow::Type::BOOL) {
      return arrow::Status::Invalid("Expected Boolean array, got: ",
                                    array->type()->ToString());
    }
    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
    return typed_array->Value(0);
  } else {
    return arrow::Status::NotImplemented("Unsupported type");
  }
}

inline arrow::Result<int64_t> get_first_int64(
    const std::shared_ptr<arrow::Array>& array) {
  return get_first_value_from_array<int64_t>(array);
}

inline arrow::Result<std::string> get_first_string(
    const std::shared_ptr<arrow::Array>& array) {
  return get_first_value_from_array<std::string>(array);
}

inline arrow::Result<bool> apply_where_to_node(
    const std::shared_ptr<WhereExpr>& where_expr,
    const std::shared_ptr<Node>& node) {
  if (!where_expr) {
    return arrow::Status::Invalid("WHERE expression is null");
  }

  return where_expr->matches(node);
}

inline arrow::Result<std::vector<std::shared_ptr<Node>>> filter_nodes_by_where(
    const std::vector<std::shared_ptr<Node>>& nodes,
    const std::shared_ptr<WhereExpr>& where_expr) {
  if (!where_expr) {
    return arrow::Status::Invalid("WHERE expression is null");
  }

  std::vector<std::shared_ptr<Node>> filtered_nodes;
  filtered_nodes.reserve(nodes.size());

  for (const auto& node : nodes) {
    ARROW_ASSIGN_OR_RAISE(bool matches, where_expr->matches(node));
    if (matches) {
      filtered_nodes.push_back(node);
    }
  }

  return filtered_nodes;
}
}  // namespace tundradb

#endif  // UTILS_HPP
