#ifndef DEBUG_UTILS_HPP
#define DEBUG_UTILS_HPP

#include <arrow/api.h>

#include <exception>
#include <iostream>
#include <memory>

namespace tundradb {

/**
 * @brief Prints a single row of an Arrow table to stdout (tab-separated).
 *
 * @param table The table to read from.
 * @param row_index Zero-based row index to print.
 */
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

/**
 * @brief Prints an Arrow table to stdout (schema, chunk info, and data).
 *
 * If the table has more than @p max_rows rows, the first and last halves
 * are printed with an ellipsis in between.
 *
 * @param table The table to print.
 * @param max_rows Maximum rows to display (0 = unlimited).
 */
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

}  // namespace tundradb

#endif  // DEBUG_UTILS_HPP
