#include "utils.hpp"

#include <arrow/api.h>

#include <memory>
#include <string>

namespace tundradb {

// Implementation of stringifyArrowScalar
std::string stringifyArrowScalar(
    const std::shared_ptr<arrow::ChunkedArray>& column, const int64_t row_idx) {
  int chunk_idx = 0;
  int64_t chunk_row = row_idx;

  // Find the correct chunk
  while (chunk_idx < column->num_chunks() &&
         chunk_row >= column->chunk(chunk_idx)->length()) {
    chunk_row -= column->chunk(chunk_idx)->length();
    chunk_idx++;
  }

  if (chunk_idx >= column->num_chunks()) {
    return "ERR";
  }

  const auto chunk = column->chunk(chunk_idx);

  if (chunk->IsNull(chunk_row)) {
    return "null";
  }

  switch (column->type()->id()) {
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
      const auto string_array =
          std::static_pointer_cast<arrow::StringArray>(chunk);
      return "\"" + string_array->GetString(chunk_row) + "\"";
    }
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32: {
      const auto int_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
      return std::to_string(int_array->Value(chunk_row));
    }
    case arrow::Type::INT64: {
      const auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
      return std::to_string(int_array->Value(chunk_row));
    }
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE: {
      const auto double_array =
          std::static_pointer_cast<arrow::DoubleArray>(chunk);
      return std::to_string(double_array->Value(chunk_row));
    }
    case arrow::Type::BOOL: {
      const auto bool_array =
          std::static_pointer_cast<arrow::BooleanArray>(chunk);
      return bool_array->Value(chunk_row) ? "true" : "false";
    }
    default:
      return "Unsupported type";
  }
}

}  // namespace tundradb