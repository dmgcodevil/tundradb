#ifndef ARROW_UTILS_HPP
#define ARROW_UTILS_HPP

#include <arrow/api.h>

namespace tundradb {
static arrow::Result<std::shared_ptr<arrow::Array>> create_int64_array(
    const int64_t value) {
  arrow::Int64Builder int64_builder;
  ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
  ARROW_RETURN_NOT_OK(int64_builder.Append(value));
  std::shared_ptr<arrow::Array> int64_array;
  ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
  return int64_array;
}

static arrow::Result<std::shared_ptr<arrow::Array>> create_str_array(
    const std::string &value) {
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(value));
  std::shared_ptr<arrow::Array> string_arr;
  ARROW_RETURN_NOT_OK(builder.Finish(&string_arr));
  return string_arr;
}

static arrow::Result<std::shared_ptr<arrow::Array>> create_null_array(
    const std::shared_ptr<arrow::DataType> &type) {
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
}  // namespace tundradb

#endif  // ARROW_UTILS_HPP
