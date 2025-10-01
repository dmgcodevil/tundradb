#include "query.hpp"

namespace tundradb {

// FieldRef implementation
FieldRef FieldRef::from_string(const std::string& field_str) {
  const size_t dot_pos = field_str.find('.');
  if (dot_pos != std::string::npos) {
    std::string variable = field_str.substr(0, dot_pos);
    std::string field_name = field_str.substr(dot_pos + 1);

    // Return unresolved FieldRef - will be resolved later in query processing
    return {variable, field_name};
  } else {
    // No variable prefix, treat entire string as field name
    return {"", field_str};
  }
}

// Convert CompareOp to appropriate Arrow compute function
arrow::compute::Expression apply_comparison_op(
    const arrow::compute::Expression& field,
    const arrow::compute::Expression& value, CompareOp op) {
  switch (op) {
    case CompareOp::Eq:
      return arrow::compute::equal(field, value);
    case CompareOp::NotEq:
      return arrow::compute::not_equal(field, value);
    case CompareOp::Gt:
      return arrow::compute::greater(field, value);
    case CompareOp::Lt:
      return arrow::compute::less(field, value);
    case CompareOp::Gte:
      return arrow::compute::greater_equal(field, value);
    case CompareOp::Lte:
      return arrow::compute::less_equal(field, value);
    case CompareOp::Contains:
      // For string operations, we'd need to use match_substring_regex or
      // similar For now, fall back to equal (this would need more sophisticated
      // handling)
      log_warn(
          "CONTAINS operator not fully implemented for Arrow expressions, "
          "using equality");
      return arrow::compute::equal(field, value);
    case CompareOp::StartsWith:
      log_warn(
          "STARTS_WITH operator not fully implemented for Arrow expressions, "
          "using equality");
      return arrow::compute::equal(field, value);
    case CompareOp::EndsWith:
      log_warn(
          "ENDS_WITH operator not fully implemented for Arrow expressions, "
          "using equality");
      return arrow::compute::equal(field, value);
    default:
      throw std::runtime_error(
          "Unsupported comparison operator for Arrow expression");
  }
}

// Convert Value to Arrow compute scalar for expressions
arrow::compute::Expression value_to_expression(const Value& value) {
  switch (value.type()) {
    case ValueType::INT32:
      return arrow::compute::literal(value.get<int32_t>());
    case ValueType::INT64:
      return arrow::compute::literal(value.get<int64_t>());
    case ValueType::STRING:
      return arrow::compute::literal(value.get<std::string>());
    case ValueType::FLOAT:
      return arrow::compute::literal(value.get<float>());
    case ValueType::DOUBLE:
      return arrow::compute::literal(value.get<double>());
    case ValueType::BOOL:
      return arrow::compute::literal(value.get<bool>());
    case ValueType::NA:
      return arrow::compute::literal(
          arrow::Datum(arrow::MakeNullScalar(arrow::null())));
    default:
      throw std::runtime_error("Unsupported value type for Arrow expression");
  }
}

}  // namespace tundradb