#include "query.hpp"

#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>

#include "logger.hpp"

namespace tundradb {

// ================== SchemaRef Implementation ==================

SchemaRef SchemaRef::parse(const std::string& s) {
  SchemaRef r;
  size_t pos = s.find(':');
  if (pos == std::string::npos) {
    r.schema_ = s;
    r.value_ = s;
    r.declaration_ = false;
  } else {
    r.value_ = s.substr(0, pos);
    r.schema_ = s.substr(pos + 1);
    r.declaration_ = true;
  }
  return r;
}

std::string SchemaRef::toString() const {
  std::stringstream ss;
  if (declaration_) {
    ss << value_;
    ss << ":";
  }
  ss << schema_;
  return ss.str();
}

// ================== FieldRef Implementation ==================

FieldRef FieldRef::from_string(const std::string& field_str) {
  const size_t dot_pos = field_str.find('.');
  if (dot_pos != std::string::npos) {
    std::string variable = field_str.substr(0, dot_pos);
    std::string field_name = field_str.substr(dot_pos + 1);

    // Return unresolved FieldRef - will be resolved later in query processing
    return {variable, field_name};
  }
  // No variable prefix, treat entire string as field name
  return {"", field_str};
}

// ================== ComparisonExpr Implementation ==================

ComparisonExpr::ComparisonExpr(const std::string& field, CompareOp op,
                               Value value)
    : field_ref_(FieldRef::from_string(field)),
      op_(op),
      value_(std::move(value)) {}

arrow::Result<bool> ComparisonExpr::compare_values(const Value& value,
                                                   CompareOp op,
                                                   const Value& where_value) {
  if (value.type() == ValueType::NA || where_value.type() == ValueType::NA) {
    switch (op) {
      case CompareOp::Eq:
        return value.type() == ValueType::NA &&
               where_value.type() == ValueType::NA;
      case CompareOp::NotEq:
        return value.type() != ValueType::NA ||
               where_value.type() != ValueType::NA;
      default:
        return arrow::Status::Invalid(
            "Null values can only be compared with == or !=");
    }
  }

  if (op == CompareOp::Contains || op == CompareOp::StartsWith ||
      op == CompareOp::EndsWith) {
    if (value.type() != ValueType::STRING ||
        where_value.type() != ValueType::STRING) {
      return arrow::Status::Invalid(
          "String operations (CONTAINS, STARTS_WITH, ENDS_WITH) can only be "
          "applied to string values");
    }
  }

  if (value.type() == ValueType::BOOL ||
      where_value.type() == ValueType::BOOL) {
    if (value.type() != ValueType::BOOL ||
        where_value.type() != ValueType::BOOL) {
      return arrow::Status::Invalid(
          "Boolean values can only be compared with other boolean values");
    }
    if (op != CompareOp::Eq && op != CompareOp::NotEq) {
      return arrow::Status::Invalid(
          "Boolean values can only be compared with == or !=");
    }
  }

  if (value.type() != where_value.type()) {
    return arrow::Status::Invalid("Type mismatch: field is ", value.type(),
                                  " but WHERE value is ", where_value.type());
  }

  switch (value.type()) {
    case ValueType::INT32: {
      int32_t field_val = value.get<int32_t>();
      int32_t where_val = where_value.get<int32_t>();
      return apply_comparison(field_val, op, where_val);
    }
    case ValueType::INT64: {
      int64_t field_val = value.get<int64_t>();
      int64_t where_val = where_value.get<int64_t>();
      return apply_comparison(field_val, op, where_val);
    }
    case ValueType::FLOAT: {
      float field_val = value.get<float>();
      float where_val = where_value.get<float>();
      return apply_comparison(field_val, op, where_val);
    }
    case ValueType::DOUBLE: {
      double field_val = value.get<double>();
      double where_val = where_value.get<double>();
      return apply_comparison(field_val, op, where_val);
    }
    case ValueType::STRING: {
      const std::string& field_val = value.as_string();
      const std::string& where_val = where_value.as_string();
      return apply_comparison(field_val, op, where_val);
    }
    case ValueType::BOOL: {
      bool field_val = value.get<bool>();
      bool where_val = where_value.get<bool>();
      return apply_comparison(field_val, op, where_val);
    }
    case ValueType::NA:
      return arrow::Status::Invalid("Unexpected null value in comparison");
    default:
      return arrow::Status::NotImplemented(
          "Unsupported value type for comparison: ", value.type());
  }
}

template <typename T>
bool ComparisonExpr::apply_comparison(const T& field_val, CompareOp op,
                                      const T& where_val) {
  switch (op) {
    case CompareOp::Eq:
      return field_val == where_val;
    case CompareOp::NotEq:
      return field_val != where_val;
    case CompareOp::Gt:
      return field_val > where_val;
    case CompareOp::Lt:
      return field_val < where_val;
    case CompareOp::Gte:
      return field_val >= where_val;
    case CompareOp::Lte:
      return field_val <= where_val;
    case CompareOp::Contains:
      if constexpr (std::is_same_v<T, std::string>) {
        return field_val.contains(where_val);
      } else {
        return false;
      }
    case CompareOp::StartsWith:
      if constexpr (std::is_same_v<T, std::string>) {
        return field_val.starts_with(where_val);
      } else {
        return false;
      }
    case CompareOp::EndsWith:
      if constexpr (std::is_same_v<T, std::string>) {
        return field_val.ends_with(where_val);
      } else {
        return false;
      }
  }
  return false;
}

// Explicit template instantiations
template bool ComparisonExpr::apply_comparison<int32_t>(const int32_t&,
                                                        CompareOp,
                                                        const int32_t&);
template bool ComparisonExpr::apply_comparison<int64_t>(const int64_t&,
                                                        CompareOp,
                                                        const int64_t&);
template bool ComparisonExpr::apply_comparison<float>(const float&, CompareOp,
                                                      const float&);
template bool ComparisonExpr::apply_comparison<double>(const double&, CompareOp,
                                                       const double&);
template bool ComparisonExpr::apply_comparison<std::string>(const std::string&,
                                                            CompareOp,
                                                            const std::string&);
template bool ComparisonExpr::apply_comparison<bool>(const bool&, CompareOp,
                                                     const bool&);

template <typename T>
bool ComparisonExpr::apply_comparison(const T& field_val, const T& where_val,
                                      CompareOp op) {
  switch (op) {
    case CompareOp::Eq:
      return field_val == where_val;
    case CompareOp::NotEq:
      return field_val != where_val;
    case CompareOp::Gt:
      return field_val > where_val;
    case CompareOp::Lt:
      return field_val < where_val;
    case CompareOp::Gte:
      return field_val >= where_val;
    case CompareOp::Lte:
      return field_val <= where_val;
    case CompareOp::Contains:
      if constexpr (std::is_same_v<T, std::string>) {
        return field_val.find(where_val) != std::string::npos;
      } else {
        return false;
      }
    case CompareOp::StartsWith:
      if constexpr (std::is_same_v<T, std::string>) {
        return field_val.find(where_val) == 0;
      } else {
        return false;
      }
    case CompareOp::EndsWith:
      if constexpr (std::is_same_v<T, std::string>) {
        return field_val.size() >= where_val.size() &&
               field_val.substr(field_val.size() - where_val.size()) ==
                   where_val;
      } else {
        return false;
      }
  }
  return false;
}

// Explicit template instantiations for the second overload
template bool ComparisonExpr::apply_comparison<int32_t>(const int32_t&,
                                                        const int32_t&,
                                                        CompareOp);
template bool ComparisonExpr::apply_comparison<int64_t>(const int64_t&,
                                                        const int64_t&,
                                                        CompareOp);
template bool ComparisonExpr::apply_comparison<float>(const float&,
                                                      const float&, CompareOp);
template bool ComparisonExpr::apply_comparison<double>(const double&,
                                                       const double&,
                                                       CompareOp);
template bool ComparisonExpr::apply_comparison<std::string>(const std::string&,
                                                            const std::string&,
                                                            CompareOp);
template bool ComparisonExpr::apply_comparison<bool>(const bool&, const bool&,
                                                     CompareOp);

std::string ComparisonExpr::toString() const {
  std::stringstream ss;
  ss << "WHERE " << field_ref_.to_string();

  switch (op_) {
    case CompareOp::Eq:
      ss << " = ";
      break;
    case CompareOp::NotEq:
      ss << " != ";
      break;
    case CompareOp::Gt:
      ss << " > ";
      break;
    case CompareOp::Lt:
      ss << " < ";
      break;
    case CompareOp::Gte:
      ss << " >= ";
      break;
    case CompareOp::Lte:
      ss << " <= ";
      break;
    case CompareOp::Contains:
      ss << " CONTAINS ";
      break;
    case CompareOp::StartsWith:
      ss << " STARTS_WITH ";
      break;
    case CompareOp::EndsWith:
      ss << " ENDS_WITH ";
      break;
  }

  switch (value_.type()) {
    case ValueType::NA:
      ss << "NULL";
      break;
    case ValueType::INT32:
      ss << value_.get<int32_t>();
      break;
    case ValueType::INT64:
      ss << value_.get<int64_t>();
      break;
    case ValueType::FLOAT:
      ss << value_.get<float>();
      break;
    case ValueType::DOUBLE:
      ss << value_.get<double>();
      break;
    case ValueType::BOOL:
      ss << (value_.get<bool>() ? "true" : "false");
      break;
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
    case ValueType::STRING:
      ss << "'" << value_.to_string() << "'";
      break;
  }

  if (inlined_) {
    ss << " (inlined)";
  }

  return ss.str();
}

std::ostream& operator<<(std::ostream& os, const ComparisonExpr& expr) {
  os << expr.toString();
  return os;
}

arrow::Result<bool> ComparisonExpr::matches(
    const std::shared_ptr<Node>& node) const {
  if (!node) {
    return arrow::Status::Invalid("Node is null");
  }
  assert(field_ref_.field() != nullptr);
  ARROW_ASSIGN_OR_RAISE(auto field_value, node->get_value(field_ref_.field()));
  return compare_values(field_value, op_, value_);
}

arrow::compute::Expression ComparisonExpr::to_arrow_expression(
    bool strip_var) const {
  std::string field_name =
      strip_var ? field_ref_.field_name() : field_ref_.value();
  const auto field_expr = arrow::compute::field_ref(field_name);
  const auto value_expr = value_to_expression(value_);

  return apply_comparison_op(field_expr, value_expr, op_);
}

std::vector<std::shared_ptr<ComparisonExpr>>
ComparisonExpr::get_conditions_for_variable(const std::string& variable) const {
  if (field_ref_.variable() == variable) {
    return {std::make_shared<ComparisonExpr>(*this)};
  }
  return {};
}

bool ComparisonExpr::can_inline(const std::string& variable) const {
  return field_ref_.variable() == variable;
}

std::string ComparisonExpr::extract_first_variable() const {
  return field_ref_.variable();
}

std::set<std::string> ComparisonExpr::get_all_variables() const {
  std::set<std::string> variables;
  variables.insert(field_ref_.variable());
  return variables;
}

arrow::Result<bool> ComparisonExpr::resolve_field_ref(
    const std::unordered_map<std::string, std::string>& aliases,
    const SchemaRegistry* schema_registry) {
  if (field_ref_.is_resolved()) {
    return true;
  }

  const std::string& variable = field_ref_.variable();
  const std::string& field_name = field_ref_.field_name();

  // Find the actual schema for this variable
  auto it = aliases.find(variable);
  if (it == aliases.end()) {
    return arrow::Status::KeyError("Unknown variable '", variable,
                                   "' in field '", field_ref_.to_string(), "'");
  }

  const std::string& schema_name = it->second;

  auto schema_result = schema_registry->get(schema_name);
  if (!schema_result.ok()) {
    return arrow::Status::KeyError("Schema '", schema_name,
                                   "' not found for variable '", variable, "'");
  }

  auto schema = schema_result.ValueOrDie();
  auto field = schema->get_field(field_name);
  if (!field) {
    return arrow::Status::KeyError("Field '", field_name,
                                   "' not found in schema '", schema_name, "'");
  }
  field_ref_.resolve(field);

  return true;
}

// ================== LogicalExpr Implementation ==================

void LogicalExpr::set_inlined(bool inlined) {
  inlined_ = inlined;
  if (left_) left_->set_inlined(inlined);
  if (right_) right_->set_inlined(inlined);
}

arrow::Result<bool> LogicalExpr::resolve_field_ref(
    const std::unordered_map<std::string, std::string>& aliases,
    const SchemaRegistry* schema_registry) {
  if (left_) {
    if (const auto res = left_->resolve_field_ref(aliases, schema_registry);
        !res.ok()) {
      return res.status();
    }
  }
  if (right_) {
    if (const auto res = right_->resolve_field_ref(aliases, schema_registry);
        !res.ok()) {
      return res.status();
    }
  }
  return true;
}

std::shared_ptr<LogicalExpr> LogicalExpr::and_expr(
    std::shared_ptr<WhereExpr> left, std::shared_ptr<WhereExpr> right) {
  return std::make_shared<LogicalExpr>(std::move(left), LogicalOp::AND,
                                       std::move(right));
}

std::shared_ptr<LogicalExpr> LogicalExpr::or_expr(
    std::shared_ptr<WhereExpr> left, std::shared_ptr<WhereExpr> right) {
  return std::make_shared<LogicalExpr>(std::move(left), LogicalOp::OR,
                                       std::move(right));
}

arrow::Result<bool> LogicalExpr::matches(
    const std::shared_ptr<Node>& node) const {
  if (!left_ || !right_) {
    return arrow::Status::Invalid("LogicalExpr missing left or right operand");
  }

  auto left_result = left_->matches(node);
  if (!left_result.ok()) {
    return left_result.status();
  }

  auto right_result = right_->matches(node);
  if (!right_result.ok()) {
    return right_result.status();
  }

  bool left_val = left_result.ValueOrDie();
  bool right_val = right_result.ValueOrDie();

  switch (op_) {
    case LogicalOp::AND:
      return left_val && right_val;
    case LogicalOp::OR:
      return left_val || right_val;
  }

  return arrow::Status::Invalid("Unknown logical operator");
}

arrow::compute::Expression LogicalExpr::to_arrow_expression(
    bool strip_var) const {
  if (!left_ || !right_) {
    throw std::runtime_error("LogicalExpr missing left or right operand");
  }

  auto left_expr = left_->to_arrow_expression(strip_var);
  auto right_expr = right_->to_arrow_expression(strip_var);

  switch (op_) {
    case LogicalOp::AND:
      return arrow::compute::and_(left_expr, right_expr);
    case LogicalOp::OR:
      return arrow::compute::or_(left_expr, right_expr);
    default:
      throw std::runtime_error("Unknown logical operator in LogicalExpr");
  }
}

std::vector<std::shared_ptr<ComparisonExpr>>
LogicalExpr::get_conditions_for_variable(const std::string& variable) const {
  auto all_variables = get_all_variables();
  for (const auto& var : all_variables) {
    if (var != variable) {
      return {};
    }
  }

  std::vector<std::shared_ptr<ComparisonExpr>> result;
  if (left_) {
    auto left_conditions = left_->get_conditions_for_variable(variable);
    result.insert(result.end(), left_conditions.begin(), left_conditions.end());
  }
  if (right_) {
    auto right_conditions = right_->get_conditions_for_variable(variable);
    result.insert(result.end(), right_conditions.begin(),
                  right_conditions.end());
  }
  return result;
}

std::string LogicalExpr::extract_first_variable() const {
  if (left_) {
    auto var = left_->extract_first_variable();
    if (!var.empty()) return var;
  }
  if (right_) {
    auto var = right_->extract_first_variable();
    if (!var.empty()) return var;
  }
  return "";
}

std::string LogicalExpr::toString() const {
  if (!left_ || !right_) {
    return "WHERE (incomplete logical expression)";
  }

  std::string left_str = left_->toString();
  std::string right_str = right_->toString();

  if (left_str.substr(0, 6) == "WHERE ") {
    left_str = left_str.substr(6);
  }
  if (right_str.substr(0, 6) == "WHERE ") {
    right_str = right_str.substr(6);
  }

  std::string op_str = (op_ == LogicalOp::AND) ? " AND " : " OR ";

  std::string result =
      "WHERE (" + left_str + ")" + op_str + "(" + right_str + ")";

  if (inlined_) {
    result += " (inlined)";
  }

  return result;
}

std::ostream& operator<<(std::ostream& os, const LogicalExpr& expr) {
  os << expr.toString();
  return os;
}

std::set<std::string> LogicalExpr::get_all_variables() const {
  std::set<std::string> variables;
  if (left_) {
    auto left_variables = left_->get_all_variables();
    variables.insert(left_variables.begin(), left_variables.end());
  }
  if (right_) {
    auto right_variables = right_->get_all_variables();
    variables.insert(right_variables.begin(), right_variables.end());
  }
  return variables;
}

bool LogicalExpr::can_inline(const std::string& variable) const {
  if (left_ && !left_->can_inline(variable)) return false;
  if (right_ && !right_->can_inline(variable)) return false;
  return true;
}

// ================== Helper Functions ==================
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
