#ifndef QUERY_HPP
#define QUERY_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>

#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "node.hpp"
#include "types.hpp"

namespace tundradb {

struct SchemaRef {
 private:
  std::string schema_;
  std::string value_;
  bool declaration_;

 public:
  [[nodiscard]] std::string schema() const { return schema_; }
  [[nodiscard]] std::string value() const { return value_; }
  [[nodiscard]] bool is_declaration() const { return declaration_; }

  static SchemaRef parse(const std::string& s) {
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

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    if (declaration_) {
      ss << value_;
      ss << ":";
    }
    ss << schema_;
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os, const SchemaRef& obj) {
    os << obj.toString();
    return os;
  }
};

struct GraphConnection {
  SchemaRef source;
  int64_t source_id;
  std::string edge_type;
  std::string label;
  SchemaRef target;
  int64_t target_id;

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    ss << "{(" << source << ":id=" << source_id << "->[:" << edge_type << "]->"
       << "(" << label << ":" << target << ":id=" << target_id << ")}";
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os, const GraphConnection& c) {
    os << c.toString();
    return os;
  }
};

// Comparison operators
enum class CompareOp {
  Eq,
  NotEq,
  Gt,
  Lt,
  Gte,
  Lte,
  Contains,
  StartsWith,
  EndsWith
};

// Base class for all query clauses
class Clause {
 public:
  virtual ~Clause() = default;

  // Potential common functionality or type identification
  enum class Type { WHERE, TRAVERSE, PROJECT, ORDER_BY, LIMIT, SELECT };
  [[nodiscard]] virtual Type type() const = 0;
};

// Forward declaration for recursive structure
class WhereExpr;
class ComparisonExpr;

// Forward declare function from core.cpp
arrow::compute::Expression value_to_expression(const Value& value);
arrow::compute::Expression apply_comparison_op(
    const arrow::compute::Expression& field,
    const arrow::compute::Expression& value, CompareOp op);

enum class LogicalOp { AND, OR };

// Base class for WHERE expressions (ADT)
class WhereExpr {
 public:
  virtual ~WhereExpr() = default;
  virtual arrow::Result<bool> matches(
      const std::shared_ptr<Node>& node) const = 0;
  virtual std::string toString() const = 0;
  virtual void set_inlined(bool inlined) = 0;
  virtual bool inlined() const = 0;
  virtual arrow::compute::Expression to_arrow_expression() const = 0;

  // Collect all conditions for a specific variable (for inline optimization)
  virtual std::vector<std::shared_ptr<ComparisonExpr>>
  get_conditions_for_variable(const std::string& variable) const = 0;

  // Collect all variables referenced in this expression tree
  virtual std::set<std::string> get_all_variables() const = 0;

  // Extract the first variable name found in this expression tree
  virtual std::string extract_first_variable() const = 0;
};

class Where : public Clause {
 private:
  std::string field_;
  CompareOp op_;
  Value value_;
  bool inlined_ = false;

 public:
  Where(std::string field, CompareOp op, Value value)
      : field_(std::move(field)), op_(op), value_(std::move(value)) {}

  [[nodiscard]] Type type() const override { return Type::WHERE; }

  [[nodiscard]] const std::string& field() const { return field_; }
  [[nodiscard]] CompareOp op() const { return op_; }
  [[nodiscard]] const Value& value() const { return value_; }

  [[nodiscard]] bool inlined() const { return inlined_; }
  void set_inlined(bool p) { inlined_ = p; }

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    ss << "WHERE " << field_;

    // Convert operator to string
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

    // convert value to string based on its type
    switch (value_.type()) {
      case ValueType::Null:
        ss << "NULL";
        break;
      case ValueType::Int64:
        ss << value_.get<int64_t>();
        break;
      case ValueType::Double:
        ss << value_.get<double>();
        break;
      case ValueType::String:
        ss << "'" << value_.get<std::string>() << "'";
        break;
      case ValueType::Bool:
        ss << (value_.get<bool>() ? "true" : "false");
        break;
    }

    if (inlined_) {
      ss << " (inlined)";
    }

    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os, const Where& where) {
    os << where.toString();
    return os;
  }

  // Method to evaluate if a Node matches this WHERE condition
  arrow::Result<bool> matches(const std::shared_ptr<Node>& node) const {
    if (!node) {
      return arrow::Status::Invalid("Node is null");
    }

    // parse field name to extract variable and field parts
    // expected format: "variable.field" (e.g., "user.age", "company.name")
    size_t dot_pos = field_.find('.');
    std::string field_name;

    if (dot_pos != std::string::npos) {
      field_name = field_.substr(dot_pos + 1);
    } else {
      field_name = field_;
    }

    ARROW_ASSIGN_OR_RAISE(auto field_array, node->get_field(field_name));

    if (!field_array || field_array->length() == 0) {
      return arrow::Status::Invalid("Field '", field_name, "' is empty");
    }

    if (field_array->IsNull(0)) {
      switch (op_) {
        case CompareOp::Eq:
          return value_.type() == ValueType::Null;
        case CompareOp::NotEq:
          return value_.type() != ValueType::Null;
        default:
          return false;  // Most operators return false for null values
      }
    }
    return compare_values(field_array, value_, op_);
  }

 private:
  static arrow::Result<bool> compare_values(
      const std::shared_ptr<arrow::Array>& field_array,
      const Value& where_value, CompareOp op) {
    switch (field_array->type_id()) {
      case arrow::Type::INT64: {
        auto int_array =
            std::static_pointer_cast<arrow::Int64Array>(field_array);
        int64_t field_val = int_array->Value(0);

        if (where_value.type() != ValueType::Int64) {
          return arrow::Status::Invalid(
              "Type mismatch: field is Int64 but WHERE value is not");
        }

        int64_t where_val = where_value.get<int64_t>();
        return apply_comparison(field_val, where_val, op);
      }

      case arrow::Type::STRING: {
        auto str_array =
            std::static_pointer_cast<arrow::StringArray>(field_array);
        std::string field_val = str_array->GetString(0);

        if (where_value.type() != ValueType::String) {
          return arrow::Status::Invalid(
              "Type mismatch: field is String but WHERE value is not");
        }

        std::string where_val = where_value.get<std::string>();
        return apply_comparison(field_val, where_val, op);
      }

      case arrow::Type::DOUBLE: {
        auto double_array =
            std::static_pointer_cast<arrow::DoubleArray>(field_array);
        double field_val = double_array->Value(0);

        if (where_value.type() != ValueType::Double) {
          return arrow::Status::Invalid(
              "Type mismatch: field is Double but WHERE value is not");
        }

        double where_val = where_value.get<double>();
        return apply_comparison(field_val, where_val, op);
      }

      case arrow::Type::BOOL: {
        auto bool_array =
            std::static_pointer_cast<arrow::BooleanArray>(field_array);
        bool field_val = bool_array->Value(0);

        if (where_value.type() != ValueType::Bool) {
          return arrow::Status::Invalid(
              "Type mismatch: field is Bool but WHERE value is not");
        }

        bool where_val = where_value.get<bool>();
        return apply_comparison(field_val, where_val, op);
      }

      default:
        return arrow::Status::NotImplemented("Unsupported field type: ",
                                             field_array->type()->ToString());
    }
  }

  template <typename T>
  static bool apply_comparison(const T& field_val, const T& where_val,
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
          return false;  // CONTAINS only makes sense for strings
        }
      case CompareOp::StartsWith:
        if constexpr (std::is_same_v<T, std::string>) {
          return field_val.find(where_val) == 0;
        } else {
          return false;  // STARTS_WITH only makes sense for strings
        }
      case CompareOp::EndsWith:
        if constexpr (std::is_same_v<T, std::string>) {
          return field_val.size() >= where_val.size() &&
                 field_val.substr(field_val.size() - where_val.size()) ==
                     where_val;
        } else {
          return false;  // ENDS_WITH only makes sense for strings
        }
    }
    return false;
  }
};

enum class TraverseType { Inner, Left, Right, Full };

class Traverse final : public Clause {
 private:
  SchemaRef source_;
  std::string edge_type_;
  SchemaRef target_;
  TraverseType traverse_type_;

 public:
  Traverse(SchemaRef source, std::string edge_type, SchemaRef target,
           TraverseType traverse_type)
      : source_(std::move(source)),
        edge_type_(std::move(edge_type)),
        target_(std::move(target)),
        traverse_type_(traverse_type) {}

  [[nodiscard]] Type type() const override { return Type::TRAVERSE; }

  [[nodiscard]] const SchemaRef& source() const { return source_; }
  [[nodiscard]] const std::string& edge_type() const { return edge_type_; }
  [[nodiscard]] const SchemaRef& target() const { return target_; }
  [[nodiscard]] TraverseType traverse_type() const { return traverse_type_; }
};

struct Select final : Clause {
  std::vector<std::string> fields_;

 public:
  explicit Select(std::vector<std::string> fields)
      : fields_(std::move(fields)) {}

  [[nodiscard]] Type type() const override { return Type::SELECT; }

  [[nodiscard]] const std::vector<std::string>& fields() const {
    return fields_;
  }
};

// Simple comparison: field op value
class ComparisonExpr : public Clause, public WhereExpr {
 private:
  std::string field_;
  CompareOp op_;
  Value value_;
  bool inlined_ = false;

 public:
  ComparisonExpr(std::string field, CompareOp op, Value value)
      : field_(std::move(field)), op_(op), value_(std::move(value)) {}

  [[nodiscard]] const std::string& field() const { return field_; }
  [[nodiscard]] CompareOp op() const { return op_; }
  [[nodiscard]] const Value& value() const { return value_; }

  [[nodiscard]] Type type() const override { return Type::WHERE; }
  [[nodiscard]] bool inlined() const override { return inlined_; }
  void set_inlined(bool inlined) override { inlined_ = inlined; }

  [[nodiscard]] std::string toString() const override {
    std::stringstream ss;
    ss << "WHERE " << field_;

    // Convert operator to string
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
      case ValueType::Null:
        ss << "NULL";
        break;
      case ValueType::Int64:
        ss << value_.get<int64_t>();
        break;
      case ValueType::Double:
        ss << value_.get<double>();
        break;
      case ValueType::String:
        ss << "'" << value_.get<std::string>() << "'";
        break;
      case ValueType::Bool:
        ss << (value_.get<bool>() ? "true" : "false");
        break;
    }

    if (inlined_) {
      ss << " (inlined)";
    }

    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const ComparisonExpr& expr) {
    os << expr.toString();
    return os;
  }

  // Method to evaluate if a Node matches this comparison
  arrow::Result<bool> matches(
      const std::shared_ptr<Node>& node) const override {
    if (!node) {
      return arrow::Status::Invalid("Node is null");
    }

    // parse field name to extract variable and field parts
    // expected format: "variable.field" (e.g., "user.age", "company.name")
    size_t dot_pos = field_.find('.');
    std::string field_name;

    if (dot_pos != std::string::npos) {
      field_name = field_.substr(dot_pos + 1);
    } else {
      field_name = field_;
    }

    ARROW_ASSIGN_OR_RAISE(auto field_array, node->get_field(field_name));

    if (!field_array || field_array->length() == 0) {
      return arrow::Status::Invalid("Field '", field_name, "' is empty");
    }

    if (field_array->IsNull(0)) {
      switch (op_) {
        case CompareOp::Eq:
          return value_.type() == ValueType::Null;
        case CompareOp::NotEq:
          return value_.type() != ValueType::Null;
        default:
          return false;  // Most operators return false for null values
      }
    }
    return compare_values(field_array, value_, op_);
  }

  arrow::compute::Expression to_arrow_expression() const override {
    // Parse field name (remove variable prefix if present)
    std::string field_name;
    size_t dot_pos = field_.find('.');
    if (dot_pos != std::string::npos) {
      field_name = field_.substr(dot_pos + 1);
    } else {
      field_name = field_;
    }

    auto field_expr = arrow::compute::field_ref(field_name);
    auto value_expr = value_to_expression(value_);

    return apply_comparison_op(field_expr, value_expr, op_);
  }

  std::vector<std::shared_ptr<ComparisonExpr>> get_conditions_for_variable(
      const std::string& variable) const override {
    size_t dot_pos = field_.find('.');
    if (dot_pos != std::string::npos) {
      std::string var = field_.substr(0, dot_pos);
      if (var == variable) {
        return {std::make_shared<ComparisonExpr>(*this)};
      }
    }
    return {};
  }

  std::string extract_first_variable() const override {
    size_t dot_pos = field_.find('.');
    if (dot_pos != std::string::npos) {
      return field_.substr(0, dot_pos);
    }
    return "";
  }

  // Collect all variables referenced in this expression tree
  std::set<std::string> get_all_variables() const override {
    std::set<std::string> variables;
    size_t dot_pos = field_.find('.');
    if (dot_pos != std::string::npos) {
      std::string var = field_.substr(0, dot_pos);
      variables.insert(var);
    }
    return variables;
  }

 private:
  static arrow::Result<bool> compare_values(
      const std::shared_ptr<arrow::Array>& field_array,
      const Value& where_value, CompareOp op) {
    switch (field_array->type_id()) {
      case arrow::Type::INT64: {
        auto int_array =
            std::static_pointer_cast<arrow::Int64Array>(field_array);
        int64_t field_val = int_array->Value(0);
        if (where_value.type() != ValueType::Int64) {
          return arrow::Status::Invalid(
              "Type mismatch: field is Int64 but WHERE value is not");
        }
        int64_t where_val = where_value.get<int64_t>();
        return apply_comparison(field_val, where_val, op);
      }
      case arrow::Type::STRING: {
        auto str_array =
            std::static_pointer_cast<arrow::StringArray>(field_array);
        std::string field_val = str_array->GetString(0);
        if (where_value.type() != ValueType::String) {
          return arrow::Status::Invalid(
              "Type mismatch: field is String but WHERE value is not");
        }
        std::string where_val = where_value.get<std::string>();
        return apply_comparison(field_val, where_val, op);
      }
      case arrow::Type::DOUBLE: {
        auto double_array =
            std::static_pointer_cast<arrow::DoubleArray>(field_array);
        double field_val = double_array->Value(0);
        if (where_value.type() != ValueType::Double) {
          return arrow::Status::Invalid(
              "Type mismatch: field is Double but WHERE value is not");
        }
        double where_val = where_value.get<double>();
        return apply_comparison(field_val, where_val, op);
      }
      case arrow::Type::BOOL: {
        auto bool_array =
            std::static_pointer_cast<arrow::BooleanArray>(field_array);
        bool field_val = bool_array->Value(0);
        if (where_value.type() != ValueType::Bool) {
          return arrow::Status::Invalid(
              "Type mismatch: field is Bool but WHERE value is not");
        }
        bool where_val = where_value.get<bool>();
        return apply_comparison(field_val, where_val, op);
      }
      default:
        return arrow::Status::NotImplemented("Unsupported field type: ",
                                             field_array->type()->ToString());
    }
  }

  template <typename T>
  static bool apply_comparison(const T& field_val, const T& where_val,
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
          return false;  // CONTAINS only makes sense for strings
        }
      case CompareOp::StartsWith:
        if constexpr (std::is_same_v<T, std::string>) {
          return field_val.find(where_val) == 0;
        } else {
          return false;  // STARTS_WITH only makes sense for strings
        }
      case CompareOp::EndsWith:
        if constexpr (std::is_same_v<T, std::string>) {
          return field_val.size() >= where_val.size() &&
                 field_val.substr(field_val.size() - where_val.size()) ==
                     where_val;
        } else {
          return false;  // ENDS_WITH only makes sense for strings
        }
    }
    return false;
  }
};

// Logical combination: left op right
class LogicalExpr : public Clause, public WhereExpr {
 private:
  std::shared_ptr<WhereExpr> left_;
  LogicalOp op_;
  std::shared_ptr<WhereExpr> right_;
  bool inlined_ = false;

 public:
  LogicalExpr(std::shared_ptr<WhereExpr> left, LogicalOp op,
              std::shared_ptr<WhereExpr> right)
      : left_(std::move(left)), op_(op), right_(std::move(right)) {}

  [[nodiscard]] Type type() const override { return Type::WHERE; }
  [[nodiscard]] bool inlined() const override { return inlined_; }
  void set_inlined(bool inlined) override {
    inlined_ = inlined;
    if (left_) left_->set_inlined(inlined);
    if (right_) right_->set_inlined(inlined);
  }

  // Factory methods for building expressions
  static std::shared_ptr<LogicalExpr> and_expr(
      std::shared_ptr<WhereExpr> left, std::shared_ptr<WhereExpr> right) {
    return std::make_shared<LogicalExpr>(std::move(left), LogicalOp::AND,
                                         std::move(right));
  }

  static std::shared_ptr<LogicalExpr> or_expr(
      std::shared_ptr<WhereExpr> left, std::shared_ptr<WhereExpr> right) {
    return std::make_shared<LogicalExpr>(std::move(left), LogicalOp::OR,
                                         std::move(right));
  }

  // Tree evaluation with proper precedence
  arrow::Result<bool> matches(
      const std::shared_ptr<Node>& node) const override {
    if (!left_ || !right_) {
      return arrow::Status::Invalid(
          "LogicalExpr missing left or right operand");
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

  arrow::compute::Expression to_arrow_expression() const override {
    if (!left_ || !right_) {
      throw std::runtime_error("LogicalExpr missing left or right operand");
    }

    auto left_expr = left_->to_arrow_expression();
    auto right_expr = right_->to_arrow_expression();

    switch (op_) {
      case LogicalOp::AND:
        return arrow::compute::and_(left_expr, right_expr);
      case LogicalOp::OR:
        return arrow::compute::or_(left_expr, right_expr);
      default:
        throw std::runtime_error("Unknown logical operator in LogicalExpr");
    }
  }

  std::vector<std::shared_ptr<ComparisonExpr>> get_conditions_for_variable(
      const std::string& variable) const override {
    // First check if ALL variables in this expression belong to the target
    // variable
    auto all_variables = get_all_variables();
    for (const auto& var : all_variables) {
      if (var != variable) {
        // Found a variable that doesn't match the target - return empty
        return {};
      }
    }

    // All variables belong to the target, so we can safely collect conditions
    std::vector<std::shared_ptr<ComparisonExpr>> result;
    if (left_) {
      auto left_conditions = left_->get_conditions_for_variable(variable);
      result.insert(result.end(), left_conditions.begin(),
                    left_conditions.end());
    }
    if (right_) {
      auto right_conditions = right_->get_conditions_for_variable(variable);
      result.insert(result.end(), right_conditions.begin(),
                    right_conditions.end());
    }
    return result;
  }

  std::string extract_first_variable() const override {
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

  std::string toString() const override {
    if (!left_ || !right_) {
      return "WHERE (incomplete logical expression)";
    }

    std::string left_str = left_->toString();
    std::string right_str = right_->toString();

    // Remove "WHERE " prefixes from nested expressions
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

  friend std::ostream& operator<<(std::ostream& os, const LogicalExpr& expr) {
    os << expr.toString();
    return os;
  }

  // Collect all variables referenced in this expression tree
  std::set<std::string> get_all_variables() const override {
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
};

class Query {
 private:
  SchemaRef from_;
  std::vector<std::shared_ptr<Clause>> clauses_;
  std::shared_ptr<Select> select_;
  bool inline_where_;

 public:
  Query(SchemaRef from, std::vector<std::shared_ptr<Clause>> clauses,
        std::shared_ptr<Select> select, bool optimize_where)
      : from_(std::move(from)),
        clauses_(std::move(clauses)),
        select_(std::move(select)),
        inline_where_(optimize_where) {}

  class Builder;
  [[nodiscard]] const SchemaRef& from() const { return from_; }
  [[nodiscard]] const std::vector<std::shared_ptr<Clause>>& clauses() const {
    return clauses_;
  }
  [[nodiscard]] const std::shared_ptr<Select>& select() const {
    return select_;
  }
  [[nodiscard]] bool inline_where() const { return inline_where_; }

  // Builder creation
  static Builder from(const std::string& schema) { return Builder(schema); }

  // Builder class
  class Builder {
   private:
    SchemaRef from_;
    std::vector<std::shared_ptr<Clause>> clauses_;
    std::shared_ptr<Select> select_;
    bool inline_where_ = false;

   public:
    explicit Builder(const std::string& schema)
        : from_(SchemaRef::parse(schema)) {}

    Builder& where(std::string field, CompareOp op, Value value) {
      clauses_.push_back(std::make_shared<ComparisonExpr>(std::move(field), op,
                                                          std::move(value)));
      return *this;
    }

    Builder& traverse(const std::string& source, std::string edge_type,
                      const std::string& target,
                      TraverseType traverse_type = TraverseType::Inner) {
      clauses_.push_back(std::make_shared<Traverse>(
          std::move(SchemaRef::parse(source)), std::move(edge_type),
          std::move(SchemaRef::parse(target)), traverse_type));
      return *this;
    }

    Builder& select(std::vector<std::string> names = {}) {
      select_ = std::make_shared<Select>(std::move(names));
      return *this;
    }

    Builder& inline_where() {
      inline_where_ = true;
      return *this;
    }

    Builder& where_logical_expr(std::shared_ptr<LogicalExpr> expr) {
      clauses_.push_back(expr);
      return *this;
    }

    Builder& and_where(const std::string& field, CompareOp op,
                       const Value& value) {
      if (clauses_.empty() || clauses_.back()->type() != Clause::Type::WHERE) {
        throw std::runtime_error(
            "and_where() can only be called after where()");
      }
      auto last_clause = clauses_.back();
      auto new_condition = std::make_shared<ComparisonExpr>(field, op, value);

      auto combined = LogicalExpr::and_expr(
          std::dynamic_pointer_cast<WhereExpr>(last_clause), new_condition);

      clauses_.back() = combined;
      return *this;
    }

    Builder& or_where(const std::string& field, CompareOp op,
                      const Value& value) {
      if (clauses_.empty() || clauses_.back()->type() != Clause::Type::WHERE) {
        throw std::runtime_error("or_where() can only be called after where()");
      }
      auto last_clause = clauses_.back();
      auto new_condition = std::make_shared<ComparisonExpr>(field, op, value);
      auto combined = LogicalExpr::or_expr(
          std::dynamic_pointer_cast<WhereExpr>(last_clause), new_condition);
      clauses_.back() = combined;
      return *this;
    }

    // Advanced API for explicit precedence control
    Builder& where_and(std::shared_ptr<WhereExpr> left,
                       std::shared_ptr<WhereExpr> right) {
      clauses_.push_back(
          LogicalExpr::and_expr(std::move(left), std::move(right)));
      return *this;
    }

    Builder& where_or(std::shared_ptr<WhereExpr> left,
                      std::shared_ptr<WhereExpr> right) {
      clauses_.push_back(
          LogicalExpr::or_expr(std::move(left), std::move(right)));
      return *this;
    }

    Query build() {
      return {from_, std::move(clauses_), std::move(select_), inline_where_};
    }
  };
};

class QueryResult {
 public:
  [[nodiscard]] std::shared_ptr<arrow::Table> table() const { return table_; }
  void set_table(const std::shared_ptr<arrow::Table>& table) { table_ = table; }

 private:
  std::shared_ptr<arrow::Table> table_;
};

}  // namespace tundradb

#endif  // QUERY_HPP
