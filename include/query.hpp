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
#include "schema.hpp"
#include "types.hpp"

namespace tundradb {

class WhereExpr;
class ComparisonExpr;

struct SchemaRef {
 private:
  std::string schema_;
  std::string value_;
  bool declaration_;
  // Cached 16-bit tag for fast path operations (e.g., BFS packing)
  uint16_t schema_tag_ = 0;

 public:
  [[nodiscard]] std::string schema() const { return schema_; }
  [[nodiscard]] std::string value() const { return value_; }
  [[nodiscard]] bool is_declaration() const { return declaration_; }
  [[nodiscard]] uint16_t tag() const { return schema_tag_; }
  void set_schema(const std::string& schema) { schema_ = schema; }
  void set_tag(uint16_t t) { schema_tag_ = t; }

  // Parse a schema reference from a string format "alias:schema"
  // If the string does not contain a colon, the value is assigned to the alias
  // and schema
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

/**
 * @brief Structured field reference that supports both unresolved and resolved
 * states
 *
 * Unresolved: "u.age" -> variable="u", field_name="age", field=nullptr
 * Resolved: variable="u", field_name="age", field=Field{name="age", type=INT32,
 * index=2}
 */
struct FieldRef {
  FieldRef(const std::string& var, const std::string& fname,
           std::shared_ptr<Field> fld)
      : variable_(var), field_name_(fname), field_(std::move(fld)) {
    value_ = var.empty() ? fname : var + "." + fname;
  }

  FieldRef(const std::string& var, const std::string& fname)
      : FieldRef(var, fname, nullptr) {}

  // Parse from string format "variable.field_name"
  static FieldRef from_string(const std::string& field_str);

  const std::string& value() const { return value_; }

  const std::string& to_string() const { return value_; }

  const std::string& field_name() const { return field_name_; }

  const std::string& variable() const { return variable_; }

  std::shared_ptr<Field> field() const { return field_; }

  bool is_resolved() const { return field_ != nullptr; }

 private:
  std::string variable_;    // e.g., "u", "c", "f"
  std::string field_name_;  // e.g., "age", "name"
  std::string value_;       // e.g.: "u.age", "c.name", "f.id"
  std::shared_ptr<Field> field_ =
      nullptr;  // Resolved Field object (null until schema resolution)

  // Resolve this FieldRef with actual Field from schema
  void resolve(std::shared_ptr<Field> resolved_field) {
    field_ = std::move(resolved_field);
  }
  friend class WhereExpr;
  friend class ComparisonExpr;
};

class Clause {
 public:
  virtual ~Clause() = default;

  enum class Type { WHERE, TRAVERSE, PROJECT, ORDER_BY, LIMIT, SELECT };
  [[nodiscard]] virtual Type type() const = 0;
};

arrow::compute::Expression value_to_expression(const Value& value);
arrow::compute::Expression apply_comparison_op(
    const arrow::compute::Expression& field,
    const arrow::compute::Expression& value, CompareOp op);

enum class LogicalOp { AND, OR };

class WhereExpr {
 public:
  virtual ~WhereExpr() = default;
  virtual arrow::Result<bool> resolve_field_ref(
      const std::unordered_map<std::string, std::string>& aliases,
      const SchemaRegistry* schema_registry) = 0;
  virtual arrow::Result<bool> matches(
      const std::shared_ptr<Node>& node) const = 0;
  virtual std::string toString() const = 0;
  virtual void set_inlined(bool inlined) = 0;
  virtual bool inlined() const = 0;
  virtual arrow::compute::Expression to_arrow_expression(
      bool strip_var) const = 0;

  virtual std::vector<std::shared_ptr<ComparisonExpr>>
  get_conditions_for_variable(const std::string& variable) const = 0;

  virtual std::set<std::string> get_all_variables() const = 0;

  virtual std::string extract_first_variable() const = 0;

  virtual bool can_inline(const std::string& variable) const = 0;
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

  // Internal mutation helpers for precomputing tags
  SchemaRef& mutable_source() { return source_; }
  SchemaRef& mutable_target() { return target_; }
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

class ComparisonExpr : public Clause, public WhereExpr {
 private:
  FieldRef field_ref_;
  CompareOp op_;
  Value value_;
  bool inlined_ = false;

  static arrow::Result<bool> compare_values(const Value& value, CompareOp op,
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
  static bool apply_comparison(const T& field_val, const CompareOp op,
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

 public:
  ComparisonExpr(FieldRef field_ref, CompareOp op, Value value)
      : field_ref_(std::move(field_ref)), op_(op), value_(std::move(value)) {}

  // Backward compatibility constructor
  ComparisonExpr(const std::string& field, CompareOp op, Value value)
      : field_ref_(FieldRef::from_string(field)),
        op_(op),
        value_(std::move(value)) {}

  [[nodiscard]] const FieldRef& field_ref() const { return field_ref_; }
  [[nodiscard]] const std::string& field() const { return field_ref_.value(); }
  [[nodiscard]] CompareOp op() const { return op_; }
  [[nodiscard]] const Value& value() const { return value_; }

  [[nodiscard]] Type type() const override { return Type::WHERE; }
  [[nodiscard]] bool inlined() const override { return inlined_; }
  void set_inlined(bool inlined) override { inlined_ = inlined; }

  [[nodiscard]] std::string toString() const override {
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

  friend std::ostream& operator<<(std::ostream& os,
                                  const ComparisonExpr& expr) {
    os << expr.toString();
    return os;
  }

  arrow::Result<bool> matches(
      const std::shared_ptr<Node>& node) const override {
    if (!node) {
      return arrow::Status::Invalid("Node is null");
    }
    assert(field_ref_.field() != nullptr);
    ARROW_ASSIGN_OR_RAISE(auto field_value,
                          node->get_value(field_ref_.field()));
    return compare_values(field_value, op_, value_);
  }

  [[nodiscard]] arrow::compute::Expression to_arrow_expression(
      bool strip_var) const override {
    std::string field_name =
        strip_var ? field_ref_.field_name() : field_ref_.value();
    const auto field_expr = arrow::compute::field_ref(field_name);
    const auto value_expr = value_to_expression(value_);

    return apply_comparison_op(field_expr, value_expr, op_);
  }

  std::vector<std::shared_ptr<ComparisonExpr>> get_conditions_for_variable(
      const std::string& variable) const override {
    if (field_ref_.variable() == variable) {
      return {std::make_shared<ComparisonExpr>(*this)};
    }
    return {};
  }

  bool can_inline(const std::string& variable) const override {
    return field_ref_.variable() == variable;
  }

  std::string extract_first_variable() const override {
    return field_ref_.variable();
  }

  std::set<std::string> get_all_variables() const override {
    std::set<std::string> variables;
    variables.insert(field_ref_.variable());
    return variables;
  }

  arrow::Result<bool> resolve_field_ref(
      const std::unordered_map<std::string, std::string>& aliases,
      const SchemaRegistry* schema_registry) override {
    if (field_ref_.is_resolved()) {
      return true;
    }

    const std::string& variable = field_ref_.variable();
    const std::string& field_name = field_ref_.field_name();

    // Find the actual schema for this variable
    auto it = aliases.find(variable);
    if (it == aliases.end()) {
      return arrow::Status::KeyError("Unknown variable '", variable,
                                     "' in field '", field_ref_.to_string(),
                                     "'");
    }

    const std::string& schema_name = it->second;

    auto schema_result = schema_registry->get(schema_name);
    if (!schema_result.ok()) {
      return arrow::Status::KeyError(
          "Schema '", schema_name, "' not found for variable '", variable, "'");
    }

    auto schema = schema_result.ValueOrDie();
    auto field = schema->get_field(field_name);
    if (!field) {
      return arrow::Status::KeyError(
          "Field '", field_name, "' not found in schema '", schema_name, "'");
    }
    field_ref_.resolve(field);

    return true;
  }

 private:
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
};

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

  arrow::Result<bool> resolve_field_ref(
      const std::unordered_map<std::string, std::string>& aliases,
      const SchemaRegistry* schema_registry) override {
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

  // Public accessors
  [[nodiscard]] const std::shared_ptr<WhereExpr>& left() const { return left_; }
  [[nodiscard]] const std::shared_ptr<WhereExpr>& right() const {
    return right_;
  }
  [[nodiscard]] LogicalOp op() const { return op_; }

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

  [[nodiscard]] arrow::compute::Expression to_arrow_expression(
      bool strip_var) const override {
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

  std::vector<std::shared_ptr<ComparisonExpr>> get_conditions_for_variable(
      const std::string& variable) const override {
    auto all_variables = get_all_variables();
    for (const auto& var : all_variables) {
      if (var != variable) {
        return {};
      }
    }

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

  bool can_inline(const std::string& variable) const override {
    if (left_ && !left_->can_inline(variable)) return false;
    if (right_ && !right_->can_inline(variable)) return false;
    return true;
  }
};

struct ExecutionConfig {
 private:
  static size_t get_default_thread_count() {
    auto hw_threads = std::thread::hardware_concurrency();
    if (hw_threads <= 4) {
      return hw_threads;
    }
    if (hw_threads <= 16) {
      return hw_threads - 1;
    }
    return hw_threads - 2;
  }

  static size_t calculate_batch_size(size_t total_items, size_t thread_count) {
    size_t target_batches = thread_count * BATCHES_PER_THREAD;
    size_t calculated = total_items / target_batches;
    return std::clamp(calculated, size_t{100}, size_t{5000});
  }

 public:
  static constexpr size_t BATCHES_PER_THREAD = 3;
  size_t parallel_batch_size = 0;
  bool parallel_enabled = true;
  size_t parallel_thread_count = get_default_thread_count();

  [[nodiscard]] size_t calculate_batch_size(size_t total_items) const {
    const size_t target_batches = parallel_thread_count * BATCHES_PER_THREAD;
    const size_t calculated = total_items / target_batches;
    return std::clamp(calculated, size_t{100}, size_t{5000});
  }
};

class Query {
 private:
  SchemaRef from_;
  std::vector<std::shared_ptr<Clause>> clauses_;
  std::shared_ptr<Select> select_;
  bool inline_where_;
  ExecutionConfig execution_config_;

 public:
  Query(SchemaRef from, std::vector<std::shared_ptr<Clause>> clauses,
        std::shared_ptr<Select> select, bool optimize_where,
        ExecutionConfig execution_config)
      : from_(std::move(from)),
        clauses_(std::move(clauses)),
        select_(std::move(select)),
        inline_where_(optimize_where),
        execution_config_(execution_config) {}

  class Builder;
  [[nodiscard]] const SchemaRef& from() const { return from_; }
  [[nodiscard]] const std::vector<std::shared_ptr<Clause>>& clauses() const {
    return clauses_;
  }
  [[nodiscard]] const std::shared_ptr<Select>& select() const {
    return select_;
  }
  [[nodiscard]] bool inline_where() const { return inline_where_; }

  [[nodiscard]] const ExecutionConfig& execution_config() const {
    return execution_config_;
  }

  static Builder from(const std::string& schema) { return Builder(schema); }

  class Builder {
   private:
    SchemaRef from_;
    std::vector<std::shared_ptr<Clause>> clauses_;
    std::shared_ptr<Select> select_;
    bool inline_where_ = false;
    ExecutionConfig execution_config_;

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

    Builder& parallel_batch_size(size_t size) {
      execution_config_.parallel_batch_size = size;
      return *this;
    }

    Builder& parallel(bool enabled = true) {
      execution_config_.parallel_enabled = enabled;
      return *this;
    }

    Builder& parallel_thread_count(size_t count) {
      execution_config_.parallel_thread_count = count;
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
      return {from_, std::move(clauses_), std::move(select_), inline_where_,
              execution_config_};
    }
  };
};

struct QueryExecutionStats {
  int num_nodes_processed = 0;
  int num_edges_traversed = 0;
  int num_where_clauses_inlined = 0;
  int num_where_clauses_post_processed = 0;
  std::vector<std::string> inlined_conditions;         // For debugging
  std::vector<std::string> post_processed_conditions;  // For debugging
};

class QueryResult {
 public:
  [[nodiscard]] std::shared_ptr<arrow::Table> table() const { return table_; }
  void set_table(const std::shared_ptr<arrow::Table>& table) { table_ = table; }

  const QueryExecutionStats& execution_stats() const { return stats_; }
  QueryExecutionStats& mutable_execution_stats() { return stats_; }

 private:
  std::shared_ptr<arrow::Table> table_;
  QueryExecutionStats stats_;
};

}  // namespace tundradb

#endif  // QUERY_HPP
