#ifndef QUERY_HPP
#define QUERY_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>

#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "node.hpp"
#include "schema.hpp"
#include "temporal_context.hpp"
#include "types.hpp"

namespace tundradb {

class WhereExpr;
class ComparisonExpr;

/**
 * @brief A reference to a schema, optionally aliased (e.g. "u:User" or just
 * "User").
 *
 * Carries a cached 16-bit tag for fast hash packing during BFS traversal.
 */
struct SchemaRef {
 private:
  std::string schema_;
  std::string value_;
  bool declaration_;
  uint16_t schema_tag_ = 0;  ///< Cached 16-bit FNV-1a tag.

 public:
  [[nodiscard]] std::string schema() const { return schema_; }
  [[nodiscard]] std::string value() const { return value_; }
  [[nodiscard]] bool is_declaration() const { return declaration_; }
  [[nodiscard]] uint16_t tag() const { return schema_tag_; }
  void set_schema(const std::string& schema) { schema_ = schema; }
  void set_tag(uint16_t t) { schema_tag_ = t; }

  /**
   * @brief Parses a schema reference from "alias:schema" format.
   *
   * If no colon is present, the entire string is used as both alias and schema.
   *
   * @param s The input string.
   * @return The parsed SchemaRef.
   */
  static SchemaRef parse(const std::string& s);

  /** @brief Returns a human-readable "alias:schema" string. */
  [[nodiscard]] std::string toString() const;

  friend std::ostream& operator<<(std::ostream& os, const SchemaRef& obj) {
    os << obj.toString();
    return os;
  }
};

/** @brief Comparison operators supported in WHERE expressions. */
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

/** @brief Base class for all query clauses (WHERE, TRAVERSE, SELECT, etc.). */
class Clause {
 public:
  virtual ~Clause() = default;

  enum class Type { WHERE, TRAVERSE, PROJECT, ORDER_BY, LIMIT, SELECT };

  /** @brief Returns the clause's discriminator. */
  [[nodiscard]] virtual Type type() const = 0;
};

/** @brief Wraps a Value as an Arrow compute literal expression. */
arrow::compute::Expression value_to_expression(const Value& value);

/**
 * @brief Builds an Arrow compute comparison expression.
 *
 * @param field The field reference expression.
 * @param value The literal value expression.
 * @param op The comparison operator.
 * @return The composed comparison expression.
 */
arrow::compute::Expression apply_comparison_op(
    const arrow::compute::Expression& field,
    const arrow::compute::Expression& value, CompareOp op);

/** @brief Logical operators for combining WHERE expressions. */
enum class LogicalOp { AND, OR };

/**
 * @brief Abstract base for WHERE expression nodes (comparisons and logical
 * operators).
 */
class WhereExpr {
 public:
  virtual ~WhereExpr() = default;

  /** @brief Resolves symbolic field references against the schema registry. */
  virtual arrow::Result<bool> resolve_field_ref(
      const std::unordered_map<std::string, std::string>& aliases,
      const SchemaRegistry* schema_registry) = 0;
  /** @brief Evaluates this expression against a node. */
  virtual arrow::Result<bool> matches(
      const std::shared_ptr<Node>& node) const = 0;

  /** @brief Returns a debug string of the expression. */
  virtual std::string toString() const = 0;

  /** @brief Marks this expression as already inlined into a traversal. */
  virtual void set_inlined(bool inlined) = 0;

  /** @brief Returns true if this expression has been inlined. */
  virtual bool inlined() const = 0;

  /** @brief Converts this expression to an Arrow compute expression. */
  virtual arrow::compute::Expression to_arrow_expression(
      bool strip_var) const = 0;

  /** @brief Extracts sub-conditions that reference a specific variable. */
  virtual std::vector<std::shared_ptr<ComparisonExpr>>
  get_conditions_for_variable(const std::string& variable) const = 0;

  /** @brief Returns the set of all variables referenced in this expression. */
  virtual std::set<std::string> get_all_variables() const = 0;

  /** @brief Returns the first variable name found (useful for single-var
   * conditions). */
  virtual std::string extract_first_variable() const = 0;

  /** @brief Returns true if this expression can be inlined for the given
   * variable. */
  virtual bool can_inline(const std::string& variable) const = 0;
};

/** @brief The type of graph traversal / join to perform. */
enum class TraverseType { Inner, Left, Right, Full };

/** @brief A TRAVERSE clause specifying an edge traversal between two schemas.
 */
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

/** @brief A SELECT clause listing the fields to include in the output. */
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

/**
 * @brief A leaf WHERE expression: field op value (e.g. "u.age > 30").
 */
class ComparisonExpr : public Clause, public WhereExpr {
 private:
  FieldRef field_ref_;
  CompareOp op_;
  Value value_;
  bool inlined_ = false;

  static arrow::Result<bool> compare_values(const Value& value, CompareOp op,
                                            const Value& where_value);

  template <typename T>
  static bool apply_comparison(const T& field_val, CompareOp op,
                               const T& where_val);

  template <typename T>
  static bool apply_comparison(const T& field_val, const T& where_val,
                               CompareOp op);

 public:
  ComparisonExpr(FieldRef field_ref, CompareOp op, Value value)
      : field_ref_(std::move(field_ref)), op_(op), value_(std::move(value)) {}

  // Backward compatibility constructor
  ComparisonExpr(const std::string& field, CompareOp op, Value value);

  [[nodiscard]] const FieldRef& field_ref() const { return field_ref_; }
  [[nodiscard]] const std::string& field() const { return field_ref_.value(); }
  [[nodiscard]] CompareOp op() const { return op_; }
  [[nodiscard]] const Value& value() const { return value_; }

  [[nodiscard]] Type type() const override { return Type::WHERE; }
  [[nodiscard]] bool inlined() const override { return inlined_; }
  void set_inlined(bool inlined) override { inlined_ = inlined; }

  [[nodiscard]] std::string toString() const override;

  friend std::ostream& operator<<(std::ostream& os, const ComparisonExpr& expr);

  arrow::Result<bool> matches(const std::shared_ptr<Node>& node) const override;

  [[nodiscard]] arrow::compute::Expression to_arrow_expression(
      bool strip_var) const override;

  std::vector<std::shared_ptr<ComparisonExpr>> get_conditions_for_variable(
      const std::string& variable) const override;

  bool can_inline(const std::string& variable) const override;

  std::string extract_first_variable() const override;

  std::set<std::string> get_all_variables() const override;

  arrow::Result<bool> resolve_field_ref(
      const std::unordered_map<std::string, std::string>& aliases,
      const SchemaRegistry* schema_registry) override;
};

/**
 * @brief A composite WHERE expression: left AND/OR right.
 */
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
  void set_inlined(bool inlined) override;

  arrow::Result<bool> resolve_field_ref(
      const std::unordered_map<std::string, std::string>& aliases,
      const SchemaRegistry* schema_registry) override;

  static std::shared_ptr<LogicalExpr> and_expr(
      std::shared_ptr<WhereExpr> left, std::shared_ptr<WhereExpr> right);

  static std::shared_ptr<LogicalExpr> or_expr(std::shared_ptr<WhereExpr> left,
                                              std::shared_ptr<WhereExpr> right);

  // Public accessors
  [[nodiscard]] const std::shared_ptr<WhereExpr>& left() const { return left_; }
  [[nodiscard]] const std::shared_ptr<WhereExpr>& right() const {
    return right_;
  }
  [[nodiscard]] LogicalOp op() const { return op_; }

  arrow::Result<bool> matches(const std::shared_ptr<Node>& node) const override;

  [[nodiscard]] arrow::compute::Expression to_arrow_expression(
      bool strip_var) const override;

  std::vector<std::shared_ptr<ComparisonExpr>> get_conditions_for_variable(
      const std::string& variable) const override;

  std::string extract_first_variable() const override;

  std::string toString() const override;

  friend std::ostream& operator<<(std::ostream& os, const LogicalExpr& expr);

  std::set<std::string> get_all_variables() const override;

  bool can_inline(const std::string& variable) const override;
};

/** @brief Configuration knobs for parallel query execution. */
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

/**
 * @brief Immutable query descriptor built via Query::Builder.
 *
 * Contains the FROM schema, a list of clauses (TRAVERSE, WHERE, SELECT),
 * execution configuration, and optional temporal snapshot.
 */
class Query {
 private:
  SchemaRef from_;
  std::vector<std::shared_ptr<Clause>> clauses_;
  std::shared_ptr<Select> select_;
  bool inline_where_;
  ExecutionConfig execution_config_;
  std::optional<TemporalSnapshot> temporal_snapshot_;

 public:
  Query(SchemaRef from, std::vector<std::shared_ptr<Clause>> clauses,
        std::shared_ptr<Select> select, bool optimize_where,
        ExecutionConfig execution_config,
        std::optional<TemporalSnapshot> temporal_snapshot = std::nullopt)
      : from_(std::move(from)),
        clauses_(std::move(clauses)),
        select_(std::move(select)),
        inline_where_(optimize_where),
        execution_config_(execution_config),
        temporal_snapshot_(std::move(temporal_snapshot)) {}

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

  [[nodiscard]] const std::optional<TemporalSnapshot>& temporal_snapshot()
      const {
    return temporal_snapshot_;
  }

  static Builder from(const std::string& schema) { return Builder(schema); }

  /** @brief Fluent builder for constructing Query objects. */
  class Builder {
   private:
    SchemaRef from_;
    std::vector<std::shared_ptr<Clause>> clauses_;
    std::shared_ptr<Select> select_;
    bool inline_where_ = false;
    ExecutionConfig execution_config_;
    std::optional<TemporalSnapshot> temporal_snapshot_;

   public:
    explicit Builder(const std::string& schema)
        : from_(SchemaRef::parse(schema)) {}

    /** @brief Adds a simple comparison WHERE clause. */
    Builder& where(std::string field, CompareOp op, Value value) {
      clauses_.push_back(std::make_shared<ComparisonExpr>(std::move(field), op,
                                                          std::move(value)));
      return *this;
    }

    /** @brief Adds a TRAVERSE clause (edge traversal between two schemas). */
    Builder& traverse(const std::string& source, std::string edge_type,
                      const std::string& target,
                      TraverseType traverse_type = TraverseType::Inner) {
      clauses_.push_back(std::make_shared<Traverse>(
          std::move(SchemaRef::parse(source)), std::move(edge_type),
          std::move(SchemaRef::parse(target)), traverse_type));
      return *this;
    }

    /** @brief Sets the SELECT clause. Empty means "all fields". */
    Builder& select(std::vector<std::string> names = {}) {
      select_ = std::make_shared<Select>(std::move(names));
      return *this;
    }

    /** @brief Enables WHERE-clause inlining into traversal steps. */
    Builder& inline_where() {
      inline_where_ = true;
      return *this;
    }

    /** @brief Overrides the automatic parallel batch size. */
    Builder& parallel_batch_size(size_t size) {
      execution_config_.parallel_batch_size = size;
      return *this;
    }

    /** @brief Enables or disables parallel execution. */
    Builder& parallel(bool enabled = true) {
      execution_config_.parallel_enabled = enabled;
      return *this;
    }

    /** @brief Sets the thread count for parallel execution. */
    Builder& parallel_thread_count(size_t count) {
      execution_config_.parallel_thread_count = count;
      return *this;
    }

    /** @brief Adds a pre-built logical expression as a WHERE clause. */
    Builder& where_logical_expr(std::shared_ptr<LogicalExpr> expr) {
      clauses_.push_back(expr);
      return *this;
    }

    /** @brief Appends an AND condition to the most recent WHERE clause. */
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

    /** @brief Appends an OR condition to the most recent WHERE clause. */
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

    /** @brief Adds a standalone AND expression from two sub-expressions. */
    Builder& where_and(std::shared_ptr<WhereExpr> left,
                       std::shared_ptr<WhereExpr> right) {
      clauses_.push_back(
          LogicalExpr::and_expr(std::move(left), std::move(right)));
      return *this;
    }

    /** @brief Adds a standalone OR expression from two sub-expressions. */
    Builder& where_or(std::shared_ptr<WhereExpr> left,
                      std::shared_ptr<WhereExpr> right) {
      clauses_.push_back(
          LogicalExpr::or_expr(std::move(left), std::move(right)));
      return *this;
    }

    /**
     * Set valid time for temporal query (AS OF VALIDTIME).
     * @param timestamp Nanosecond timestamp for valid time dimension
     */
    Builder& as_of_valid_time(uint64_t timestamp) {
      if (!temporal_snapshot_.has_value()) {
        temporal_snapshot_ = TemporalSnapshot::as_of_valid(timestamp);
      } else {
        temporal_snapshot_->valid_time = timestamp;
      }
      return *this;
    }

    /**
     * Set transaction time for temporal query (AS OF TXNTIME).
     * @param timestamp Nanosecond timestamp for transaction time dimension
     */
    Builder& as_of_tx_time(uint64_t timestamp) {
      if (!temporal_snapshot_.has_value()) {
        temporal_snapshot_ = TemporalSnapshot::as_of_tx(timestamp);
      } else {
        temporal_snapshot_->tx_time = timestamp;
      }
      return *this;
    }

    /**
     * Set both valid and transaction times for bitemporal query.
     * @param valid_timestamp Valid time (when fact was true in domain)
     * @param tx_timestamp Transaction time (when DB recorded it)
     */
    Builder& as_of(uint64_t valid_timestamp, uint64_t tx_timestamp) {
      temporal_snapshot_ = TemporalSnapshot{valid_timestamp, tx_timestamp};
      return *this;
    }

    /** @brief Constructs an immutable Query from the accumulated builder state.
     */
    Query build() {
      return {
          from_,         std::move(clauses_), std::move(select_),
          inline_where_, execution_config_,   std::move(temporal_snapshot_)};
    }
  };
};

/** @brief Counters collected during query execution for diagnostics. */
struct QueryExecutionStats {
  int num_nodes_processed = 0;
  int num_edges_traversed = 0;
  int num_where_clauses_inlined = 0;
  int num_where_clauses_post_processed = 0;
  std::vector<std::string> inlined_conditions;         // For debugging
  std::vector<std::string> post_processed_conditions;  // For debugging
};

/** @brief Holds the output Arrow table and execution statistics from a query.
 */
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
