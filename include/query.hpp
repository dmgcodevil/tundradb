#ifndef QUERY_HPP
#define QUERY_HPP

#include <arrow/api.h>
#include <arrow/result.h>

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "node.hpp"
#include "types.hpp"

namespace tundradb {

struct SchemaRef {
 private:
  std::string schema_;
  std::string value_;
  bool has_alias_;

 public:
  [[nodiscard]] std::string schema() const { return schema_; }
  [[nodiscard]] std::string value() const { return value_; }
  [[nodiscard]] bool has_alias() const { return has_alias_; }

  static SchemaRef parse(const std::string& s) {
    SchemaRef r;
    size_t pos = s.find(':');
    if (pos == std::string::npos) {
      r.schema_ = s;
      r.value_ = s;
      r.has_alias_ = false;
    } else {
      r.value_ = s.substr(0, pos);
      r.schema_ = s.substr(pos + 1);
      r.has_alias_ = true;
    }
    return r;
  }

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    if (has_alias_) {
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
  enum class Type { WHERE, TRAVERSE, PROJECT, ORDER_BY, LIMIT };
  virtual Type type() const = 0;
};

class Where : public Clause {
 private:
  std::string field_;
  CompareOp op_;
  Value value_;

 public:
  Where(std::string field, CompareOp op, Value value)
      : field_(std::move(field)), op_(op), value_(std::move(value)) {}

  Type type() const override { return Type::WHERE; }

  [[nodiscard]] const std::string& field() const { return field_; }
  [[nodiscard]] CompareOp op() const { return op_; }
  [[nodiscard]] const Value& value() const { return value_; }
};

enum class TraverseType { Inner };

class Traverse : public Clause {
 private:
  SchemaRef source_;
  std::string edge_type_;
  SchemaRef target_;
  TraverseType traverse_type_ = TraverseType::Inner;

 public:
  Traverse(SchemaRef source, std::string edge_type, SchemaRef target)
      : source_(std::move(source)),
        edge_type_(std::move(edge_type)),
        target_(std::move(target)) {}

  Type type() const override { return Type::TRAVERSE; }

  [[nodiscard]] const SchemaRef& source() const { return source_; }
  [[nodiscard]] const std::string& edge_type() const { return edge_type_; }
  [[nodiscard]] const SchemaRef& target() const { return target_; }
  [[nodiscard]] TraverseType traverse_type() const { return traverse_type_; }
};

class Query {
 private:
  SchemaRef from_;
  std::vector<std::shared_ptr<Clause>> clauses_;

  // Constructor used by Builder
  Query(SchemaRef from, std::vector<std::shared_ptr<Clause>> clauses)
      : from_(std::move(from)), clauses_(std::move(clauses)) {}

 public:
  class Builder;
  const SchemaRef& from() const { return from_; }
  const std::vector<std::shared_ptr<Clause>>& clauses() const {
    return clauses_;
  }

  // Builder creation
  static Builder from(std::string schema) { return Builder(std::move(schema)); }

  // Builder class
  class Builder {
   private:
    SchemaRef from_;
    std::vector<std::shared_ptr<Clause>> clauses_;

   public:
    explicit Builder(std::string schema) : from_(SchemaRef::parse(schema)) {}

    Builder& where(std::string field, CompareOp op, Value value) {
      clauses_.push_back(
          std::make_shared<Where>(std::move(field), op, std::move(value)));
      return *this;
    }

    Builder& traverse(std::string source, std::string edge_type,
                      std::string target) {
      clauses_.push_back(std::make_shared<Traverse>(
          std::move(SchemaRef::parse(source)), std::move(edge_type),
          std::move(SchemaRef::parse(target))));
      return *this;
    }

    // Additional builder methods for other clause types

    Query build() { return Query(from_, std::move(clauses_)); }
  };
};

// Result handling
class QueryResult {
 public:
  // Get a vector of matching nodes
  std::vector<std::shared_ptr<Node>> nodes() const { return nodes_; }

  // Get the first node or null
  std::shared_ptr<Node> first() const {
    return nodes_.empty() ? nullptr : nodes_[0];
  }

  // Get count of results
  size_t count() const { return nodes_.size(); }

  // Access as Arrow Table
  std::shared_ptr<arrow::Table> as_table() const {
    if (!table_) {
      // Create table with our schema and populate with data
      auto schema_result = build_denormalized_schema();
      if (schema_result.ok()) {
        auto result = populate_denormalized_table(schema_result.ValueOrDie());
        if (result.ok()) {
          table_ = result.ValueOrDie();
        } else {
          // If population fails, return empty table with the schema
          table_ =
              arrow::Table::Make(schema_result.ValueOrDie(),
                                 std::vector<std::shared_ptr<arrow::Array>>{});
        }
      }
    }
    return table_;
  }

  // void add_table(std::string schema_name, std::shared_ptr<arrow::Table>
  // table) {
  //   tables_[schema_name] = std::move(table);
  // }

  void set_tables(
      std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables) {
    tables_ = std::move(tables);
  }

  void set_aliases(std::unordered_map<std::string, std::string> aliases) {
    aliases_ = std::move(aliases);
  }

  const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& tables()
      const {
    return tables_;
  }

  void set_connections(
      std::map<int64_t, std::vector<GraphConnection>> connections) {
    connections_ = std::move(connections);
  }

  void set_node_manager(const std::shared_ptr<NodeManager>& node_manager) {
    node_manager_ = node_manager;
  }

  void set_schema_registry(
      const std::shared_ptr<SchemaRegistry>& schema_registry) {
    schema_registry_ = schema_registry;
  }

  void set_ids(std::unordered_map<std::string, std::set<int64_t>> ids) {
    _ids = std::move(ids);
  }

  [[nodiscard]] const std::unordered_map<std::string, std::set<int64_t>>& ids()
      const {
    return _ids;
  }

  // Build a schema for the denormalized table that combines all connected
  // tables
  arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema()
      const;

  // Populate the denormalized table with data from all connected nodes
  arrow::Result<std::shared_ptr<arrow::Table>> populate_denormalized_table(
      const std::shared_ptr<arrow::Schema>& schema) const;

 private:
  std::vector<std::shared_ptr<Node>> nodes_;
  mutable std::shared_ptr<arrow::Table> table_;

  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
  std::unordered_map<std::string, std::set<int64_t>> _ids;
  std::unordered_map<std::string, std::string> aliases_;
  std::map<int64_t, std::vector<GraphConnection>> connections_;
  std::shared_ptr<NodeManager> node_manager_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
};

}  // namespace tundradb

#endif  // QUERY_HPP
