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

struct GraphConnection {
  std::string source;
  int64_t source_id;
  std::string edge_type;
  std::string label;
  std::string target;
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

  const std::string& field() const { return field_; }
  CompareOp op() const { return op_; }
  const Value& value() const { return value_; }
};

class Traverse : public Clause {
 private:
  std::string source_;
  std::string edge_type_;
  std::string label_;
  std::set<std::string> target_schema_;

 public:
  Traverse(std::string source, std::string edge_type, std::string label,
           std::set<std::string> target_schema = std::set<std::string>())
      : source_(std::move(source)),
        edge_type_(std::move(edge_type)),
        label_(std::move(label)),
        target_schema_(std::move(target_schema)) {}

  Type type() const override { return Type::TRAVERSE; }

  const std::string& source() const { return source_; }
  const std::string& edge_type() const { return edge_type_; }
  const std::string& label() const { return label_; }
  const std::set<std::string>& target_schema() const { return target_schema_; }
};

class Query {
 private:
  std::string from_schema_;
  std::vector<std::shared_ptr<Clause>> clauses_;

  // Constructor used by Builder
  Query(std::string from_schema, std::vector<std::shared_ptr<Clause>> clauses)
      : from_schema_(std::move(from_schema)), clauses_(std::move(clauses)) {}

 public:
  class Builder;
  const std::string& from_schema() const { return from_schema_; }
  const std::vector<std::shared_ptr<Clause>>& clauses() const {
    return clauses_;
  }

  // Builder creation
  static Builder from_schema(std::string schema) {
    return Builder(std::move(schema));
  }

  // Builder class
  class Builder {
   private:
    std::string from_schema_;
    std::vector<std::shared_ptr<Clause>> clauses_;

   public:
    explicit Builder(std::string schema) : from_schema_(std::move(schema)) {}

    Builder& where(std::string field, CompareOp op, Value value) {
      clauses_.push_back(
          std::make_shared<Where>(std::move(field), op, std::move(value)));
      return *this;
    }

    Builder& traverse(
        std::string source, std::string edge_type, std::string label,
        std::set<std::string> target_schema = std::set<std::string>()) {
      clauses_.push_back(std::make_shared<Traverse>(
          std::move(source), std::move(edge_type), std::move(label),
          std::move(target_schema)));
      return *this;
    }

    // Additional builder methods for other clause types

    Query build() { return Query(from_schema_, std::move(clauses_)); }
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
      // Create a simple table with our schema
      auto schema_result = build_denormalized_schema();
      if (schema_result.ok()) {
        // For now, just return an empty table with the right schema
        table_ =
            arrow::Table::Make(schema_result.ValueOrDie(),
                               std::vector<std::shared_ptr<arrow::Array>>{});
      }
    }
    return table_;
  }

  void add_table(std::string schema_name, std::shared_ptr<arrow::Table> table) {
    tables_[schema_name] = std::move(table);
  }

  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables()
      const {
    return tables_;
  }

  void set_connections(
      const std::map<int64_t, std::vector<GraphConnection>>& connections) {
    connections_ = connections;
  }

  void set_node_manager(const std::shared_ptr<NodeManager>& node_manager) {
    node_manager_ = node_manager;
  }
  void set_schema_registry(
      const std::shared_ptr<SchemaRegistry>& schema_registry) {
    schema_registry_ = schema_registry;
  }

  // Build a schema for the denormalized table that combines all connected
  // tables
  arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema()
      const;

 private:
  std::vector<std::shared_ptr<Node>> nodes_;
  mutable std::shared_ptr<arrow::Table> table_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
  std::map<int64_t, std::vector<GraphConnection>> connections_;
  std::shared_ptr<NodeManager> node_manager_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
};

}  // namespace tundradb

#endif  // QUERY_HPP
