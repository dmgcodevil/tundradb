#ifndef QUERY_HPP
#define QUERY_HPP

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "node.hpp"
#include "types.hpp"

namespace tundradb {

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
  std::string edge_type_;
  std::set<std::string> target_schema_;

 public:
  Traverse(std::string edge_type,
           std::set<std::string> target_schema = std::set<std::string>())
      : edge_type_(std::move(edge_type)),
        target_schema_(std::move(target_schema)) {}

  Type type() const override { return Type::TRAVERSE; }

  const std::string& edge_type() const { return edge_type_; }
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
        std::string edge_type,
        std::set<std::string> target_schema = std::set<std::string>()) {
      clauses_.push_back(std::make_shared<Traverse>(std::move(edge_type),
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
  std::shared_ptr<arrow::Table> as_table() const { return table_; }

 private:
  std::vector<std::shared_ptr<Node>> nodes_;
  std::shared_ptr<arrow::Table> table_;
};

}  // namespace tundradb

#endif  // QUERY_HPP
