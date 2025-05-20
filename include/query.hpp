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
  bool declaration;

 public:
  [[nodiscard]] std::string schema() const { return schema_; }
  [[nodiscard]] std::string value() const { return value_; }
  [[nodiscard]] bool is_declaration() const { return declaration; }

  static SchemaRef parse(const std::string& s) {
    SchemaRef r;
    size_t pos = s.find(':');
    if (pos == std::string::npos) {
      r.schema_ = s;
      r.value_ = s;
      r.declaration = false;
    } else {
      r.value_ = s.substr(0, pos);
      r.schema_ = s.substr(pos + 1);
      r.declaration = true;
    }
    return r;
  }

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    if (declaration) {
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

enum class TraverseType { Inner, Left, Right, Full };

class Traverse : public Clause {
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

  Type type() const override { return Type::TRAVERSE; }

  [[nodiscard]] const SchemaRef& source() const { return source_; }
  [[nodiscard]] const std::string& edge_type() const { return edge_type_; }
  [[nodiscard]] const SchemaRef& target() const { return target_; }
  [[nodiscard]] TraverseType traverse_type() const { return traverse_type_; }
};

struct Select : public Clause {
  std::vector<std::string> fields_;

 public:
  Select(std::vector<std::string> fields) : fields_(std::move(fields)) {}

  Type type() const override { return Type::SELECT; }

  [[nodiscard]] const std::vector<std::string>& fields() const {
    return fields_;
  }
};

class Query {
 private:
  SchemaRef from_;
  std::vector<std::shared_ptr<Clause>> clauses_;
  std::shared_ptr<Select> select_;

  // Constructor used by Builder
  Query(SchemaRef from, std::vector<std::shared_ptr<Clause>> clauses, std::shared_ptr<Select> select)
      : from_(std::move(from)), clauses_(std::move(clauses)), select_(std::move(select)) {}

 public:
  class Builder;
  const SchemaRef& from() const { return from_; }
  const std::vector<std::shared_ptr<Clause>>& clauses() const {
    return clauses_;
  }
  [[nodiscard]] const std::shared_ptr<Select>& select() const {
    return select_;
  }

  // Builder creation
  static Builder from(std::string schema) { return Builder(std::move(schema)); }

  // Builder class
  class Builder {
   private:
    SchemaRef from_;
    std::vector<std::shared_ptr<Clause>> clauses_;
    std::shared_ptr<Select> select_;

   public:
    explicit Builder(std::string schema) : from_(SchemaRef::parse(schema)) {}

    Builder& where(std::string field, CompareOp op, Value value) {
      clauses_.push_back(
          std::make_shared<Where>(std::move(field), op, std::move(value)));
      return *this;
    }

    Builder& traverse(std::string source, std::string edge_type,
                      std::string target,
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

    // Additional builder methods for other clause types

    Query build() { return Query(from_, std::move(clauses_), std::move(select_)); }
  };
};

// Result handling
class QueryResult {
 public:
  // Get a vector of matching nodes

  [[nodiscard]] std::shared_ptr<arrow::Table> table() const { return table_; }
  void set_table(const std::shared_ptr<arrow::Table>& table) { table_ = table; }

 private:
  std::shared_ptr<arrow::Table> table_;
};

}  // namespace tundradb

#endif  // QUERY_HPP
