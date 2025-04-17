#ifndef QUERY_HPP
#define QUERY_HPP

#include <string>
#include <vector>
#include <memory>
#include <optional>

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

class QueryBuilder {
public:

    static QueryBuilder from_schema(std::string schema_name) {
        QueryBuilder builder;
        builder.schema_name_ = std::move(schema_name);
        return builder;
    }

    static QueryBuilder from_node(int64_t node_id) {
        QueryBuilder builder;
        builder.node_id_ = node_id;
        return builder;
    }


    QueryBuilder& where(std::string field, CompareOp op, Value value) {
        conditions_.push_back({std::move(field), op, std::move(value)});
        return *this;
    }

    QueryBuilder& traverse(std::string edge_type,
                         std::optional<std::string> target_schema = std::nullopt) {
        traversals_.push_back({std::move(edge_type), std::move(target_schema)});
        return *this;
    }

    QueryBuilder& select(std::vector<std::string> fields) {
        projection_fields_ = std::move(fields);
        return *this;
    }

    // Pagination
    QueryBuilder& limit(size_t limit_val) {
        limit_ = limit_val;
        return *this;
    }

    QueryBuilder& skip(size_t skip_val) {
        skip_ = skip_val;
        return *this;
    }

    // Order by
    QueryBuilder& order_by(std::string field, bool ascending = true) {
        order_fields_.push_back({std::move(field), ascending});
        return *this;
    }


private:
    // Query components
    std::string schema_name_;
    std::optional<int64_t> node_id_;

    // Conditions
    struct Condition {
        std::string field;
        CompareOp op;
        Value value;
    };
    std::vector<Condition> conditions_;

    // Traversal specifications
    struct Traversal {
        std::string edge_type;
        std::optional<std::string> target_schema;
    };
    std::vector<Traversal> traversals_;

    // Projection and sorting
    std::vector<std::string> projection_fields_;
    std::vector<std::pair<std::string, bool>> order_fields_;

    // Pagination
    std::optional<size_t> limit_;
    std::optional<size_t> skip_;
};

// Result handling
class QueryResult {
public:
    // Get a vector of matching nodes
    std::vector<std::shared_ptr<Node>> nodes() const {
        return nodes_;
    }

    // Get the first node or null
    std::shared_ptr<Node> first() const {
        return nodes_.empty() ? nullptr : nodes_[0];
    }

    // Get count of results
    size_t count() const {
        return nodes_.size();
    }

    // Access as Arrow Table
    std::shared_ptr<arrow::Table> as_table() const {
        return table_;
    }

private:
    std::vector<std::shared_ptr<Node>> nodes_;
    std::shared_ptr<arrow::Table> table_;
};

} // namespace tundradb

#endif //QUERY_HPP
