#include "../include/core.hpp"

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/datum.h>
#include <arrow/table.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

#include <chrono>
#include <future>
#include <iostream>
#include <list>
#include <memory>
#include <queue>
#include <ranges>
#include <stack>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "container_concepts.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

namespace tundradb {

// Utility function to join containers using C++23 ranges
template <typename Container>
std::string join_container(const Container& container,
                           std::string_view delimiter = ", ") {
  return [&]() {
    std::ostringstream oss;
    for (auto it = container.begin(); it != container.end(); ++it) {
      oss << (it != container.begin() ? delimiter : "") << *it;
    }
    return oss.str();
  }();
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

// Convert Value to Arrow scalar for builders
arrow::Result<std::shared_ptr<arrow::Scalar>> value_to_arrow_scalar(
    const Value& value) {
  switch (value.type()) {
    case ValueType::INT32:
      return arrow::MakeScalar(value.as_int32());
    case ValueType::INT64:
      return arrow::MakeScalar(value.as_int64());
    case ValueType::DOUBLE:
      return arrow::MakeScalar(value.as_double());
    case ValueType::STRING:
      return arrow::MakeScalar(value.as_string());
    case ValueType::BOOL:
      return arrow::MakeScalar(value.as_bool());
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          tundradb::to_string(value.type()));
  }
}

arrow::Result<std::shared_ptr<arrow::Scalar>> value_ptr_to_arrow_scalar(
    const char* ptr, const ValueType type) {
  switch (type) {
    case ValueType::INT32:
      return arrow::MakeScalar(*reinterpret_cast<const int32_t*>(ptr));
    case ValueType::INT64:
      return arrow::MakeScalar(*reinterpret_cast<const int64_t*>(ptr));
    case ValueType::DOUBLE:
      return arrow::MakeScalar(*reinterpret_cast<const double*>(ptr));
    case ValueType::STRING: {
      auto str_ref = *reinterpret_cast<const StringRef*>(ptr);
      return arrow::MakeScalar(str_ref.to_string());
    }
    case ValueType::BOOL:
      return arrow::MakeScalar(*reinterpret_cast<const bool*>(ptr));
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          tundradb::to_string(type));
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

// Recursively convert WhereExpr to Arrow compute expression
arrow::compute::Expression where_condition_to_expression(
    const WhereExpr& condition, bool strip_var) {
  // Use the unified to_arrow_expression() method from WhereExpr
  return condition.to_arrow_expression(strip_var);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_nodes(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes) {
  log_debug("Creating table from {} nodes with schema '{}'", nodes.size(),
            schema->ToString());

  // Create builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : schema->fields()) {
    log_debug("Creating builder for field '{}' with type {}", field->name(),
              field->type()->ToString());
    auto builder_result = arrow::MakeBuilder(field->type());
    if (!builder_result.ok()) {
      log_error("Failed to create builder for field '{}': {}", field->name(),
                builder_result.status().ToString());
      return builder_result.status();
    }
    builders.push_back(std::move(builder_result.ValueOrDie()));
  }

  // Populate builders with data from each node
  log_debug("Adding data from {} nodes to builders", nodes.size());
  for (const auto& node : nodes) {
    // Add each field's value to the appropriate builder
    for (int i = 0; i < schema->num_fields(); i++) {
      auto field = schema->field(i);
      const auto& field_name = field->name();

      // Find the value in the node's data
      auto res = node->get_value_ptr(field_name);
      if (res.ok()) {
        // Convert Value to Arrow scalar and append to builder
        auto value = res.ValueOrDie();
        if (value) {
          auto scalar_result = value_ptr_to_arrow_scalar(value, arrow_type_to_value_type(field->type()));
          if (!scalar_result.ok()) {
            log_error("Failed to convert value to scalar for field '{}': {}",
                      field_name, scalar_result.status().ToString());
            return scalar_result.status();
          }

          auto scalar = scalar_result.ValueOrDie();
          auto status = builders[i]->AppendScalar(*scalar);
          if (!status.ok()) {
            log_error("Failed to append scalar for field '{}': {}", field_name,
                      status.ToString());
            return status;
          }
        } else {
          log_debug("Null value for field '{}', appending null", field_name);
          auto status = builders[i]->AppendNull();
          if (!status.ok()) {
            log_error("Failed to append null for field '{}': {}", field_name,
                      status.ToString());
            return status;
          }
        }
      } else {
        log_debug("Field '{}' not found in node, appending null", field_name);
        auto status = builders[i]->AppendNull();
        if (!status.ok()) {
          log_error("Failed to append null for field '{}': {}", field_name,
                    status.ToString());
          return status;
        }
      }
    }
  }

  // Finish building arrays
  log_debug("Finalizing arrays from builders");
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());
  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    auto status = builder->Finish(&array);
    if (!status.ok()) {
      log_error("Failed to finish array builder: {}", status.ToString());
      return status;
    }
    arrays.push_back(array);
  }

  // Create table
  log_debug("Creating table with {} rows and {} columns",
            arrays.empty() ? 0 : arrays[0]->length(), arrays.size());
  return arrow::Table::Make(schema, arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> filter(
    std::shared_ptr<arrow::Table> table, const WhereExpr& condition,
    bool strip_var) {
  log_debug("Filtering table with WhereCondition: {}", condition.toString());

  try {
    // Convert WhereCondition to Arrow compute expression
    auto filter_expr = where_condition_to_expression(condition, strip_var);

    log_debug("Creating in-memory dataset from table with {} rows",
              table->num_rows());
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);

    // Create scanner builder
    log_debug("Creating scanner builder");
    auto scan_builder_result = dataset->NewScan();
    if (!scan_builder_result.ok()) {
      log_error("Failed to create scanner builder: {}",
                scan_builder_result.status().ToString());
      return scan_builder_result.status();
    }
    auto scan_builder = scan_builder_result.ValueOrDie();

    log_debug("Applying compound filter to scanner builder");
    auto filter_status = scan_builder->Filter(filter_expr);
    if (!filter_status.ok()) {
      log_error("Failed to apply filter: {}", filter_status.ToString());
      return filter_status;
    }

    log_debug("Finishing scanner");
    auto scanner_result = scan_builder->Finish();
    if (!scanner_result.ok()) {
      log_error("Failed to finish scanner: {}",
                scanner_result.status().ToString());
      return scanner_result.status();
    }
    auto scanner = scanner_result.ValueOrDie();

    log_debug("Executing scan to table");
    auto table_result = scanner->ToTable();
    if (!table_result.ok()) {
      log_error("Failed to convert scan results to table: {}",
                table_result.status().ToString());
      return table_result.status();
    }

    auto result_table = table_result.ValueOrDie();
    log_debug("Filter completed: {} rows in, {} rows out", table->num_rows(),
              result_table->num_rows());
    return result_table;

  } catch (const std::exception& e) {
    log_error("Failed to convert WhereCondition to Arrow expression: {}",
              e.what());
    return arrow::Status::Invalid("Failed to convert WHERE condition: ",
                                  e.what());
  }
}

void debug_connections(
    int64_t id,
    const std::map<int64_t, std::vector<GraphConnection>>& connections,
    std::vector<std::string> path, std::vector<std::string>& res) {
  if (!connections.contains(id)) {
    res.push_back(join_container(path));
    return;
  }
  for (const auto& conn : connections.at(id)) {
    path.push_back(conn.toString());
    debug_connections(conn.target_id, connections, path, res);
  }
}

void print_paths(
    const std::map<int64_t, std::vector<GraphConnection>>& connections) {
  log_debug("Printing all paths in connection graph:");

  if (connections.empty()) {
    log_debug("  No connections found");
    return;
  }

  for (const auto& [source_id, conn_list] : connections) {
    if (conn_list.empty()) {
      log_debug("  Node {} has no outgoing connections", source_id);
      continue;
    }

    for (const auto& conn : conn_list) {
      log_debug("  {} -[{}]-> {}", source_id, conn.edge_type, conn.target_id);
    }
  }

  log_debug("Total of {} source nodes with connections", connections.size());
}

std::set<int64_t> get_roots(
    const std::map<int64_t, std::vector<GraphConnection>>& connections) {
  std::set<int64_t> roots;
  std::unordered_map<int64_t, int64_t> count;
  std::vector<int64_t> stack;
  for (const auto& id : connections | std::views::keys) {
    count[id] = 0;
    stack.push_back(id);
  }

  while (!stack.empty()) {
    auto curr = stack[stack.size() - 1];
    stack.pop_back();

    if (connections.contains(curr)) {
      for (auto const next : connections.at(curr)) {
        count[next.target_id]++;
        stack.push_back(next.target_id);
      }
    }
  }
  for (const auto& [id, c] : count) {
    if (c == 0) {
      roots.insert(id);
    }
  }
  return roots;
}

struct QueryState {
  SchemaRef from;
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables;
  std::unordered_map<std::string, std::set<int64_t>> ids;
  std::unordered_map<std::string, std::string> aliases;
  std::unordered_map<std::string,
                     std::map<int64_t, std::vector<GraphConnection>>>
      connections;  // outgoing

  std::unordered_map<int64_t, std::vector<GraphConnection>> incoming;

  std::shared_ptr<NodeManager> node_manager;
  std::shared_ptr<SchemaRegistry> schema_registry;
  std::vector<Traverse> traversals;

  arrow::Result<std::string> resolve_schema(const SchemaRef& schema_ref) {
    // todo we need to separate functions: assign alias , resolve
    if (aliases.contains(schema_ref.value()) && schema_ref.is_declaration()) {
      log_warn("duplicated schema alias '" + schema_ref.value() +
               "' already assigned to '" + aliases[schema_ref.value()] + "'");
      return aliases[schema_ref.value()];
    }
    if (schema_ref.is_declaration()) {
      aliases[schema_ref.value()] = schema_ref.schema();
      return schema_ref.schema();
    }
    return aliases[schema_ref.value()];
  }

  const std::set<int64_t>& get_ids(const SchemaRef& schema_ref) {
    return ids[schema_ref.value()];
  }

  // removes node_id and updates all connections and ids
  void remove_node(int64_t node_id, const SchemaRef& schema_ref) {
    ids[schema_ref.value()].erase(node_id);
  }

  arrow::Result<bool> update_table(std::shared_ptr<arrow::Table> table,
                                   const SchemaRef& schema_ref) {
    this->tables[schema_ref.value()] = table;
    auto ids_result = get_ids_from_table(table);
    if (!ids_result.ok()) {
      log_error("Failed to get IDs from table: {}", schema_ref.value());
      return ids_result.status();
    }
    ids[schema_ref.value()] = ids_result.ValueOrDie();
    return true;
  }

  bool has_outgoing(const SchemaRef& schema_ref, int64_t node_id) const {
    return connections.contains(schema_ref.value()) &&
           connections.at(schema_ref.value()).contains(node_id) &&
           !connections.at(schema_ref.value()).at(node_id).empty();
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "QueryState {\n";
    ss << "  From: " << from.toString() << "\n";

    ss << "  Tables (" << tables.size() << "):\n";
    for (const auto& [alias, table_ptr] : tables) {
      if (table_ptr) {
        ss << "    - " << alias << ": " << table_ptr->num_rows() << " rows, "
           << table_ptr->num_columns() << " columns\n";
      } else {
        ss << "    - " << alias << ": (nullptr)\n";
      }
    }

    ss << "  IDs (" << ids.size() << "):\n";
    for (const auto& [alias, id_set] : ids) {
      ss << "    - " << alias << ": " << id_set.size() << " IDs\n";
    }

    ss << "  Aliases (" << aliases.size() << "):\n";
    for (const auto& [alias, schema_name] : aliases) {
      ss << "    - " << alias << " -> " << schema_name << "\n";
    }

    ss << "  Connections (Outgoing) (" << connections.size()
       << " source nodes):";
    for (const auto& [from, conns] : connections) {
      for (const auto& [from_id, conn_vec] : conns) {
        ss << "from " << from << ":" << from_id << ":\n";
        for (const auto& conn : conn_vec) {
          ss << "    - " << conn.target.value() << ":" << conn.target_id
             << "\n";
        }
      }
    }

    ss << "  Connections (Incoming) (" << incoming.size() << " target nodes):";
    int target_nodes_printed = 0;
    for (const auto& [target_id, conns_vec] : incoming) {
      if (target_nodes_printed >= 3 &&
          incoming.size() > 5) {  // Limit nodes printed
        ss << "      ... and " << (incoming.size() - target_nodes_printed)
           << " more target nodes ...\n";
        break;
      }
      ss << "    - Target ID " << target_id << " (" << conns_vec.size()
         << " incoming):";
      int conns_printed_for_target = 0;
      for (const auto& conn : conns_vec) {
        if (conns_printed_for_target >= 3 &&
            conns_vec.size() > 5) {  // Limit connections per node
          ss << "        ... and "
             << (conns_vec.size() - conns_printed_for_target)
             << " more connections ...\n";
          break;
        }
        ss << "        <- " << conn.source.value() << ":" << conn.source_id
           << " (via '" << conn.edge_type << "')\n";
        conns_printed_for_target++;
      }
      target_nodes_printed++;
    }

    ss << "  Traversals (" << traversals.size() << "):\n";
    for (size_t i = 0; i < traversals.size(); ++i) {
      const auto& trav = traversals[i];
      ss << "    - [" << i << "]: " << trav.source().value() << " -["
         << trav.edge_type() << "]-> " << trav.target().value() << " (Type: "
         << (trav.traverse_type() == TraverseType::Inner ? "Inner" : "Other")
         << ")\n";
    }

    ss << "}";
    return ss.str();
  }
};

arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema(
    const QueryState& query_state) {
  log_debug("Building schema for denormalized table");

  std::set<std::string> processed_fields;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::set<std::string> processed_schemas;

  // First add fields from the FROM schema
  std::string from_schema = query_state.from.value();

  log_debug("Adding fields from FROM schema '{}'", from_schema);

  auto schema_result = query_state.schema_registry->get_arrow(
      query_state.aliases.at(from_schema));
  if (!schema_result.ok()) {
    return schema_result.status();
  }

  auto arrow_schema = schema_result.ValueOrDie();
  for (const auto& field : arrow_schema->fields()) {
    std::string prefixed_field_name = from_schema + "." + field->name();
    processed_fields.insert(prefixed_field_name);
    fields.push_back(arrow::field(prefixed_field_name, field->type()));
  }
  processed_schemas.insert(from_schema);

  std::vector<SchemaRef> unique_schemas;
  for (const auto& traverse : query_state.traversals) {
    if (processed_schemas.insert(traverse.source().value()).second) {
      unique_schemas.push_back(traverse.source());
    }
    if (processed_schemas.insert(traverse.target().value()).second) {
      unique_schemas.push_back(traverse.target());
    }
  }

  for (const auto& schema_ref : unique_schemas) {
    log_debug("Adding fields from schema '{}'", schema_ref.value());

    schema_result = query_state.schema_registry->get_arrow(
        query_state.aliases.at(schema_ref.value()));
    if (!schema_result.ok()) {
      return schema_result.status();
    }

    arrow_schema = schema_result.ValueOrDie();
    for (const auto& field : arrow_schema->fields()) {
      std::string prefixed_field_name =
          schema_ref.value() + "." + field->name();
      if (processed_fields.contains(prefixed_field_name)) {
        return arrow::Status::KeyError("Field '{}' already exists",
                                       prefixed_field_name);
      }

      processed_fields.insert(prefixed_field_name);
      fields.push_back(arrow::field(prefixed_field_name, field->type()));
    }
  }

  return std::make_shared<arrow::Schema>(fields);
}

struct PathSegment {
  std::string schema;
  int64_t node_id;

  std::string toString() const {
    return schema + ":" + std::to_string(node_id);
  }

  bool operator==(const PathSegment& other) const {
    return schema == other.schema && node_id == other.node_id;
  }
};

bool is_prefix(const std::vector<PathSegment>& prefix,
               const std::vector<PathSegment>& path) {
  if (prefix.size() > path.size()) {
    return false;
  }
  int i = 0;
  while (i < prefix.size()) {
    if (prefix[i] != path[i]) return false;
    i++;
  }
  return true;
}

std::string join_schema_path(const std::vector<PathSegment>& schema_path) {
  std::ostringstream oss;
  for (size_t i = 0; i < schema_path.size(); ++i) {
    if (i != 0) oss << "->";
    oss << schema_path[i].toString();
  }
  return oss.str();
}

struct Row {
  int64_t id;
  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> cells;
  std::vector<PathSegment> path;

  void set_cell(const std::string& name,
                std::shared_ptr<arrow::Scalar> scalar) {
    cells[name] = std::move(scalar);
  }

  bool has_value(const std::string& name) const {
    return cells.contains(name) && cells.at(name) != nullptr &&
           cells.at(name)->is_valid;
  }

  void set_cell_from_node(const SchemaRef& schema_ref,
                          const std::shared_ptr<Node>& node) {
    for (const auto& field : node->get_schema()->fields()) {
      auto full_name = schema_ref.value() + "." + field->name();
      this->set_cell(full_name, node->get_value_ptr(field->name()).ValueOrDie(),
                     field->type());
    }
  }

  // New set_cell method for Value objects
  void set_cell(const std::string& name, const char* ptr,
                const ValueType type) {
    if (ptr) {
      auto scalar_result = value_ptr_to_arrow_scalar(ptr, type);
      if (scalar_result.ok()) {
        cells[name] = scalar_result.ValueOrDie();
        return;
      }
    }

    // Default to null if value is null or conversion fails
    cells[name] = nullptr;
  }

  void set_cell(const std::string& name, std::shared_ptr<arrow::Array> array) {
    if (array && array->length() > 0) {
      auto scalar_result = array->GetScalar(0);
      if (scalar_result.ok()) {
        cells[name] = scalar_result.ValueOrDie();
        return;
      }
    }

    // Default to null if array is empty or conversion fails
    cells[name] = nullptr;
  }

  bool start_with(const std::vector<PathSegment>& prefix) const {
    return is_prefix(prefix, this->path);
  }

  std::unordered_map<std::string, int64_t> extract_schema_ids() const {
    std::unordered_map<std::string, int64_t> result;
    for (const auto& [field_name, value] : cells) {
      if (!value || !value->is_valid) continue;

      // Extract schema prefix (everything before the first dot)
      size_t dot_pos = field_name.find('.');
      if (dot_pos != std::string::npos) {
        std::string schema = field_name.substr(0, dot_pos);

        // Store ID for this schema if it's an ID field
        if (field_name.substr(dot_pos + 1) == "id") {
          auto id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(value);
          result[schema] = id_scalar->value;
        }
      }
    }
    return result;
  }

  // returns new Row which is result of merging this row and other
  [[nodiscard]] Row merge(const Row& other) const {
    Row merged = *this;
    for (const auto& [name, value] : other.cells) {
      if (!merged.has_value(name)) {
        merged.cells[name] = value;
      }
    }
    return merged;
  }

  [[nodiscard]] std::string ToString() const {
    std::stringstream ss;
    ss << "Row{";
    ss << "path='" << join_schema_path(path) << "', ";

    bool first = true;
    for (const auto& [field_name, scalar] : cells) {
      if (!first) {
        ss << ", ";
      }
      first = false;

      ss << field_name << ": ";

      if (!scalar) {
        ss << "NULL";
      } else if (scalar->is_valid) {
        // Handle different scalar types appropriately
        switch (scalar->type->id()) {
          case arrow::Type::INT64:
            ss << std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
            break;
          case arrow::Type::DOUBLE:
            ss << std::static_pointer_cast<arrow::DoubleScalar>(scalar)->value;
            break;
          case arrow::Type::STRING:
          case arrow::Type::LARGE_STRING:
            ss << "\""
               << std::static_pointer_cast<arrow::StringScalar>(scalar)->view()
               << "\"";
            break;
          case arrow::Type::BOOL:
            ss << (std::static_pointer_cast<arrow::BooleanScalar>(scalar)->value
                       ? "true"
                       : "false");
            break;
          default:
            ss << scalar->ToString();
            break;
        }
      } else {
        ss << "NULL";
      }
    }

    ss << "}";
    return ss.str();
  }
};

static Row create_empty_row_from_schema(
    const std::shared_ptr<arrow::Schema>& final_output_schema) {
  Row new_row;
  for (const auto& field : final_output_schema->fields()) {
    // Create a null scalar of the correct type
    auto null_scalar = arrow::MakeNullScalar(field->type());
    if (null_scalar != nullptr) {
      new_row.cells[field->name()] = null_scalar;
    } else {
      // If creating a null scalar fails, use nullptr as a fallback
      new_row.cells[field->name()] = nullptr;
      log_warn("Failed to create null scalar for field '{}' with type '{}'",
               field->name(), field->type()->ToString());
    }
  }
  return new_row;
}

struct RowNode;

std::vector<Row> get_child_rows(const Row& parent,
                                const std::vector<Row>& rows) {
  std::vector<Row> child;
  for (const auto& row : rows) {
    if (parent.id != row.id && row.start_with(parent.path)) {
      child.push_back(row);
    }
  }
  return child;
}

struct RowNode {
  std::optional<Row> row;
  int depth;
  PathSegment path_segment;
  std::vector<std::unique_ptr<RowNode>> children;

  RowNode() : depth(0), path_segment{"", -1} {}

  RowNode(std::optional<Row> r, int d,
          std::vector<std::unique_ptr<RowNode>> c = {})
      : row(std::move(r)),
        depth(d),
        path_segment{"", -1},
        children(std::move(c)) {}

  bool leaf() const { return row.has_value(); }

  void insert_row_dfs(size_t path_idx, const Row& new_row) {
    if (path_idx == new_row.path.size()) {
      this->row = new_row;
      return;
    }

    for (const auto& n : children) {
      if (n->path_segment == new_row.path[path_idx]) {
        n->insert_row_dfs(path_idx + 1, new_row);
        return;
      }
    }

    auto new_node = std::make_unique<RowNode>();
    new_node->depth = depth + 1;
    new_node->path_segment = new_row.path[path_idx];
    new_node->insert_row_dfs(path_idx + 1, new_row);
    children.emplace_back(std::move(new_node));
  }

  void insert_row(const Row& new_row) { insert_row_dfs(0, new_row); }

  std::vector<Row> merge_rows() {
    if (this->leaf()) {
      return {this->row.value()};
    }

    // collect all records from child node and group them by schema
    std::unordered_map<std::string, std::vector<Row>> grouped;
    for (const auto& c : children) {
      auto child_rows = c->merge_rows();
      grouped[c->path_segment.schema].insert(
          grouped[c->path_segment.schema].end(), child_rows.begin(),
          child_rows.end());
    }

    std::vector<std::vector<Row>> groups_for_product;
    // Add this->row as its own group (that is important for cartesian product)
    // if it exists and has data,
    // to represent the node itself if it should be part of the product
    // independently.
    if (this->row.has_value()) {
      Row node_self_row = this->row.value();
      // Normalize path for the node's own row to ensure it combines correctly
      // and doesn't carry a longer BFS path if it was a leaf of BFS.
      // i.e. current node path can be a:0->b:1->c:2
      // this code sets it to 'c:2'
      node_self_row.path = {this->path_segment};
      groups_for_product.push_back({node_self_row});
    }

    for (const auto& pair : grouped) {
      if (!pair.second.empty()) {
        groups_for_product.push_back(pair.second);
      }
    }

    if (groups_for_product.empty()) {
      return {};
    }
    // If only one group (e.g. only this->row.value() or only one child branch
    // with data), no Cartesian product is needed. Just return its rows, but
    // ensure paths are correct.
    if (groups_for_product.size() == 1) {
      std::vector<Row> single_group_rows = groups_for_product[0];
      // Ensure path is normalized for these rows if they came from children
      // For rows that are just this->row.value(), path is already set.
      // This might be too aggressive if child rows are already fully merged
      // products. For now, let's assume rows from c->merge_rows() are final
      // products of that child branch.
      return single_group_rows;
    }

    std::vector<Row> final_merged_rows = groups_for_product.back();
    for (int i = static_cast<int>(groups_for_product.size()) - 2; i >= 0; --i) {
      std::vector<Row> temp_product_accumulator;
      for (const auto& r1_from_current_group : groups_for_product[i]) {
        for (const auto& r2_from_previous_product : final_merged_rows) {
          // Check for conflicts in shared variables between rows
          bool can_merge = true;

          // Get variable prefixes (schema names) from cells
          std::unordered_map<std::string, int64_t> schema_ids_r1 =
              r1_from_current_group.extract_schema_ids();
          std::unordered_map<std::string, int64_t> schema_ids_r2 =
              r2_from_previous_product.extract_schema_ids();

          // Check for conflicts - same schema name but different IDs
          for (const auto& [schema, id1] : schema_ids_r1) {
            if (schema_ids_r2.contains(schema) &&
                schema_ids_r2[schema] != id1) {
              // Found a conflict - same schema but different IDs
              if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
                log_debug(
                    "Conflict detected: Schema '{}' has different IDs: {} vs "
                    "{}",
                    schema, id1, schema_ids_r2[schema]);
              }
              can_merge = false;
              break;
            }
          }

          // Additional cell-by-cell check for conflicts
          if (can_merge) {
            for (const auto& [field_name, value1] :
                 r1_from_current_group.cells) {
              if (!value1 || !value1->is_valid) continue;

              auto it = r2_from_previous_product.cells.find(field_name);
              if (it != r2_from_previous_product.cells.end() && it->second &&
                  it->second->is_valid) {
                // Both rows have this field with non-null values - check if
                // they match
                if (!value1->Equals(*(it->second))) {
                  if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
                    log_debug(
                        "Conflict detected: Field '{}' has different values",
                        field_name);
                  }
                  can_merge = false;
                  break;
                }
              }
            }
          }

          if (can_merge) {
            Row merged_r =
                r1_from_current_group.merge(r2_from_previous_product);
            // Set the path of the newly merged row to the path of the current
            // RowNode
            merged_r.path = {this->path_segment};
            temp_product_accumulator.push_back(merged_r);
          }
        }
      }
      final_merged_rows = std::move(temp_product_accumulator);
      if (final_merged_rows.empty()) {
        log_debug("product_accumulator is empty. stop merge");
        break;
      }
    }
    return final_merged_rows;
  }

  std::string toString(bool recursive = true, int indent_level = 0) const {
    // Helper to build indentation string based on level
    auto get_indent = [](int level) { return std::string(level * 2, ' '); };

    std::stringstream ss;
    std::string indent = get_indent(indent_level);

    // Print basic node info
    ss << indent << "RowNode [path=" << path_segment.toString()
       << ", depth=" << depth << "] {\n";

    // Print Row
    if (row.has_value()) {
      ss << indent << "  Path: ";
      if (row.value().path.empty()) {
        ss << "(empty)";
      } else {
        for (size_t i = 0; i < row.value().path.size(); ++i) {
          if (i > 0) ss << " → ";
          ss << row.value().path[i].schema << ":"
             << row.value().path[i].node_id;
        }
      }
      ss << "\n";

      // Print key cell values (limited to avoid overwhelming output)
      ss << indent << "  Cells: ";
      if (row.value().cells.empty()) {
        ss << "(empty)";
      } else {
        size_t count = 0;
        ss << "{ ";
        for (const auto& [key, value] : row.value().cells) {
          if (count++ > 0) ss << ", ";
          if (count > 5) {  // Limit display
            ss << "... +" << (row.value().cells.size() - 5) << " more";
            break;
          }

          ss << key << ": ";
          if (!value) {
            ss << "NULL";
          } else {
            ss << value->ToString();  // Assuming arrow::Scalar has ToString()
          }
        }
        ss << " }";
      }
    }

    ss << "\n";

    // Print children count
    ss << indent << "  Children: " << children.size() << "\n";

    // Recursively print children if requested
    if (recursive && !children.empty()) {
      ss << indent << "  [\n";
      for (const auto& child : children) {
        if (child) {
          ss << child->toString(true, indent_level + 2);
        } else {
          ss << get_indent(indent_level + 2) << "(null child)\n";
        }
      }
      ss << indent << "  ]\n";
    }

    ss << indent << "}\n";
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os, const RowNode& node) {
    return os << node.toString();
  }

  void print(bool recursive = true) const { log_debug(toString(recursive)); }
};

struct QueueItem {
  int64_t node_id;
  SchemaRef schema_ref;
  int level;
  std::shared_ptr<Row> row;
  std::set<std::string>
      path_visited_nodes;  // schema:node visited in this specific path
  std::vector<PathSegment> path;

  QueueItem(int64_t id, const SchemaRef& schema, int l, std::shared_ptr<Row> r)
      : node_id(id), schema_ref(schema), level(l), row(std::move(r)) {
    path_visited_nodes.insert(schema_ref.value() + ":" + std::to_string(id));
    path.push_back(PathSegment{schema.value(), id});
  }
};

// Log grouped connections for a node
void log_grouped_connections(
    int64_t node_id,
    const std::unordered_map<std::string, std::vector<GraphConnection>>&
        grouped_connections) {
  if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
    if (grouped_connections.empty()) {
      log_debug("Node {} has no grouped connections", node_id);
      return;
    }

    log_debug("Node {} has connections to {} target schemas:", node_id,
              grouped_connections.size());

    for (const auto& [target_schema, connections] : grouped_connections) {
      log_debug("  To schema '{}': {} connections", target_schema,
                connections.size());

      for (size_t i = 0; i < connections.size(); ++i) {
        const auto& conn = connections[i];
        log_debug("    [{}] {} -[{}]-> {}.{} (target_id: {})", i,
                  conn.source.value(), conn.edge_type, conn.target.value(),
                  conn.target.schema(), conn.target_id);
      }
    }
  }
}

template <StringSet VisitedSet>
arrow::Result<std::shared_ptr<std::vector<Row>>> populate_rows_bfs(
    int64_t node_id, const SchemaRef& start_schema,
    const std::shared_ptr<arrow::Schema>& output_schema,
    const QueryState& query_state, VisitedSet& global_visited) {
  log_debug("populate_rows_bfs::node={}:{}", start_schema.value(), node_id);
  auto result = std::make_shared<std::vector<Row>>();
  int64_t row_id_counter = 0;
  auto initial_row =
      std::make_shared<Row>(create_empty_row_from_schema(output_schema));

  std::queue<QueueItem> queue;
  queue.emplace(node_id, start_schema, 0, initial_row);

  while (!queue.empty()) {
    auto size = queue.size();
    while (size-- > 0) {
      auto item = queue.front();
      queue.pop();
      auto node = query_state.node_manager->get_node(item.node_id).ValueOrDie();
      item.row->set_cell_from_node(item.schema_ref, node);
      std::string schema_node_key =
          item.schema_ref.value() + ":" + std::to_string(item.node_id);
      global_visited.insert(schema_node_key);
      item.path_visited_nodes.insert(schema_node_key);

      // group connections by target schema
      std::unordered_map<std::string, std::vector<GraphConnection>>
          grouped_connections;

      bool skip = false;
      if (query_state.has_outgoing(item.schema_ref, item.node_id)) {
        for (const auto& conn :
             query_state.connections.at(item.schema_ref.value())
                 .at(item.node_id)) {
          std::string schema_node_key =
              conn.target.value() + ":" + std::to_string(conn.target_id);
          if (!item.path_visited_nodes.contains(schema_node_key)) {
            if (query_state.ids.at(conn.target.value())
                    .contains(conn.target_id)) {
              grouped_connections[conn.target.value()].push_back(conn);
            } else {
              skip = true;
            }
          }
        }
      }
      log_grouped_connections(item.node_id, grouped_connections);

      if (grouped_connections.empty()) {
        // we've done
        if (!skip) {
          auto r = *item.row;
          r.path = item.path;
          r.id = row_id_counter++;
          if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
            log_debug("add row: {}", r.ToString());
          }
          result->push_back(r);
        }

      } else {
        for (const auto& pair : grouped_connections) {
          const auto& connections = pair.second;
          if (connections.size() == 1) {
            // continue the path
            auto conn = connections[0];
            auto next =
                QueueItem(connections[0].target_id, connections[0].target,
                          item.level + 1, item.row);

            next.path = item.path;
            next.path.push_back(PathSegment{connections[0].target.value(),
                                            connections[0].target_id});
            if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
              log_debug("continue the path: {}", join_schema_path(next.path));
            }
            queue.push(next);
          } else {
            for (const auto& conn : connections) {
              auto next_row = std::make_shared<Row>(*item.row);
              auto next = QueueItem(conn.target_id, conn.target, item.level + 1,
                                    next_row);
              next.path = item.path;
              next.path.push_back(
                  PathSegment{conn.target.value(), conn.target_id});
              if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
                log_debug("create a new path {}, node={}",
                          join_schema_path(next.path), conn.target_id);
              }
              queue.push(next);
            }
          }
        }
      }
    }
  }
  RowNode tree;
  tree.path_segment = PathSegment{"root", -1};
  for (const auto& r : *result) {
    if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
      log_debug("bfs result: {}", r.ToString());
    }
    tree.insert_row(r);
  }
  tree.print();
  auto merged = tree.merge_rows();
  if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
    for (const auto& row : merged) {
      log_debug("merge result: {}", row.ToString());
    }
  }
  return std::make_shared<std::vector<Row>>(merged);
}

template <NodeIds NodeIdsT>
arrow::Result<std::shared_ptr<std::vector<Row>>> populate_batch_rows(
    const NodeIdsT& node_ids, const SchemaRef& schema_ref,
    const std::shared_ptr<arrow::Schema>& output_schema,
    const QueryState& query_state, const TraverseType join_type,
    tbb::concurrent_unordered_set<std::string>& global_visited) {
  auto rows = std::make_shared<std::vector<Row>>();
  std::set<std::string> local_visited;
  // For INNER join: only process nodes that have connections
  // For LEFT join: process all nodes from the "left" side
  for (const auto node_id : node_ids) {
    auto key = schema_ref.value() + ":" + std::to_string(node_id);
    if (!global_visited.insert(key).second) {
      // Skip if already processed in an earlier traversal
      continue;
    }

    // For INNER JOIN: Skip nodes without connections
    if (join_type == TraverseType::Inner &&
        !query_state.has_outgoing(schema_ref, node_id)) {
      continue;
    }

    auto res = populate_rows_bfs(node_id, schema_ref, output_schema,
                                 query_state, local_visited);
    if (!res.ok()) {
      log_error("Failed to populate rows for node {} in schema '{}': {}",
                node_id, schema_ref.value(), res.status().ToString());
      return res.status();
    }

    const auto& res_value = res.ValueOrDie();
    rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                 std::make_move_iterator(res_value->end()));
  }
  global_visited.insert(local_visited.begin(), local_visited.end());
  return rows;
}

std::vector<std::vector<int64_t>> batch_node_ids(const std::set<int64_t>& ids,
                                                 size_t batch_size) {
  std::vector<std::vector<int64_t>> batches;
  std::vector<int64_t> current_batch;
  current_batch.reserve(batch_size);

  for (const auto& id : ids) {
    current_batch.push_back(id);

    if (current_batch.size() >= batch_size) {
      batches.push_back(std::move(current_batch));
      current_batch.clear();
      current_batch.reserve(batch_size);
    }
  }

  if (!current_batch.empty()) {
    batches.push_back(std::move(current_batch));
  }

  return batches;
}

// process all schemas used in traverse
// Phase 1: Process connected nodes
// Phase 2: Handle outer joins for unmatched nodes
arrow::Result<std::shared_ptr<std::vector<Row>>> populate_rows(
    const ExecutionConfig& execution_config, const QueryState& query_state,
    const std::vector<Traverse>& traverses,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  auto rows = std::make_shared<std::vector<Row>>();
  std::mutex rows_mtx;
  tbb::concurrent_unordered_set<std::string> global_visited;

  // Map schemas to their join types
  std::unordered_map<std::string, TraverseType> schema_join_types;
  schema_join_types[query_state.from.value()] =
      TraverseType::Inner;  // FROM is always inner by default
  if (traverses.empty()) {
    schema_join_types[query_state.from.value()] = TraverseType::Left;
  }

  // Only apply LEFT JOIN to FROM schema if the FROM schema is directly involved
  // in a LEFT JOIN traversal
  for (const auto& traverse : traverses) {
    if (traverse.source().value() == query_state.from.value() &&
        (traverse.traverse_type() == TraverseType::Left ||
         traverse.traverse_type() == TraverseType::Full)) {
      schema_join_types[query_state.from.value()] = traverse.traverse_type();
      break;
    }
  }

  // Build ordered list of schema references to process
  std::vector<SchemaRef> ordered_schemas;
  ordered_schemas.push_back(query_state.from);

  // Add schemas from traversals in order
  for (const auto& traverse : traverses) {
    // Update join type for the target schema
    schema_join_types[traverse.target().value()] = traverse.traverse_type();

    // Add target schema to the ordered list if not already present
    if (std::ranges::find_if(ordered_schemas, [&](const SchemaRef& sr) {
          return sr.value() == traverse.target().value();
        }) == ordered_schemas.end()) {
      ordered_schemas.push_back(traverse.target());
    }
  }

  log_debug("Processing {} schemas with their respective join types",
            ordered_schemas.size());

  // Process each schema in order
  for (const auto& schema_ref : ordered_schemas) {
    TraverseType join_type = schema_join_types[schema_ref.value()];
    log_debug("Processing schema '{}' with join type {}", schema_ref.value(),
              static_cast<int>(join_type));

    if (!query_state.ids.contains(schema_ref.value())) {
      log_warn("Schema '{}' not found in query state IDs", schema_ref.value());
      continue;
    }

    // Get all nodes for this schema
    const auto& schema_nodes = query_state.ids.at(schema_ref.value());
    std::vector<std::vector<int64_t>> batch_ids;
    if (execution_config.parallel_enabled) {
      size_t batch_size = 0;
      if (execution_config.parallel_batch_size > 0) {
        batch_size = execution_config.parallel_batch_size;
      } else {
        batch_size = execution_config.calculate_batch_size(schema_nodes.size());
      }
      auto batches = batch_node_ids(schema_nodes, batch_size);
      if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
        log_debug(
            "process concurrently. thread_count={}, batch_size={}, "
            "batches_count={}",
            execution_config.parallel_thread_count, batch_size, batches.size());
      }
      tbb::task_arena arena(execution_config.parallel_thread_count);
      std::atomic error_occurred{false};
      std::string error_message;
      std::mutex error_mutex;
      arena.execute([&] {
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, batches.size()),
            [&](const tbb::blocked_range<size_t>& range) {
              for (size_t i = range.begin(); i != range.end(); ++i) {
                if (error_occurred.load()) {
                  return;  // Early exit from this thread
                }
                auto res =
                    populate_batch_rows(batches[i], schema_ref, output_schema,
                                        query_state, join_type, global_visited);
                if (!res.ok()) {
                  error_occurred.store(true);
                  std::lock_guard lock(error_mutex);
                  if (error_message.empty()) {  // First error wins
                    error_message = res.status().ToString();
                  }
                  return;
                }
                const auto& batch_rows = res.ValueOrDie();
                std::lock_guard lock(rows_mtx);
                rows->insert(rows->end(), batch_rows->begin(),
                             batch_rows->end());
              }
            });
      });
      if (error_occurred.load()) {
        return arrow::Status::ExecutionError(
            "Parallel batch processing failed: " + error_message);
      }

    } else {
      auto res = populate_batch_rows(schema_nodes, schema_ref, output_schema,
                                     query_state, join_type, global_visited);
      if (!res.ok()) {
        return res.status();
      }
      const auto& res_value = res.ValueOrDie();
      rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                   std::make_move_iterator(res_value->end()));
    }

    if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
      log_debug("Processing schema '{}' nodes: [{}]", schema_ref.value(),
                join_container(schema_nodes));
    }
  }

  log_debug("Generated {} total rows after processing all schemas",
            rows->size());
  return rows;
}

arrow::Result<std::shared_ptr<arrow::Table>> create_empty_table(
    const std::shared_ptr<arrow::Schema>& schema) {
  // Create empty arrays for each field in the schema
  std::vector<std::shared_ptr<arrow::Array>> empty_arrays;

  for (const auto& field : schema->fields()) {
    std::shared_ptr<arrow::Array> empty_array;

    switch (field->type()->id()) {
      case arrow::Type::INT64: {
        arrow::Int64Builder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::STRING: {
        arrow::StringBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::DOUBLE: {
        arrow::DoubleBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::BOOL: {
        arrow::BooleanBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      default:
        // For any other type, create a generic empty array
        empty_array = std::make_shared<arrow::NullArray>(0);
    }

    empty_arrays.push_back(empty_array);
  }

  // Create table from schema and empty arrays
  return arrow::Table::Make(schema, empty_arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_rows(
    const std::shared_ptr<std::vector<Row>>& rows,
    const std::shared_ptr<arrow::Schema>& schema = nullptr) {
  if (!rows || rows->empty()) {
    if (schema == nullptr) {
      return arrow::Status::Invalid("No rows provided to create table");
    }
    return create_empty_table(schema);
  }

  std::shared_ptr<arrow::Schema> output_schema;

  if (schema) {
    // Use the provided schema
    output_schema = schema;
  } else {
    // Get all field names from all rows to create a complete schema
    std::set<std::string> all_field_names;
    for (const auto& row : *rows) {
      for (const auto& field_name : row.cells | std::views::keys) {
        all_field_names.insert(field_name);
      }
    }

    // Create schema from field names
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (const auto& field_name : all_field_names) {
      // Find first non-null value to determine field type
      std::shared_ptr<arrow::DataType> field_type = nullptr;
      for (const auto& row : *rows) {
        auto it = row.cells.find(field_name);
        if (it != row.cells.end() && it->second) {
          if (auto array_result = arrow::MakeArrayFromScalar(*(it->second), 1);
              array_result.ok()) {
            field_type = array_result.ValueOrDie()->type();
            break;
          }
        }
      }

      // If we couldn't determine type, default to string
      if (!field_type) {
        field_type = arrow::utf8();
      }

      fields.push_back(arrow::field(field_name, field_type));
    }

    output_schema = std::make_shared<arrow::Schema>(fields);
  }

  // Create array builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : output_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
  }

  // Populate the builders from each row
  for (const auto& row : *rows) {
    for (size_t i = 0; i < output_schema->num_fields(); i++) {
      const auto& field_name = output_schema->field(i)->name();
      auto it = row.cells.find(field_name);

      if (it != row.cells.end() && it->second) {
        // We have a value for this field
        auto array_result = arrow::MakeArrayFromScalar(*(it->second), 1);
        if (array_result.ok()) {
          auto array = array_result.ValueOrDie();
          auto scalar_result = array->GetScalar(0);
          if (scalar_result.ok()) {
            ARROW_RETURN_NOT_OK(
                builders[i]->AppendScalar(*scalar_result.ValueOrDie()));
            continue;
          }
        }
      }

      // Fall back to NULL if we couldn't get or append the scalar
      ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
    }
  }

  // Finish building the arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());

  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder->Finish(&array));
    arrays.push_back(array);
  }

  // Create and return the table
  return arrow::Table::Make(output_schema, arrays);
}

// Function to project a table based on Select clause
std::shared_ptr<arrow::Table> apply_select(
    const std::shared_ptr<Select>& select,
    const std::shared_ptr<arrow::Table>& table) {
  // If select is null or fields are empty, return the original table
  if (!select || select->fields().empty()) {
    return table;
  }

  // Get all column names
  std::vector<std::string> all_columns;
  for (const auto& field : table->schema()->fields()) {
    all_columns.push_back(field->name());
  }

  // Track which columns to keep
  std::unordered_set<int> columns_to_keep;

  // Process each field in the SELECT clause
  for (const auto& field : select->fields()) {
    bool is_prefix = true;

    // Check if it's a fully qualified name (contains a dot)
    if (field.find('.') != std::string::npos) {
      // This is a fully qualified column name, look for exact match
      for (int i = 0; i < all_columns.size(); ++i) {
        if (all_columns[i] == field) {
          columns_to_keep.insert(i);
          break;
        }
      }
      is_prefix = false;
    }

    // If it's a prefix (or no exact match was found), find all columns starting
    // with prefix
    if (is_prefix) {
      std::string prefix = field + ".";
      for (int i = 0; i < all_columns.size(); ++i) {
        // Check if column starts with the prefix
        if (all_columns[i].find(prefix) == 0) {
          columns_to_keep.insert(i);
        }
      }
    }
  }

  // Convert set to vector and sort for consistent column order
  std::vector<int> column_indices(columns_to_keep.begin(),
                                  columns_to_keep.end());
  std::ranges::sort(column_indices);

  // Create a result with selected columns
  arrow::Result<std::shared_ptr<arrow::Table>> result =
      table->SelectColumns(column_indices);

  if (!result.ok()) {
    // Handle error - you might want to define how to handle errors
    // For now, let's return the original table if selection fails
    return table;
  }

  return result.ValueOrDie();
}

std::vector<std::shared_ptr<WhereExpr>> get_where_to_inline(
    const std::string& target_var, size_t i,
    const std::vector<std::shared_ptr<Clause>>& clauses) {
  std::vector<std::shared_ptr<WhereExpr>> inlined;
  for (; i < clauses.size(); i++) {
    if (clauses[i]->type() == Clause::Type::WHERE) {
      auto where_expr = std::dynamic_pointer_cast<WhereExpr>(clauses[i]);
      if (where_expr->can_inline(target_var)) {
        log_debug("inline where: '{}'", where_expr->toString());
        inlined.push_back(where_expr);
      }
    }
  }
  return inlined;
}

arrow::Result<std::shared_ptr<arrow::Table>> inline_where(
    const SchemaRef& ref, std::shared_ptr<arrow::Table> table,
    QueryState& query_state,
    const std::vector<std::shared_ptr<WhereExpr>>& where_exprs) {
  auto curr_table = std::move(table);
  for (const auto& exp : where_exprs) {
    log_debug("inline where '{}'", exp->toString());
    auto result = filter(curr_table, *exp, true);
    if (!result.ok()) {
      log_error(
          "Where inline. Failed to filter table '{}', where: '{}', error: {}",
          ref.toString(), exp->toString(), result.status().ToString());
      return result.status();
    }
    ARROW_RETURN_NOT_OK(query_state.update_table(result.ValueOrDie(), ref));
    curr_table = result.ValueOrDie();
    exp->set_inlined(true);
  }
  return curr_table;
}

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state;
  auto result = std::make_shared<QueryResult>();
  log_debug("Executing query starting from schema '{}'",
            query.from().toString());
  query_state.node_manager = this->node_manager_;
  query_state.schema_registry = this->schema_registry_;
  query_state.from = query.from();

  {
    log_debug("processing 'from' {}", query.from().toString());
    ARROW_ASSIGN_OR_RAISE(auto source_schema,
                          query_state.resolve_schema(query.from()));
    if (!this->schema_registry_->exists(source_schema)) {
      log_error("schema '{}' doesn't exist", source_schema);
      return arrow::Status::KeyError("schema doesn't exit: {}", source_schema);
    }
    auto table_res = this->get_table(source_schema);
    if (!table_res.ok()) {
      log_error("failed to get table 'from' {}", query.from().toString());
      return table_res.status();
    }
    ARROW_ASSIGN_OR_RAISE(auto source_table, this->get_table(source_schema));
    ARROW_RETURN_NOT_OK(query_state.update_table(source_table, query.from()));
  }

  {
    auto where_exps =
        get_where_to_inline(query.from().value(), 0, query.clauses());
    result->mutable_execution_stats().num_where_clauses_inlined +=
        where_exps.size();
    auto res =
        inline_where(query.from(), query_state.tables[query.from().value()],
                     query_state, where_exps);
    if (!res.ok()) {
      return res.status();
    }
  }

  log_debug("Processing {} query clauses", query.clauses().size());
  std::vector<std::shared_ptr<WhereExpr>> post_where;
  for (auto i = 0; i < query.clauses().size(); ++i) {
    auto clause = query.clauses()[i];
    switch (clause->type()) {
      case Clause::Type::WHERE: {
        auto where = std::dynamic_pointer_cast<WhereExpr>(clause);
        if (where->inlined()) {
          log_debug("where '{}' is inlined, skip", where->toString());
          continue;
        }
        auto variables = where->get_all_variables();
        if (variables.empty()) {
          return arrow::Status::Invalid(
              "where clause field must have variable "
              "<var>.<field>, actual={}",
              where->toString());
        }
        if (variables.size() == 1) {
          log_debug("Processing WHERE clause: '{}'", where->toString());

          std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
          std::string variable = *variables.begin();
          if (!query_state.tables.contains(variable)) {
            return arrow::Status::Invalid("Unknown variable '{}'", variable);
          }
          auto table = query_state.tables.at(variable);
          auto filtered_table_result = filter(table, *where, true);
          if (!filtered_table_result.ok()) {
            log_error("Failed to process where: '{}', error: {}",
                      where->toString(),
                      filtered_table_result.status().ToString());
            return filtered_table_result.status();
          }
          ARROW_RETURN_NOT_OK(query_state.update_table(
              filtered_table_result.ValueOrDie(), SchemaRef::parse(variable)));
        } else {
          log_debug("Add compound WHERE expression: '{}' to post process",
                    where->toString());
          post_where.emplace_back(where);
        }
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        std::vector<std::shared_ptr<WhereExpr>> where_clauses;
        if (query.inline_where()) {
          where_clauses = get_where_to_inline(traverse->target().value(), i + 1,
                                              query.clauses());
          result->mutable_execution_stats().num_where_clauses_inlined +=
              where_clauses.size();
        }

        ARROW_ASSIGN_OR_RAISE(auto source_schema,
                              query_state.resolve_schema(traverse->source()));
        ARROW_ASSIGN_OR_RAISE(auto target_schema,
                              query_state.resolve_schema(traverse->target()));
        query_state.traversals.push_back(*traverse);
        if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
          log_debug("Processing TRAVERSE {}-({})->{}",
                    traverse->source().toString(), traverse->edge_type(),
                    traverse->target().toString());
        }
        auto source = traverse->source();
        if (!query_state.tables.contains(source.value())) {
          if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
            log_debug("Source table '{}' not found. Loading",
                      traverse->source().toString());
          }
          ARROW_ASSIGN_OR_RAISE(auto source_table,
                                this->get_table(source_schema));
          ARROW_RETURN_NOT_OK(
              query_state.update_table(source_table, traverse->source()));
        }

        if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
          log_debug("Traversing from {} source nodes",
                    query_state.ids[source.value()].size());
        }
        std::set<int64_t> matched_source_ids;
        std::set<int64_t> matched_target_ids;
        std::set<int64_t> unmatched_source_ids;
        for (auto source_id : query_state.ids[source.value()]) {
          auto outgoing_edges =
              edge_store_->get_outgoing_edges(source_id, traverse->edge_type())
                  .ValueOrDie();  // todo check result
          log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                    outgoing_edges.size(), traverse->edge_type());

          std::vector<std::shared_ptr<Node>> target_nodes;
          for (auto edge : outgoing_edges) {
            auto target_id = edge->get_target_id();
            if (query_state.ids.contains(traverse->target().value()) &&
                !query_state.ids.at(traverse->target().value())
                     .contains(target_id)) {
              continue;
            }
            auto node_result = node_manager_->get_node(target_id);
            if (node_result.ok()) {
              auto target_node = node_result.ValueOrDie();
              if (target_node->schema_name == target_schema) {
                // Then apply all WHERE clauses with AND logic
                bool passes_all_filters = true;
                // Multiple conditions - could optimize by creating a
                // temporary table and using Arrow expressions For now, use
                // the existing approach but this could be optimized
                for (const auto& where_clause : where_clauses) {
                  if (!apply_where_to_node(where_clause, target_node)
                           .ValueOrDie()) {
                    passes_all_filters = false;
                    break;
                  }
                }
                if (passes_all_filters) {
                  log_debug("found edge {}:{} -[{}]-> {}:{}", source.value(),
                            source_id, traverse->edge_type(),
                            traverse->target().value(), target_node->id);
                  target_nodes.push_back(target_node);
                }
              }
            } else {
              log_warn("Failed to get node {}:{}, error: {}",
                       traverse->target().value(), target_id,
                       node_result.status().ToString());
            }
          }
          if (!target_nodes.empty()) {
            matched_source_ids.insert(source_id);
            for (const auto& target_node : target_nodes) {
              matched_target_ids.insert(target_node->id);
              auto conn = GraphConnection{
                  traverse->source(), source_id,      traverse->edge_type(), "",
                  traverse->target(), target_node->id};

              query_state.connections[traverse->source().value()][source_id]
                  .push_back(conn);
              query_state.incoming[target_node->id].push_back(conn);
            }
          } else {
            log_debug("no edge found from {}:{}", source.value(), source_id);
            unmatched_source_ids.insert(source_id);
          }
        }
        if (traverse->traverse_type() == TraverseType::Inner &&
            !unmatched_source_ids.empty()) {
          for (auto id : unmatched_source_ids) {
            log_debug("remove unmatched node={}:{}", source.value(), id);
            query_state.remove_node(id, source);
          }
          log_debug("rebuild table for schema {}:{}", source.value(),
                    query_state.aliases[source.value()]);
          auto table_result =
              filter_table_by_id(query_state.tables[source.value()],
                                 query_state.ids[source.value()]);
          if (!table_result.ok()) {
            return table_result.status();
          }
          query_state.tables[source.value()] = table_result.ValueOrDie();
        }
        log_debug("found {} neighbors for {}", matched_target_ids.size(),
                  traverse->target().toString());

        if (traverse->traverse_type() == TraverseType::Inner) {
          // intersect
          // a:0 -> c:0
          // b:0 -> c:1
          // after a:0 -> c:0 => ids[c] = {0}
          // after b:0 -> c:1 we need to intersect it with ids[c] =
          // intersect({0}, {1}) => {}
          auto target_ids = query_state.get_ids(traverse->target());
          std::set<int64_t> intersect_ids;
          if (target_ids.empty()) {
            intersect_ids = matched_target_ids;
          } else {
            std::ranges::set_intersection(
                target_ids, matched_target_ids,
                std::inserter(intersect_ids, intersect_ids.begin()));
          }

          query_state.ids[traverse->target().value()] = intersect_ids;
          log_debug("intersect_ids count: {}", intersect_ids.size());
          log_debug("{} intersect_ids: {}", traverse->target().toString(),
                    join_container(intersect_ids));
        } else if (traverse->traverse_type() == TraverseType::Left) {
          query_state.ids[traverse->target().value()].insert(
              matched_target_ids.begin(), matched_target_ids.end());
        } else {  // Right, Full remove nodes with incoming connections
          auto target_ids =
              get_ids_from_table(get_table(target_schema).ValueOrDie())
                  .ValueOrDie();
          log_debug(
              "traverse type: '{}', matched_source_ids=[{}], "
              "target_ids=[{}]",
              traverse->target().value(), join_container(matched_source_ids),
              join_container(target_ids));
          std::set<int64_t> result;
          std::ranges::set_difference(target_ids, matched_source_ids,
                                      std::inserter(result, result.begin()));
          query_state.ids[traverse->target().value()] = result;
        }

        std::vector<std::shared_ptr<Node>> neighbors;
        for (auto id : query_state.ids[traverse->target().value()]) {
          auto node_res = node_manager_->get_node(id);
          if (node_res.ok()) {
            neighbors.push_back(node_res.ValueOrDie());
          }
        }
        auto target_table_schema =
            schema_registry_->get_arrow(target_schema).ValueOrDie();
        auto table_result =
            create_table_from_nodes(target_table_schema, neighbors);
        if (!table_result.ok()) {
          log_error("Failed to create table from neighbors: {}",
                    table_result.status().ToString());
          return table_result.status();
        }
        ARROW_RETURN_NOT_OK(query_state.update_table(table_result.ValueOrDie(),
                                                     traverse->target()));
        break;
      }
      default:
        log_error("Unsupported clause type: {}",
                  static_cast<int>(clause->type()));
        return arrow::Status::NotImplemented(
            "Database::query unsupported clause");
    }
  }

  if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
    log_debug("Query processing complete, building result");
    log_debug("Query state: {}", query_state.ToString());
  }

  auto output_schema_res = build_denormalized_schema(query_state);
  if (!output_schema_res.ok()) {
    return output_schema_res.status();
  }
  const auto output_schema = output_schema_res.ValueOrDie();
  log_debug("output_schema={}", output_schema->ToString());

  auto row_res = populate_rows(query.execution_config(), query_state,
                               query_state.traversals, output_schema);
  if (!row_res.ok()) {
    return row_res.status();
  }
  auto rows = row_res.ValueOrDie();
  auto output_table_res = create_table_from_rows(rows, output_schema);
  if (!output_table_res.ok()) {
    log_error("Failed to create table from rows: {}",
              output_table_res.status().ToString());
    return output_table_res.status();
  }
  auto output_table = output_table_res.ValueOrDie();
  for (const auto& expr : post_where) {
    result->mutable_execution_stats().num_where_clauses_post_processed++;
    log_debug("post process where: {}", expr->toString());
    output_table = filter(output_table, *expr, false).ValueOrDie();
  }
  result->set_table(apply_select(query.select(), output_table));
  return result;
}

}  // namespace tundradb
