#include "../include/core.hpp"

#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <fmt/ranges.h>
#include <support/CPPUtils.h>

#include <chrono>
#include <future>
#include <iostream>
#include <list>
#include <memory>
#include <stack>
#include <thread>
#include <utility>
#include <vector>

#include "logger.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

namespace tundradb {

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
      const auto& field_name = schema->field(i)->name();

      // Find the array in the node's data
      auto res = node->get_field(field_name);
      if (res.ok()) {
        // Extract the first value from the array and append to builder
        auto array = res.ValueOrDie();
        if (array->length() > 0) {
          auto scalar_result = array->GetScalar(0);
          if (!scalar_result.ok()) {
            log_error("Failed to get scalar from array for field '{}': {}",
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
          log_debug("Empty array for field '{}', appending null", field_name);
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

arrow::Result<std::set<int64_t>> get_ids_from_table(
    std::shared_ptr<arrow::Table> table) {
  log_debug("Extracting IDs from table with {} rows", table->num_rows());

  auto id_idx = table->schema()->GetFieldIndex("id");
  if (id_idx == -1) {
    log_error("Table does not have an 'id' column");
    return arrow::Status::Invalid("table does not have an 'id' column");
  }

  auto id_column = table->column(id_idx);
  std::set<int64_t> result_ids;

  for (int chunk_idx = 0; chunk_idx < id_column->num_chunks(); chunk_idx++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(
        id_column->chunk(chunk_idx));
    log_debug("Processing chunk {} with {} rows", chunk_idx, chunk->length());
    for (int i = 0; i < chunk->length(); i++) {
      result_ids.insert(chunk->Value(i));
    }
  }

  log_debug("Extracted {} unique IDs from table", result_ids.size());
  return result_ids;
}

arrow::Result<std::shared_ptr<arrow::Table>> filter(
    std::shared_ptr<arrow::Table> table, const std::string& field_name,
    const CompareOp& op, const Value& value) {
  log_info("Filtering table on field '{}' with {} operator", field_name,
           static_cast<int>(op));

  // First check if the field exists
  auto field_idx = table->schema()->GetFieldIndex(field_name);
  if (field_idx == -1) {
    log_error("Field '{}' not found in table", field_name);
    return arrow::Status::Invalid("Field '", field_name,
                                  "' not found in table");
  }

  // Get the column to filter on
  auto column = table->column(field_idx);
  log_debug("Found column '{}' at index {}", field_name, field_idx);

  // Create the comparison scalar
  arrow::compute::Expression scalar_value;
  arrow::compute::Expression field = arrow::compute::field_ref(field_name);
  log_debug("Created field reference for '{}'", field_name);

  switch (value.type()) {
    case ValueType::Int64:
      log_debug("Using Int64 value {} for filter", value.get<int64_t>());
      scalar_value = arrow::compute::literal(value.get<int64_t>());
      break;
    case ValueType::String:
      log_debug("Using String value '{}' for filter", value.get<std::string>());
      scalar_value = arrow::compute::literal(value.get<std::string>());
      break;
    // Add other types as needed
    default:
      log_error("Unsupported value type for filter: {}",
                static_cast<int>(value.type()));
      return arrow::Status::Invalid("Unsupported value type");
  }

  arrow::compute::Expression op_exp;
  log_debug("Creating filter expression");

  switch (op) {
    case CompareOp::Eq:
      log_debug("Using EQUAL operator");
      op_exp = arrow::compute::equal(field, scalar_value);
      break;
    case CompareOp::NotEq:
      log_debug("Using NOT_EQUAL operator");
      op_exp = arrow::compute::not_equal(field, scalar_value);
      break;
    case CompareOp::Gt:
      log_debug("Using GREATER operator");
      op_exp = arrow::compute::greater(field, scalar_value);
      break;
    case CompareOp::Lt:
      log_debug("Using LESS operator");
      op_exp = arrow::compute::less(field, scalar_value);
      break;
    default:
      log_error("Unsupported comparison operator: {}", static_cast<int>(op));
      return arrow::Status::Invalid("Unsupported operation");
  }

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

  log_debug("Applying filter to scanner builder");
  auto filter_status = scan_builder->Filter(op_exp);
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
  log_info("Filter completed: {} rows in, {} rows out", table->num_rows(),
           result_table->num_rows());
  return result_table;
}

void debug_connections(
    int64_t id,
    const std::map<int64_t, std::vector<GraphConnection>>& connections,
    std::vector<std::string> path, std::vector<std::string>& res) {
  if (!connections.contains(id)) {
    res.push_back(fmt::format("{}", fmt::join(path, ", ")));
    return;
  }
  for (const auto& conn : connections.at(id)) {
    path.push_back(conn.toString());
    debug_connections(conn.target_id, connections, path, res);
  }
}

void print_paths(
    const std::map<int64_t, std::vector<GraphConnection>>& connections) {
  log_info("Printing all paths in connection graph:");

  if (connections.empty()) {
    log_info("  No connections found");
    return;
  }

  for (const auto& [source_id, conn_list] : connections) {
    if (conn_list.empty()) {
      log_info("  Node {} has no outgoing connections", source_id);
      continue;
    }

    for (const auto& conn : conn_list) {
      log_info("  {} -[{}]-> {}", source_id, conn.edge_type, conn.target_id);
    }
  }

  log_info("Total of {} source nodes with connections", connections.size());
}

std::set<int64_t> get_roots(
    const std::map<int64_t, std::vector<GraphConnection>>& connections) {
  std::set<int64_t> roots;
  std::unordered_map<int64_t, int64_t> count;
  std::vector<int64_t> stack;
  for (const auto& conn : connections) {
    count[conn.first] = 0;
    stack.push_back(conn.first);
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
  log_info("Building schema for denormalized table");

  std::set<std::string> processed_fields;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::set<std::string> processed_schemas;

  // First add fields from the FROM schema
  std::string from_schema = query_state.from.value();

  log_debug("Adding fields from FROM schema '{}'", from_schema);

  auto schema_result =
      query_state.schema_registry->get(query_state.aliases.at(from_schema));
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
  for (const auto& traversal : query_state.traversals) {
    if (processed_schemas.insert(traversal.source().value()).second) {
      unique_schemas.push_back(traversal.source());
    }
    if (processed_schemas.insert(traversal.target().value()).second) {
      unique_schemas.push_back(traversal.target());
    }
  }

  for (const auto& schema_ref : unique_schemas) {
    log_debug("Adding fields from schema '{}'", schema_ref.value());

    schema_result = query_state.schema_registry->get(
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
    for (const auto& [name, value] : node->data()) {
      auto full_name = schema_ref.value() + "." + name;
      this->set_cell(full_name, value);
    }
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
  Row merge(const Row& other) const {
    Row merged;
    merged = *this;
    for (const auto& [name, value] : other.cells) {
      if (!merged.has_value(name)) {
        merged.cells[name] = value;
      }
    }
    return merged;
  }

  std::string ToString() const {
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

  // Default constructor
  RowNode() : depth(0) {}

  RowNode(std::optional<Row> r, int d,
          std::vector<std::unique_ptr<RowNode>> c = {})
      : row(move(r)), depth(d), children(std::move(c)) {}

  bool leaf() const { return row.has_value(); }

  void insert_row_dfs(size_t path_idx, const Row& new_row) {
    if (path_idx == new_row.path.size()) {
      this->row = new_row;
      this->depth = depth;
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
            if (schema_ids_r2.count(schema) > 0 &&
                schema_ids_r2[schema] != id1) {
              // Found a conflict - same schema but different IDs
              log_debug(
                  "Conflict detected: Schema '{}' has different IDs: {} vs {}",
                  schema, id1, schema_ids_r2[schema]);
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
                  log_debug(
                      "Conflict detected: Field '{}' has different values",
                      field_name);
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

  void print(bool recursive = true) const { std::cout << toString(recursive); }
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
      : node_id(id), schema_ref(schema), level(l), row(r) {
    path_visited_nodes.insert(schema_ref.value() + ":" + std::to_string(id));
    path.push_back(PathSegment{schema.value(), id});
  }
};

// Log grouped connections for a node
void log_grouped_connections(
    int64_t node_id,
    const std::unordered_map<std::string, std::vector<GraphConnection>>&
        grouped_connections) {
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

arrow::Result<std::shared_ptr<std::vector<Row>>> populate_rows_bfs(
    int64_t node_id, const SchemaRef& start_schema,
    const std::shared_ptr<arrow::Schema>& output_schema,
    const QueryState& query_state, std::set<std::string>& global_visited) {
  log_debug("populate_rows_bfs::node={}:{}", start_schema.value(), node_id);
  auto result = std::make_shared<std::vector<Row>>();
  int64_t row_id_counter = 0;
  auto initial_row =
      std::make_shared<Row>(create_empty_row_from_schema(output_schema));

  std::queue<QueueItem> queue;
  queue.push(QueueItem(node_id, start_schema, 0, initial_row));

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
          std::cout << "add row: " << r.ToString() << std::endl;
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
            log_debug("continue the path: {}", join_schema_path(next.path));
            queue.push(next);
          } else {
            for (const auto& conn : connections) {
              auto next_row = std::make_shared<Row>(*item.row);
              auto next = QueueItem(conn.target_id, conn.target, item.level + 1,
                                    next_row);
              next.path = item.path;
              next.path.push_back(
                  PathSegment{conn.target.value(), conn.target_id});
              log_debug("create a new path {}, node={}",
                        join_schema_path(next.path), conn.target_id);
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
    std::cout << "bfs result: " << r.ToString() << std::endl;
    tree.insert_row(r);
  }
  tree.print();
  auto merged = tree.merge_rows();
  for (const auto& row : merged) {
    std::cout << "merge result: " << row.ToString() << std::endl;
  }
  return std::make_shared<std::vector<Row>>(merged);
}

// process all schemas used in traverse
// Phase 1: Process connected nodes
// Phase 2: Handle outer joins for unmatched nodes
arrow::Result<std::shared_ptr<std::vector<Row>>> populate_rows(
    const QueryState& query_state, const std::vector<Traverse>& traverses,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  auto rows = std::make_shared<std::vector<Row>>();
  std::set<std::string>
      global_visited;  // Track processed nodes across all schemas

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

    // Add target schema to ordered list if not already present
    if (std::find_if(ordered_schemas.begin(), ordered_schemas.end(),
                     [&](const SchemaRef& sr) {
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

    log_debug(">>Processing schema '{}' nodes '{}'", schema_ref.value(),
              schema_nodes);
    std::set<std::string> local_visited;

    // For INNER join: only process nodes that have connections
    // For LEFT join: process all nodes from the "left" side
    for (auto node_id : schema_nodes) {
      // Skip if already processed in an earlier traversal
      auto key = schema_ref.value() + ":" + std::to_string(node_id);
      if (global_visited.contains(key)) {
        continue;
      }

      // For INNER JOIN: Skip nodes without connections
      if (join_type == TraverseType::Inner &&
          !query_state.has_outgoing(schema_ref, node_id)) {
        continue;
      }

      // Process this node with BFS
      auto res = populate_rows_bfs(node_id, schema_ref, output_schema,
                                   query_state, local_visited);
      if (!res.ok()) {
        log_error("Failed to populate rows for node {} in schema '{}': {}",
                  node_id, schema_ref.value(), res.status().ToString());
        return res.status();
      }

      // Add the rows from BFS to our result
      auto res_value = res.ValueOrDie();
      rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                   std::make_move_iterator(res_value->end()));

      // Mark this node as visited
      global_visited.insert(key);
    }
    global_visited.insert(local_visited.begin(), local_visited.end());
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
      for (const auto& [field_name, _] : row.cells) {
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
          auto array_result = arrow::MakeArrayFromScalar(*(it->second), 1);
          if (array_result.ok()) {
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

#include <arrow/table.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

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
  std::sort(column_indices.begin(), column_indices.end());

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

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state;
  log_info("Executing query starting from schema '{}'",
           query.from().toString());
  query_state.node_manager = this->node_manager;
  query_state.schema_registry = this->schema_registry;
  query_state.from = query.from();

  {
    log_info("processing 'from' {}", query.from().toString());
    ARROW_ASSIGN_OR_RAISE(auto source_schema,
                          query_state.resolve_schema(query.from()));
    ARROW_ASSIGN_OR_RAISE(auto source_table, this->get_table(source_schema));
    ARROW_RETURN_NOT_OK(query_state.update_table(source_table, query.from()));
  }

  log_info("Processing {} query clauses", query.clauses().size());
  for (const auto& clause : query.clauses()) {
    switch (clause->type()) {
      // note: consecutive 'where' clauses should be combined into one
      case Clause::Type::WHERE: {
        auto where = std::static_pointer_cast<Where>(clause);
        log_info("Processing WHERE clause on field '{}' with operator {}",
                 where->field(), static_cast<int>(where->op()));

        std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
        size_t pos = where->field().find('.');
        std::string variable;
        std::string field;
        if (pos == std::string::npos) {
          return arrow::Status::Invalid("expected <var>.<field>, actual={}",
                                        where->field());
        }
        variable = where->field().substr(0, pos);
        field = where->field().substr(pos + 1);
        if (!query_state.tables.contains(variable)) {
          return arrow::Status::Invalid("Unknown variable '{}'", variable);
        }
        auto table = query_state.tables.at(variable);
        if (table->schema()->GetFieldIndex(field) == -1) {
          return arrow::Status::Invalid("Unknown field '{}'", field);
        }
        auto filtered_table_result =
            filter(table, field, where->op(), where->value());
        if (!filtered_table_result.ok()) {
          log_error("Failed to filter table '{}': {}", where->field(),
                    filtered_table_result.status().ToString());
          return filtered_table_result.status();
        }
        log_info("filtered '{}' table ", variable);
        print_table(filtered_table_result.ValueOrDie());
        auto res = query_state.update_table(filtered_table_result.ValueOrDie(),
                                            SchemaRef::parse(variable));
        if (!res.ok()) {
          return res.status();
        }
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        ARROW_ASSIGN_OR_RAISE(auto source_schema,
                              query_state.resolve_schema(traverse->source()));
        ARROW_ASSIGN_OR_RAISE(auto target_schema,
                              query_state.resolve_schema(traverse->target()));
        query_state.traversals.push_back(*traverse);
        log_info("Processing TRAVERSE {}-({})->{}",
                 traverse->source().toString(), traverse->edge_type(),
                 traverse->target().toString());
        auto source = traverse->source();
        if (!query_state.tables.contains(source.value())) {
          log_debug("Source table '{}' not found. Loading",
                    traverse->source().toString());
          ARROW_ASSIGN_OR_RAISE(auto source_table,
                                this->get_table(source_schema));
          ARROW_RETURN_NOT_OK(
              query_state.update_table(source_table, traverse->source()));
        }

        log_debug("Traversing from {} source nodes",
                  query_state.ids[source.value()].size());
        std::set<int64_t> matched_source_ids;
        std::set<int64_t> matched_target_ids;
        std::set<int64_t> unmatched_source_ids;
        for (auto source_id : query_state.ids[source.value()]) {
          auto outgoing_edges =
              edge_store->get_outgoing_edges(source_id, traverse->edge_type())
                  .ValueOrDie();  // todo check result
          log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                    outgoing_edges.size(), traverse->edge_type());

          std::vector<std::shared_ptr<Node>> target_nodes;
          for (auto edge : outgoing_edges) {
            auto target_id = edge->get_target_id();
            auto node_result = node_manager->get_node(target_id);
            if (node_result.ok()) {
              auto target_node = node_result.ValueOrDie();
              if (target_node->schema_name == target_schema) {
                log_info("found edge {}:{} -[{}]-> {}:{}", source.value(),
                         source_id, traverse->edge_type(),
                         traverse->target().value(), target_node->id);
                target_nodes.push_back(target_node);
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

              /*

              Connections (Outgoing) (3 source nodes):    - Source ID 0 (3
            outgoing):        -> f:1 (via 'friend')
                -> f:2 (via 'friend')
                -> f:3 (via 'friend')
            - Source ID 1 (1 outgoing):        -> c:6 (via 'works-at')
            - Source ID 2 (1 outgoing):        -> l:5 (via 'likes')

               */

              query_state.connections[traverse->source().value()][source_id]
                  .push_back(conn);
              query_state.incoming[target_node->id].push_back(conn);
            }
          } else {
            log_info("no edge found from {}:{}", source.value(), source_id);
            unmatched_source_ids.insert(source_id);
          }
        }
        if (traverse->traverse_type() == TraverseType::Inner &&
            !unmatched_source_ids.empty()) {
          for (auto id : unmatched_source_ids) {
            log_debug("remove unmatched node={}:{}", source.value(), id);
            query_state.remove_node(id, source);
          }
          log_info("rebuild table for schema {}:{}", source.value(),
                   query_state.aliases[source.value()]);
          auto table_result =
              FilterTableById(query_state.tables[source.value()],
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
          log_info("intersect_ids count: {}", intersect_ids.size());
          log_info("{} intersect_ids: {}", traverse->target().toString(),
                   intersect_ids);
        } else if (traverse->traverse_type() == TraverseType::Left) {
          query_state.ids[traverse->target().value()].insert(
              matched_target_ids.begin(), matched_target_ids.end());
        } else {  // Right, Full remove nodes with incoming connections
          auto target_ids =
              get_ids_from_table(get_table(target_schema).ValueOrDie())
                  .ValueOrDie();
          log_debug(
              "traverse type: '{}', remove matched_source_ids={} from "
              "target_ds={}",
              traverse->target().value(), matched_source_ids, target_ids);
          std::set<int64_t> result;
          std::set_difference(
              target_ids.begin(), target_ids.end(), matched_source_ids.begin(),
              matched_source_ids.end(), std::inserter(result, result.begin()));
          query_state.ids[traverse->target().value()] = result;
        }

        std::vector<std::shared_ptr<Node>> neighbors;
        for (auto id : query_state.ids[traverse->target().value()]) {
          auto node_res = node_manager->get_node(id);
          if (node_res.ok()) {
            neighbors.push_back(node_res.ValueOrDie());
          }
        }
        auto target_table_schema =
            schema_registry->get(target_schema).ValueOrDie();
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

  log_info("Query processing complete, building result");
  log_info("Query state: {}", query_state.ToString());
  auto result = std::make_shared<QueryResult>();

  auto output_schema_res = build_denormalized_schema(query_state);
  if (!output_schema_res.ok()) {
    return output_schema_res.status();
  }
  const auto output_schema = output_schema_res.ValueOrDie();
  log_info("output_schema={}", output_schema->ToString());

  auto row_res =
      populate_rows(query_state, query_state.traversals, output_schema);
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
  result->set_table(apply_select(query.select(), output_table));

  return result;
}

}  // namespace tundradb
