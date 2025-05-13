#include "../include/core.hpp"

#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <fmt/ranges.h>

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
namespace fs = std::filesystem;

namespace tundradb {

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_nodes(
    std::shared_ptr<SchemaRegistry> schema_registry,
    const std::vector<std::shared_ptr<Node>>& nodes) {
  log_debug("Creating table from {} nodes with schema '{}'", nodes.size(),
            nodes.empty() ? "unknown" : nodes[0]->schema_name);

  if (nodes.empty()) {
    log_error("Cannot create table from empty nodes list");
    return arrow::Status::Invalid("Cannot create table from empty nodes list");
  }

  // All nodes should have the same schema
  std::string schema_name = nodes[0]->schema_name;
  log_debug("Using schema '{}' for table creation", schema_name);

  // Get schema from SchemaRegistry
  auto schema_result = schema_registry->get(schema_name);
  if (!schema_result.ok()) {
    log_error("Failed to get schema '{}': {}", schema_name,
              schema_result.status().ToString());
    return schema_result.status();
  }
  auto arrow_schema = schema_result.ValueOrDie();

  log_debug("Creating table from schema: {}", arrow_schema->ToString());
  log_debug("Retrieved schema with {} fields", arrow_schema->num_fields());

  // Create builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : arrow_schema->fields()) {
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
    // Ensure node has the expected schema
    if (node->schema_name != schema_name) {
      log_error("Node schema '{}' doesn't match expected schema '{}'",
                node->schema_name, schema_name);
      return arrow::Status::Invalid("Inconsistent schema names in nodes list");
    }

    // Add each field's value to the appropriate builder
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      const auto& field_name = arrow_schema->field(i)->name();

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
  return arrow::Table::Make(arrow_schema, arrays);
}

arrow::Result<std::set<int64_t>> get_ids(std::shared_ptr<arrow::Table> table) {
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
  // roots.insert(connections.begin(), connections.end());
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
  std::map<int64_t, std::vector<GraphConnection>> connections;
  std::shared_ptr<NodeManager> node_manager;
  std::shared_ptr<SchemaRegistry> schema_registry;
  std::vector<Traverse> traversals;

  arrow::Result<bool> init_table(const std::shared_ptr<arrow::Table> table,
                                 const SchemaRef& schema_ref) {
    if (this->aliases.contains(schema_ref.value())) {
      return arrow::Status::Invalid("Duplicated alias: {}", schema_ref.value());
    }
    this->aliases[schema_ref.value()] = schema_ref.schema();

    this->tables[schema_ref.value()] = table;
    log_debug("Getting IDs from initial table: {}", schema_ref.toString());
    auto initial_ids_result = get_ids(this->tables[schema_ref.value()]);
    if (!initial_ids_result.ok()) {
      log_error("Failed to get IDs from initial table '{}': {}",
                schema_ref.toString(), initial_ids_result.status().ToString());
      return initial_ids_result.status();
    }
    this->ids[schema_ref.value()] = initial_ids_result.ValueOrDie();

    return true;
  }

  arrow::Result<bool> init_table(const Database& db,
                                 const SchemaRef& schema_ref) {
    auto initial_table_result = db.get_table(schema_ref.schema());
    if (!initial_table_result.ok()) {
      log_error("Failed to get initial table for schema '{}': {}",
                schema_ref.toString(),
                initial_table_result.status().ToString());
      return initial_table_result.status();
    }
    return init_table(initial_table_result.ValueOrDie(), schema_ref);
  }

  arrow::Result<bool> update_table(std::shared_ptr<arrow::Table> table,
                                   const std::string& table_name) {
    if (!this->tables.contains(table_name)) {
      return arrow::Status::Invalid("Table '{}' does not exist", table_name);
    }
    this->tables[table_name] = table;
    auto ids_result = get_ids(table);
    if (!ids_result.ok()) {
      log_error("Failed to get IDs from table: {}", table_name);
      return ids_result.status();
    }
    return true;
  }
};

arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema(
    const QueryState& query_state) {
  log_info("Building schema for denormalized table");

  std::set<std::string> processed_fields;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::set<std::string> processed_schemas;

  // First add fields from the FROM schema
  std::string from_schema = query_state.from.schema();
  std::string from_alias = query_state.from.value();

  log_debug("Adding fields from FROM schema '{}'", from_schema);

  auto schema_result = query_state.schema_registry->get(from_schema);
  if (!schema_result.ok()) {
    return schema_result.status();
  }

  auto arrow_schema = schema_result.ValueOrDie();
  for (const auto& field : arrow_schema->fields()) {
    std::string prefixed_field_name = from_alias + "." + field->name();
    processed_fields.insert(prefixed_field_name);
    fields.push_back(arrow::field(prefixed_field_name, field->type()));
  }
  processed_schemas.insert(from_schema);

  // Now process all referenced schemas in the connections
  for (const auto& [node_id, node_connections] : query_state.connections) {
    for (const auto& conn : node_connections) {
      std::string target_schema = conn.target.schema();
      std::string target_alias = conn.target.value();

      if (processed_schemas.insert(target_alias).second) {
        log_debug("Adding fields from target schema '{}'", target_schema);

        auto target_schema_result =
            query_state.schema_registry->get(target_schema);
        if (!target_schema_result.ok()) {
          return target_schema_result.status();
        }

        auto target_arrow_schema = target_schema_result.ValueOrDie();
        for (const auto& field : target_arrow_schema->fields()) {
          std::string prefixed_field_name = target_alias + "." + field->name();
          if (processed_fields.contains(prefixed_field_name)) {
            return arrow::Status::KeyError("Field '{}' already exists",
                                           prefixed_field_name);
          }

          processed_fields.insert(prefixed_field_name);
          fields.push_back(arrow::field(prefixed_field_name, field->type()));
        }
      }
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
};

std::string join_schema_path(const std::vector<PathSegment>& schema_path) {
  std::ostringstream oss;
  for (size_t i = 0; i < schema_path.size(); ++i) {
    if (i != 0) oss << "->";
    oss << schema_path[i].toString();
  }
  return oss.str();
}

struct Row {
  // int64_t id;
  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> cells;
  std::vector<PathSegment> path;

  void set_cell(const std::string& name,
                std::shared_ptr<arrow::Scalar> scalar) {
    cells[name] = std::move(scalar);
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

struct QueueItem {
  int64_t node_id;
  SchemaRef schema_ref;
  int level;
  std::shared_ptr<Row> row;
  // std::vector<std::string> path;
  std::set<int64_t> path_visited_nodes;  // Nodes visited in this specific path
  std::vector<PathSegment> path;

  QueueItem(int64_t id, const SchemaRef& schema, int l, std::shared_ptr<Row> r)
      : node_id(id), schema_ref(schema), level(l), row(r) {
    // path.push_back(schema_ref.value());
    path_visited_nodes.insert(id);
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
    const QueryState& query_state) {
  log_debug("populate_rows_bfs::node={}", node_id);
  auto result = std::make_shared<std::vector<Row>>();
  std::set<std::string> visited_schemas;

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
      visited_schemas.insert(item.schema_ref.value());

      // group connections by target schema
      std::unordered_map<std::string, std::vector<GraphConnection>>
          grouped_connections;

      if (query_state.connections.contains(item.node_id)) {
        for (const auto& conn : query_state.connections.at(item.node_id)) {
          if (!visited_schemas.contains(conn.target.value())) {
            grouped_connections[conn.target.value()].push_back(conn);
          }
        }
      }
      log_grouped_connections(item.node_id, grouped_connections);

      if (grouped_connections.empty()) {
        // we've done
        auto r = *item.row;
        r.path = item.path;
        std::cout << "add row: " << r.ToString() << std::endl;
        result->push_back(r);
      } else {
        for (const auto& connections :
             grouped_connections | std::views::values) {
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
  for (const auto& r : *result) {
    std::cout << "bfs result: " << r.ToString() << std::endl;
  }

  return result;
}

arrow::Result<bool> populate_rows_dfs(Row& row, int64_t node_id,
                                      const SchemaRef& schema_ref,
                                      std::vector<Row>& rows,
                                      const QueryState& query_state,
                                      std::set<int64_t>& visited,
                                      std::set<std::string>& visited_schemas,
                                      int64_t total) {
  log_debug("populate_rows_dfs:: visit node: {}", node_id);
  // Check if node has already been visited to avoid cycles
  if (!visited.insert(node_id).second) {
    return true;  // Already processed this node, nothing to do
  }
  // if (!visited_schemas.insert(schema_ref.value()).second) {
  //   return true;
  // };

  auto node_result = query_state.node_manager->get_node(node_id);
  if (!node_result.ok()) {
    return node_result.status();
  }
  auto node = node_result.ValueOrDie();
  for (const auto& [name, value] : node->data()) {
    auto full_name = schema_ref.value() + "." + name;
    row.set_cell(full_name, value);
  }
  bool path_found = false;
  if (query_state.connections.contains(node_id)) {
    std::unordered_map<std::string, std::vector<GraphConnection>>
        grouped_by_schema;

    for (const auto& conn : query_state.connections.at(node_id)) {
      grouped_by_schema[conn.target.value()].push_back(conn);
    }

    for (const auto& [_, conn_list] : grouped_by_schema) {
      if (conn_list.size() == 1) {
        const auto& conn = conn_list[0];
        if (!visited.contains(conn.target_id) &&
            !visited_schemas.contains(conn.target.value())) {
          path_found = true;
          log_debug("populate_rows_dfs:: continue path: {}.{} -[{}]-> {}.{}",
                    schema_ref.toString(), node_id, conn.edge_type,
                    conn.target.toString(), conn.target_id);
          // visited_schemas.insert(schema_ref.value());
          auto res =
              populate_rows_dfs(row, conn.target_id, conn.target, rows,
                                query_state, visited, visited_schemas, total);
          // visited_schemas.erase(schema_ref.value());

          if (!res.ok()) {
            return res.status();
          }
        }
        // continue row
      } else {
        for (const auto& conn : conn_list) {
          if (!visited.contains(conn.target_id) &&
              !visited_schemas.contains(conn.target.value())) {
            path_found = true;
            log_debug(
                "populate_rows_dfs:: create new path: {}.{} -[{}]-> {}.{}",
                schema_ref.toString(), node_id, conn.edge_type,
                conn.target.toString(), conn.target_id);
            Row next_row = row;
            // visited_schemas.insert(schema_ref.value());
            auto res =
                populate_rows_dfs(next_row, conn.target_id, conn.target, rows,
                                  query_state, visited, visited_schemas, total);
            // visited_schemas.erase(schema_ref.value());
            if (!res.ok()) {
              return res.status();
            }
          }
        }
      }
    }
  }
  if (!path_found && total == visited_schemas.size()) {
    log_debug("populate_rows_dfs:: add row: {}", row.ToString());
    rows.emplace_back(std::move(row));
  }
  // visited_schemas.erase(schema_ref.value());
  return true;
}

// process all schemas used in traverse
// Phase 1: Process connected nodes
// Phase 2: Handle outer joins for unmatched nodes
arrow::Result<std::shared_ptr<std::vector<Row>>> populate_rows(
    const QueryState& query_state, const std::vector<Traverse>& traverses,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  auto rows = std::make_shared<std::vector<Row>>();
  std::set<int64_t> visited;  // Tracks processed nodes across all schemas
  std::set<std::string> unique_schemas;
  std::vector<SchemaRef> schemas;

  print_paths(query_state.connections);

  // Phase 1: Process connected paths
  // --------------------------------
  // Identify all schemas in the query chain
  unique_schemas.insert(query_state.from.value());
  schemas.push_back(query_state.from);

  // for (const auto& traverse : traverses) {
  //   if (unique_schemas.insert(traverse.source().value()).second) {
  //     schemas.push_back(traverse.source());
  //   }
  //   if (unique_schemas.insert(traverse.target().value()).second) {
  //     schemas.push_back(traverse.target());
  //   }
  // }

  log_debug("Phase 1: Processing connected paths from {} unique schemas",
            schemas.size());

  // Process each schema and build rows for connected paths
  for (const auto& schema_ref : schemas) {
    if (!query_state.ids.contains(schema_ref.value())) {
      log_warn("Schema '{}' not found in query state IDs", schema_ref.value());
      continue;
    }

    log_debug("Processing {} nodes from schema '{}'",
              query_state.ids.at(schema_ref.value()).size(),
              schema_ref.value());
    std::set<int64_t> schema_visited_nodes;
    for (auto id : query_state.ids.at(schema_ref.value())) {
      // Skip already visited nodes (they were already added as part of another
      // path)
      if (visited.find(id) != visited.end() ||
          !query_state.connections.contains(id)) {
        continue;
      }

      // Row row = create_empty_row_from_schema(output_schema);
      // std::set<int64_t> local_visited;
      // std::set<std::string> local_visited_schemas;
      // auto res =
      //     populate_rows_dfs(row, id, schema_ref, rows, query_state,
      //     local_visited, local_visited_schemas, unique_schemas.size());
      // schema_visited_nodes.insert(local_visited.begin(),
      // local_visited.end());
      auto res = populate_rows_bfs(id, schema_ref, output_schema, query_state);
      if (!res.ok()) {
        log_error("Failed to populate row for node {} in schema '{}': {}", id,
                  schema_ref.value(), res.status().ToString());
        return res.status();
      }
      auto res_value = res.ValueOrDie();
      // rows.splice(rows.end(), res.ValueOrDie());
      rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                   std::make_move_iterator(res_value->end()));
    }

    visited.insert(schema_visited_nodes.begin(), schema_visited_nodes.end());
  }

  log_debug("Phase 1 complete. Found {} connected paths, visited {} nodes",
            rows->size(), visited.size());

  // Phase 2: Process unmatched nodes for outer joins
  // -----------------------------------------------
  log_debug("Phase 2: Processing outer joins");

  // Create a map to track which traverses apply to each schema
  std::unordered_map<std::string, std::vector<const Traverse*>>
      source_traverses;
  std::unordered_map<std::string, std::vector<const Traverse*>>
      target_traverses;

  for (const auto& traverse : traverses) {
    // Skip inner joins - they're fully handled in phase 1
    if (traverse.traverse_type() == TraverseType::Inner) {
      continue;
    }

    source_traverses[traverse.source().value()].push_back(&traverse);
    target_traverses[traverse.target().value()].push_back(&traverse);
  }

  // Handle LEFT and FULL joins - process unvisited source nodes
  /*
    for (const auto& [alias, traverses_list] : source_traverses) {
      for (const auto* traverse : traverses_list) {
        if (traverse->traverse_type() != TraverseType::Left &&
            traverse->traverse_type() != TraverseType::Full) {
          continue;
        }

        log_debug(
            "Processing LEFT/FULL join for source '{}' in traverse '{} -> {}'",
            traverse->source().value(), traverse->source().value(),
            traverse->target().value());

        for (auto id : query_state.ids.at(traverse->source().value())) {
          if (visited.find(id) != visited.end()) {
            continue;  // Skip nodes already processed in phase 1
          }

          log_debug("Creating row for unmatched source node {} in LEFT/FULL
    join", id); Row row = create_empty_row_from_schema(output_schema); auto res
    = populate_rows_dfs(row, id, traverse->source(), rows, query_state,
    visited); if (!res.ok()) { log_error("Failed to populate row for unmatched
    source node {}: {}", id, res.status().ToString()); return res.status();
          }
        }
      }
    }

    // Handle RIGHT and FULL joins - process unvisited target nodes
    for (const auto& [alias, traverses_list] : target_traverses) {
      for (const auto* traverse : traverses_list) {
        if (traverse->traverse_type() != TraverseType::Right &&
            traverse->traverse_type() != TraverseType::Full) {
          continue;
        }

        log_debug(
            "Processing RIGHT/FULL join for target '{}' in traverse '{} -> {}'",
            traverse->target().value(), traverse->source().value(),
            traverse->target().value());

        for (auto id : query_state.ids.at(traverse->target().value())) {
          if (visited.find(id) != visited.end()) {
            continue;  // Skip nodes already processed in phase 1
          }

          log_debug(
              "Creating row for unmatched target node {} in RIGHT/FULL join",
    id); Row row = create_empty_row_from_schema(output_schema); auto res =
    populate_rows_dfs(row, id, traverse->target(), rows, query_state, visited);
          if (!res.ok()) {
            log_error("Failed to populate row for unmatched target node {}: {}",
                      id, res.status().ToString());
            return res.status();
          }
        }
      }
    }
    */

  log_debug("Phase 2 complete. Total of {} rows created", rows->size());
  return rows;  //  std::make_shared<std::vector<Row>>(std::mov);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_rows(
    const std::shared_ptr<std::vector<Row>>& rows,
    const std::shared_ptr<arrow::Schema>& schema = nullptr) {
  if (!rows || rows->empty()) {
    return arrow::Status::Invalid("No rows provided to create table");
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

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state;
  log_info("Executing query starting from schema '{}'",
           query.from().toString());
  query_state.node_manager = this->node_manager;
  query_state.schema_registry = this->schema_registry;
  query_state.from = query.from();

  // node id -> [node id]
  // All nodes that are part of the query result, by schema
  std::unordered_map<std::string, std::set<int64_t>> selected;

  auto init_from_table_result = query_state.init_table(*this, query.from());
  if (!init_from_table_result.ok()) {
    log_error("Failed to get initial table from schema '{}'",
              query.from().toString());
    return init_from_table_result.status();
  }
  // std::vector<Traverse> traverses;
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
        auto res = query_state.update_table(filtered_table_result.ValueOrDie(),
                                            variable);
        if (!res.ok()) {
          return res.status();
        }
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        query_state.traversals.push_back(*traverse);
        log_info("Processing TRAVERSE on edge type '{}' from source '{}'",
                 traverse->edge_type(), traverse->source().toString());
        auto source = traverse->source();
        if (!query_state.tables.contains(source.value())) {
          log_debug("Source '{}' not found. Loading",
                    traverse->source().toString());
          auto res = query_state.init_table(*this, traverse->source());
          if (!res.ok()) {
            return res.status();
          }
        }

        log_debug("Traversing from {} source nodes",
                  query_state.ids[source.value()].size());

        auto source_schema = query_state.aliases[source.value()];
        std::vector<std::shared_ptr<Node>> neighbors;
        std::set<int64_t> unmatched_source_ids;
        for (auto source_id : query_state.ids[source.value()]) {
          auto outgoing_edges =
              edge_store->get_outgoing_edges(source_id, traverse->edge_type())
                  .ValueOrDie();  // todo check result
          log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                    outgoing_edges.size(), traverse->edge_type());
          if (outgoing_edges.empty()) {
            unmatched_source_ids.insert(source_id);
            // query_state.ids[source.value()].erase(source_id);
          }
          for (auto edge : outgoing_edges) {
            auto target_id = edge->get_target_id();
            log_info(">> edge {} -[{}]-> {}", source_id, traverse->edge_type(),
                     target_id);
            auto node_result = node_manager->get_node(target_id);
            if (!node_result.ok()) {
              log_error("Failed to get node {}: {}", target_id,
                        node_result.status().ToString());
              // should probably skip in case of concurrent remove
              return node_result.status();
            }
            auto node = node_result.ValueOrDie();

            // Fixed schema comparison - check against target schema, not source
            // schema
            log_debug("Node schema: '{}', target schema: '{}'",
                      node->schema_name, traverse->target().schema());

            if (traverse->target().schema() == node->schema_name) {
              neighbors.push_back(node);
              query_state.connections[source_id].push_back(GraphConnection{
                  traverse->source(), source_id, traverse->edge_type(), "",
                  traverse->target(), target_id});
            } else {
              log_warn(
                  "Node {} schema '{}' doesn't match target schema '{}' in "
                  "traversal",
                  target_id, node->schema_name, traverse->target().schema());
            }
          }
        }
        if (traverse->traverse_type() == TraverseType::Inner) {
          for (auto id : unmatched_source_ids) {
            log_debug("remove unmatched node={}", id);
            query_state.ids[source.value()].erase(id);
          }
          // query_state.ids[source.value()].erase(unmatched_source_ids.begin(),
          // unmatched_source_ids.end());
        }
        log_debug("found {} neighbors", neighbors.size());
        if (traverse->traverse_type() == TraverseType::Inner) {
          auto table_result =
              create_table_from_nodes(schema_registry, neighbors);
          if (!table_result.ok()) {
            log_error("Failed to create table from nodes: {}",
                      table_result.status().ToString());
            return table_result.status();
          }
          auto target_table_init_result = query_state.init_table(
              table_result.ValueOrDie(), traverse->target());
          if (!target_table_init_result.ok()) {
            log_error("Failed to init table from neighbors: {}",
                      target_table_init_result.status().ToString());
            return target_table_init_result.status();
          }
        } else {
          auto target_table_init_result =
              query_state.init_table(*this, traverse->target());
          if (!target_table_init_result.ok()) {
            return target_table_init_result.status();
          }
        }

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
  auto result = std::make_shared<QueryResult>();

  auto output_schema_res = build_denormalized_schema(query_state);
  if (!output_schema_res.ok()) {
    return output_schema_res.status();
  }
  const auto output_schema = output_schema_res.ValueOrDie();
  log_info("output_schema={}", output_schema->ToString());
  query_state.ids["u"] = {1};
  auto row_res =
      populate_rows(query_state, query_state.traversals, output_schema);
  if (!row_res.ok()) {
    return row_res.status();
  }
  auto rows = row_res.ValueOrDie();
  auto output_table_res = create_table_from_rows(rows, output_schema);
  if (!output_table_res.ok()) {
    return output_table_res.status();
  }
  result->set_table(output_table_res.ValueOrDie());

  return result;
}

}  // namespace tundradb
