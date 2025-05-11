#include "../include/core.hpp"

#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <fmt/ranges.h>

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <stack>
#include <thread>
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

  auto roots = get_roots(query_state.connections);
  std::set<std::string> processed;

  std::vector<int64_t> stack;
  for (auto id : roots) {
    stack.push_back(id);

    auto schema_name =
        query_state.node_manager->get_node(id).ValueOrDie()->schema_name;
    if (processed.insert(schema_name).second) {
      for (auto field : query_state.schema_registry->get(schema_name)
                            .ValueOrDie()
                            ->fields()) {
        processed_fields.insert(field->name());
        fields.push_back(field);
      }
    }
  }

  while (stack.size() > 0) {
    auto id = stack.back();
    stack.pop_back();

    if (query_state.connections.contains(id)) {
      for (auto const& conn : query_state.connections.at(id)) {
        std::string schema_name = conn.target.value();
        if (processed.insert(schema_name).second) {
          auto schema = query_state.schema_registry->get(conn.target.schema())
                            .ValueOrDie();
          for (auto field_name : schema->field_names()) {
            auto full_field_name = schema_name + "." + field_name;
            if (processed_fields.contains(full_field_name)) {
              return arrow::Status::KeyError("Field '{}' already exists",
                                             full_field_name);
            }
            processed_fields.insert(full_field_name);
            fields.push_back(arrow::field(
                full_field_name, schema->GetFieldByName(field_name)->type()));
          }
        }
        stack.push_back(conn.target_id);
      }
    }
  }

  return std::make_shared<arrow::Schema>(fields);
}

struct Row {
  // int64_t id;
  std::unordered_map<std::string, std::shared_ptr<arrow::Array>> cells;

  void set_cell(const std::string& name, std::shared_ptr<arrow::Array> array) {
    cells[name] = std::move(array);
  }
};

static Row create_empty_row_from_schema(
    const std::shared_ptr<arrow::Schema>& final_output_schema) {
  Row new_row;
  for (const auto& field : final_output_schema->fields()) {
    // Initialize with a null scalar of the correct type.
    // arrow::MakeNullScalar(field->type()) can create this.
    // If that's complex, a placeholder like `nullptr` could be used,
    // and the final append-to-builders step would handle it.
    // For true "no padding needed later", explicit typed nulls are best.
    new_row.cells[field->name()] = arrow::MakeNullScalar(
        field->type());  // Or your preferred null representation
    // fix compile error
  }
  return new_row;
}

arrow::Result<bool> populate_rows_dfs(Row& row, int64_t node_id,
                                      const SchemaRef& schema_ref,
                                      std::vector<Row>& rows,
                                      const QueryState& query_state,
                                      std::set<int64_t>& visited) {
  if (!visited.insert(node_id).second) {
    return false;
  }
  auto node_result = query_state.node_manager->get_node(node_id);
  if (!node_result.ok()) {
    return node_result.status();
  }
  auto node = node_result.ValueOrDie();
  for (const auto& [name, value] : node->data()) {
    auto full_name = schema_ref.value() + "." + name;
    row.set_cell(full_name, value);
  }
  if (query_state.connections.contains(node_id)) {
    for (const auto& conn : query_state.connections.at(node_id)) {
      Row next_row = row;
      auto res = populate_rows_dfs(next_row, conn.target_id, conn.target, rows,
                                   query_state, visited);
      if (!res.ok()) {
        return res.status();
      }
    }

  } else {
    rows.emplace_back(std::move(row));
  }
  return true;
}

// process all schemas used in traverse
// it only processes connected nodes
arrow::Result<std::shared_ptr<std::vector<Row>>> populate_rows(
    const QueryState& query_state, const std::vector<Traverse>& traverses,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  std::vector<Row> rows;
  std::set<int64_t> visited;
  std::set<std::string> unique_schemas;
  std::vector<SchemaRef> schemas;

  // first phase:
  // Process each schema from the traverse chain (A, B, C) independently
  // For each node in a schema, do a DFS of its connections
  // Build complete rows for valid paths
  // Track visited nodes to avoid duplicates
  unique_schemas.insert(query_state.from.value());
  schemas.push_back(query_state.from);

  for (const auto& traverse : traverses) {
    if (unique_schemas.insert(traverse.source().value()).second) {
      schemas.push_back(traverse.source());
    }
    if (unique_schemas.insert(traverse.target().value()).second) {
      schemas.push_back(traverse.target());
    }
  }

  for (const auto& schema_ref : schemas) {
    for (auto id : query_state.ids.at(schema_ref.value())) {
      Row row = create_empty_row_from_schema(output_schema);
      auto res =
          populate_rows_dfs(row, id, schema_ref, rows, query_state, visited);
      if (res.ok()) {
        return res.status();
      }
    }
  }

  // second phase:
  // After all connected paths are processed
  // For each schema with outer joins (LEFT/RIGHT/FULL)
  // Check which nodes weren't visited in Phase 1
  // Add appropriate rows with NULLs for the missing sides
  for (const auto& traverse : traverses) {
    if (traverse.traverse_type() == TraverseType::Left ||
        traverse.traverse_type() == TraverseType::Full) {
      for (auto id : query_state.ids.at(traverse.source().value())) {
        Row row = create_empty_row_from_schema(output_schema);
        auto res = populate_rows_dfs(row, id, traverse.source(), rows,
                                     query_state, visited);
        if (res.ok()) {
          return res.status();
        }
      }
    }

    if (traverse.traverse_type() == TraverseType::Right ||
        traverse.traverse_type() == TraverseType::Full) {
      for (auto id : query_state.ids.at(traverse.target().value())) {
        Row row = create_empty_row_from_schema(output_schema);
        auto res = populate_rows_dfs(row, id, traverse.target(), rows,
                                     query_state, visited);
        if (res.ok()) {
          return res.status();
        }
      }
    }
  }

  return {std::make_shared<std::vector<Row>>(rows)};
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_rows(
    const std::shared_ptr<std::vector<Row>>& rows) {
  // todo
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
  std::vector<Traverse> traverses;
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
        traverses.push_back(*traverse);
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
        for (auto source_id : query_state.ids[source.value()]) {
          auto outgoing_edges =
              edge_store->get_outgoing_edges(source_id, traverse->edge_type())
                  .ValueOrDie();  // todo check result
          log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                    outgoing_edges.size(), traverse->edge_type());

          for (auto edge : outgoing_edges) {
            auto target_id = edge->get_target_id();
            auto node_result = node_manager->get_node(target_id);
            if (!node_result.ok()) {
              log_error("Failed to get node {}: {}", target_id,
                        node_result.status().ToString());
              // should probably skip in case of concurrent remove
              return node_result.status();
            }
            auto node = node_result.ValueOrDie();

            if (source_schema == node->schema_name) {
              neighbors.push_back(node);
              query_state.connections[source_id].push_back(GraphConnection{
                  traverse->source(), source_id, traverse->edge_type(), "",
                  traverse->target(), target_id});
            }
          }
        }
        log_debug("found {} neighbors", neighbors.size());
        auto table_result = create_table_from_nodes(schema_registry, neighbors);
        if (!table_result.ok()) {
          log_error("Failed to create table from nodes: {}",
                    table_result.status().ToString());
          return table_result.status();
        }
        // if it's right / full => add all target ids
        auto target_table_init_result = query_state.init_table(
            table_result.ValueOrDie(), traverse->target());
        if (!target_table_init_result.ok()) {
          log_error("Failed to init table from neighbors: {}",
                    target_table_init_result.status().ToString());
          return target_table_init_result.status();
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
  auto row_res = populate_rows(query_state, traverses, output_schema);
  if (!row_res.ok()) {
    return row_res.status();
  }
  auto rows = row_res.ValueOrDie();
  auto output_table_res = create_table_from_rows(rows);
  if (!output_table_res.ok()) {
    return output_table_res.status();
  }
  result->set_table(output_table_res.ValueOrDie());

  return result;
}

}  // namespace tundradb
