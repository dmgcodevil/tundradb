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
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables;
  std::unordered_map<std::string, std::set<int64_t>> ids;
  std::unordered_map<std::string, std::string> aliases;
  std::map<int64_t, std::vector<GraphConnection>> connections;

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

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state;
  log_info("Executing query starting from schema '{}'",
           query.from().toString());

  // node id -> [node id]
  // All nodes that are part of the query result, by schema
  std::unordered_map<std::string, std::set<int64_t>> selected;

  auto init_from_table_result = query_state.init_table(*this, query.from());
  if (!init_from_table_result.ok()) {
    log_error("Failed to get initial table from schema '{}'",
              query.from().toString());
    return init_from_table_result.status();
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
        auto res = query_state.update_table(filtered_table_result.ValueOrDie(),
                                            variable);
        if (!res.ok()) {
          return res.status();
        }
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
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

  // Add tables
  // for (const auto& [label, table] : front_tables) {
  //   log_debug("Adding table with label '{}' ({} rows) to result", label,
  //             table->num_rows());
  //   result->add_table(label, table);
  // }

  result->set_tables(query_state.tables);
  result->set_ids(query_state.ids);
  result->set_aliases(query_state.aliases);
  result->set_connections(query_state.connections);
  result->set_node_manager(node_manager);
  result->set_schema_registry(schema_registry);

  auto roots = get_roots(query_state.connections);
  log_debug("roots: {}", fmt::join(roots, ", "));

  return result;
}

arrow::Result<std::shared_ptr<arrow::Schema>>
QueryResult::build_denormalized_schema() const {
  log_info("Building schema for denormalized table");

  std::set<std::string> processed_fields;
  std::vector<std::shared_ptr<arrow::Field>> fields;

  auto roots = get_roots(connections_);
  std::set<std::string> processed;

  std::vector<int64_t> stack;
  for (auto id : roots) {
    stack.push_back(id);

    auto schema_name = node_manager_->get_node(id).ValueOrDie()->schema_name;
    if (processed.insert(schema_name).second) {
      for (auto field :
           schema_registry_->get(schema_name).ValueOrDie()->fields()) {
        processed_fields.insert(field->name());
        fields.push_back(field);
      }
    }
  }

  while (stack.size() > 0) {
    auto id = stack.back();
    stack.pop_back();

    if (connections_.contains(id)) {
      for (auto const& conn : connections_.at(id)) {
        std::string schema_name = conn.target.value();
        if (processed.insert(schema_name).second) {
          auto schema =
              schema_registry_->get(conn.target.schema()).ValueOrDie();
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

arrow::Result<std::shared_ptr<arrow::Table>>
QueryResult::populate_denormalized_table(
    const std::shared_ptr<arrow::Schema>& schema) const {
  log_info("Populating denormalized table with data");

  // Create builders for each field in the schema
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
  }

  // Map field names to their positions in the schema
  std::unordered_map<std::string, int> field_indices;
  for (int i = 0; i < schema->num_fields(); i++) {
    field_indices[schema->field(i)->name()] = i;
  }

  // Get roots of the connection graph
  std::set<int64_t> roots = get_roots(connections_);
  log_debug("Creating table with {} root nodes", roots.size());

  // For each root node, create a row in the denormalized table
  for (int64_t root_id : roots) {
    // Find the root node's schema
    auto node_result = node_manager_->get_node(root_id);
    if (!node_result.ok()) {
      log_warn("Could not find node with ID {}, skipping", root_id);
      continue;
    }

    auto root_node = node_result.ValueOrDie();
    std::string root_schema = root_node->schema_name;

    for (auto i = 0; i < connections_.at(root_id).size(); i++) {
      // Add each field from the root schema
      for (const auto& field_name :
           schema_registry_->get(root_schema).ValueOrDie()->field_names()) {
        if (field_indices.find(field_name) != field_indices.end()) {
          int idx = field_indices[field_name];
          auto field_result = root_node->get_field(field_name);
          if (field_result.ok()) {
            auto array = field_result.ValueOrDie();
            if (array->length() > 0) {
              auto scalar_result = array->GetScalar(0);
              if (scalar_result.ok()) {
                auto scalar = scalar_result.ValueOrDie();
                auto status = builders[idx]->AppendScalar(*scalar);
                if (!status.ok()) {
                  return status;
                }
              } else {
                ARROW_RETURN_NOT_OK(builders[idx]->AppendNull());
              }
            } else {
              ARROW_RETURN_NOT_OK(builders[idx]->AppendNull());
            }
          } else {
            ARROW_RETURN_NOT_OK(builders[idx]->AppendNull());
          }
        }
      }
    }

    // Track nodes we've already processed to avoid cycles
    std::set<int64_t> visited;
    visited.insert(root_id);

    // Process connected nodes
    std::stack<std::pair<int64_t, std::string>> node_stack;
    node_stack.push({root_id, ""});

    while (!node_stack.empty()) {
      auto [node_id, parent_prefix] = node_stack.top();
      node_stack.pop();

      // Add connected nodes' data
      if (connections_.find(node_id) != connections_.end()) {
        for (const auto& conn : connections_.at(node_id)) {
          if (visited.find(conn.target_id) != visited.end()) continue;
          visited.insert(conn.target_id);

          std::string target_schema = conn.target.schema();
          std::string prefix = conn.target.value();

          // Get target node
          auto target_node_result = node_manager_->get_node(conn.target_id);
          if (!target_node_result.ok()) {
            log_warn("Could not find target node with ID {}, skipping",
                     conn.target_id);
            continue;
          }

          auto target_node = target_node_result.ValueOrDie();
          auto target_arrow_schema =
              schema_registry_->get(target_schema).ValueOrDie();

          // Add each field from the target node with the appropriate prefix
          for (const auto& field : target_arrow_schema->fields()) {
            std::string field_name = field->name();
            std::string prefixed_field = prefix + "." + field_name;

            if (field_indices.find(prefixed_field) != field_indices.end()) {
              int idx = field_indices[prefixed_field];
              auto field_result = target_node->get_field(field_name);
              if (field_result.ok()) {
                auto array = field_result.ValueOrDie();
                if (array->length() > 0) {
                  auto scalar_result = array->GetScalar(0);
                  if (scalar_result.ok()) {
                    auto scalar = scalar_result.ValueOrDie();
                    auto status = builders[idx]->AppendScalar(*scalar);
                    if (!status.ok()) {
                      return status;
                    }
                  } else {
                    ARROW_RETURN_NOT_OK(builders[idx]->AppendNull());
                  }
                } else {
                  ARROW_RETURN_NOT_OK(builders[idx]->AppendNull());
                }
              } else {
                ARROW_RETURN_NOT_OK(builders[idx]->AppendNull());
              }
            }
          }

          // Add this node to the stack to traverse its connections
          node_stack.push({conn.target_id, prefix});
        }
      }
    }

    // Fill in nulls for any missing fields in this row
    for (size_t i = 0; i < builders.size(); i++) {
      // Check current lengths to see if we need to append null
      int64_t expected_length = builders[0]->length();
      if (builders[i]->length() < expected_length) {
        ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
      }
    }
  }

  // Finish builders and create arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());

  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder->Finish(&array));
    arrays.push_back(array);
  }

  // Create and return the table
  return arrow::Table::Make(schema, arrays);
}

}  // namespace tundradb
