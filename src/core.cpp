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
    std::shared_ptr<arrow::Table> table, std::shared_ptr<Where> where) {
  auto value = where->value();
  auto op = where->op();
  auto field_name = where->field();

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

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) {
  log_info("Executing query starting from schema '{}'", query.from_schema());
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> front_tables;

  // initialize front
  log_debug("Initializing query with schema '{}'", query.from_schema());
  auto initial_table_result = get_table(query.from_schema());
  if (!initial_table_result.ok()) {
    log_error("Failed to get initial table for schema '{}': {}",
              query.from_schema(), initial_table_result.status().ToString());
    return initial_table_result.status();
  }
  front_tables[query.from_schema()] = initial_table_result.ValueOrDie();

  log_debug("Getting IDs from initial table");
  auto initial_ids_result = get_ids(front_tables[query.from_schema()]);
  if (!initial_ids_result.ok()) {
    log_error("Failed to get IDs from initial table: {}",
              initial_ids_result.status().ToString());
    return initial_ids_result.status();
  }

  std::unordered_map<std::string, std::set<int64_t>> front_ids;
  front_ids[query.from_schema()] = initial_ids_result.ValueOrDie();
  log_debug("Initial front contains {} IDs from schema '{}'",
            front_ids[query.from_schema()].size(), query.from_schema());

  std::map<int64_t, std::vector<GraphConnection>>
      connections;  // node id -> [node id]
  // All nodes that are part of the query result, by schema
  std::unordered_map<std::string, std::set<int64_t>> selected;

  log_info("Processing {} query clauses", query.clauses().size());
  for (const auto& clause : query.clauses()) {
    switch (clause->type()) {
      // note: consecutive where clauses should be combined into one
      case Clause::Type::WHERE: {
        auto where = std::static_pointer_cast<Where>(clause);
        log_info("Processing WHERE clause on field '{}' with operator {}",
                 where->field(), static_cast<int>(where->op()));

        // applies to ALL tables in the active front that have the given field
        // then we need add filtered tables ids selected
        std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
        log_debug("Applying WHERE to {} schemas in current front",
                  front_tables.size());

        for (auto& [schema_name, table] : front_tables) {
          if (table->schema()->GetFieldIndex(where->field()) != -1) {
            log_debug("Filtering table for schema '{}' on field '{}'",
                      schema_name, where->field());
            auto filtered_table_result = filter(table, where);
            if (!filtered_table_result.ok()) {
              log_error("Failed to filter table for schema '{}': {}",
                        schema_name, filtered_table_result.status().ToString());
              return filtered_table_result.status();
            }
            front_tables[schema_name] = filtered_table_result.ValueOrDie();

            auto filtered_ids_result = get_ids(front_tables[schema_name]);
            if (!filtered_ids_result.ok()) {
              log_error("Failed to get IDs from filtered table: {}",
                        filtered_ids_result.status().ToString());
              return filtered_ids_result.status();
            }
            auto filtered_ids = filtered_ids_result.ValueOrDie();
            log_debug("Filter resulted in {} matching IDs for schema '{}'",
                      filtered_ids.size(), schema_name);
            front_ids[schema_name] = filtered_ids;
          } else {
            log_debug("Schema '{}' does not have field '{}', skipping filter",
                      schema_name, where->field());
          }
        }
        log_info("After WHERE: front contains {} schemas", front_ids.size());
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        log_info(
            "Processing TRAVERSE on edge type '{}' from source '{}' with label "
            "'{}'",
            traverse->edge_type(), traverse->source(), traverse->label());
        if (!front_ids.contains(traverse->source())) {
          log_debug("Source '{}' not found in current front. Loading",
                    traverse->source());
          front_tables[traverse->source()] =
              get_table(traverse->source()).ValueOrDie();
          front_ids[traverse->source()] =
              get_ids(front_tables[traverse->source()]).ValueOrDie();
        }

        std::unordered_map<std::string, std::vector<std::shared_ptr<Node>>>
            traversed_nodes;

        std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
        log_debug("Traversing from {} source nodes",
                  front_ids[traverse->source()].size());

        int edge_count = 0;
        for (auto source_id : front_ids[traverse->source()]) {
          auto outgoing_edges =
              edge_store->get_outgoing_edges(source_id, traverse->edge_type())
                  .ValueOrDie();
          log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                    outgoing_edges.size(), traverse->edge_type());
          edge_count += outgoing_edges.size();

          for (auto edge : outgoing_edges) {
            auto target_id = edge->get_target_id();
            auto node_result = node_manager->get_node(target_id);
            if (!node_result.ok()) {
              log_error("Failed to get node {}: {}", target_id,
                        node_result.status().ToString());
              return node_result.status();
            }
            auto node = node_result.ValueOrDie();

            if (traverse->target_schema().empty() ||
                traverse->target_schema().contains(node->schema_name)) {
              log_debug(
                  "Adding target node {} with schema '{}' to traversed nodes",
                  target_id, node->schema_name);
              traversed_nodes[node->schema_name].push_back(node);
              new_front_ids[node->schema_name].insert(target_id);
              connections[source_id].push_back(GraphConnection{
                  traverse->source(), source_id, traverse->edge_type(),
                  traverse->label(), node->schema_name, target_id});
            } else {
              log_debug(
                  "Target node {} with schema '{}' not in target schemas, "
                  "skipping",
                  target_id, node->schema_name);
            }
          }
        }
        log_info("Traversed {} edges, found {} target nodes across {} schemas",
                 edge_count,
                 std::accumulate(new_front_ids.begin(), new_front_ids.end(), 0,
                                 [](size_t sum, const auto& pair) {
                                   return sum + pair.second.size();
                                 }),
                 new_front_ids.size());

        for (const auto& [schema_name, nodes] : traversed_nodes) {
          if (!nodes.empty()) {
            log_debug("Creating table for {} nodes with schema '{}'",
                      nodes.size(), schema_name);
            auto full_name = traverse->label() + "_" + schema_name;
            auto table_result = create_table_from_nodes(schema_registry, nodes);
            if (!table_result.ok()) {
              log_error("Failed to create table from nodes: {}",
                        table_result.status().ToString());
              return table_result.status();
            }
            front_tables[full_name] = table_result.ValueOrDie();
            log_debug("Created table with label '{}' for schema '{}'",
                      full_name, schema_name);
          }
        }

        log_debug("Updating front IDs with {} new schemas",
                  new_front_ids.size());
        for (const auto& [schema_name, ids] : new_front_ids) {
          auto full_name = traverse->label() + "_" + schema_name;
          front_ids[full_name] = ids;
          log_debug("Added {} IDs to front with label '{}'", ids.size(),
                    full_name);
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
  for (const auto& [label, table] : front_tables) {
    log_debug("Adding table with label '{}' ({} rows) to result", label,
              table->num_rows());
    result->add_table(label, table);
  }

  result->set_node_manager(node_manager);
  result->set_schema_registry(schema_registry);

  // Set connections
  log_debug("Adding {} connections to result", connections.size());
  result->set_connections(connections);

  log_info("Returning query result with {} tables", front_tables.size());
  for (const auto& [schema_name, table] : front_tables) {
    result->add_table(schema_name, table);
  }

  for (const auto& [id, _] : connections) {
    std::vector<std::string> paths;
    log_debug("Node id: {} paths: ", id);
    debug_connections(id, connections, {}, paths);
    log_debug("   {}", fmt::join(paths, "\n "));
  }

  auto roots = get_roots(connections);
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
        std::string schema_name =
            conn.label.empty() ? conn.target : conn.label + "_" + conn.target;
        if (processed.insert(schema_name).second) {
          auto schema = schema_registry_->get(conn.target).ValueOrDie();
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

          std::string target_schema = conn.target;
          std::string prefix = conn.label.empty()
                                   ? target_schema
                                   : conn.label + "_" + target_schema;

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
