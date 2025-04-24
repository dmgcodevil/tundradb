#include "../include/core.hpp"

#include <arrow/compute/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "logger.hpp"
namespace fs = std::filesystem;

namespace tundradb {

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_nodes(
    std::shared_ptr<SchemaRegistry> schema_registry,
    const std::vector<std::shared_ptr<Node>>& nodes) {
  if (nodes.empty()) {
    return arrow::Status::Invalid("Cannot create table from empty nodes list");
  }

  // All nodes should have the same schema
  std::string schema_name = nodes[0]->schema_name;

  // Get schema from SchemaRegistry
  ARROW_ASSIGN_OR_RAISE(auto arrow_schema, schema_registry->get(schema_name));

  // Create builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : arrow_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
  }

  // Populate builders with data from each node
  for (const auto& node : nodes) {
    // Ensure node has the expected schema
    if (node->schema_name != schema_name) {
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
          auto scalar = array->GetScalar(0).ValueOrDie();
          ARROW_RETURN_NOT_OK(builders[i]->AppendScalar(*scalar));
        } else {
          ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
        }
      } else {
        ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
      }
    }
  }

  // Finish building arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());
  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder->Finish(&array));
    arrays.push_back(array);
  }

  // Create table
  return arrow::Table::Make(arrow_schema, arrays);
}

arrow::Result<std::set<int64_t>> get_ids(std::shared_ptr<arrow::Table> table) {
  auto id_idx = table->schema()->GetFieldIndex("id");
  if (id_idx == -1) {
    return arrow::Status::Invalid("table does not have an 'id' column");
  }

  auto id_column = table->column(id_idx);
  std::set<int64_t> result_ids;

  for (int chunk_idx = 0; chunk_idx < id_column->num_chunks(); chunk_idx++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(
        id_column->chunk(chunk_idx));
    for (int i = 0; i < chunk->length(); i++) {
      result_ids.insert(chunk->Value(i));
    }
  }

  return result_ids;
}

arrow::Result<std::shared_ptr<arrow::Table>> filter(
    std::shared_ptr<arrow::Table> table, std::shared_ptr<Where> where) {
  auto value = where->value();
  auto op = where->op();
  auto field_name = where->field();

  // First check if the field exists
  auto field_idx = table->schema()->GetFieldIndex(field_name);
  if (field_idx == -1) {
    return arrow::Status::Invalid("Field '", field_name,
                                  "' not found in table");
  }

  // Get the column to filter on
  auto column = table->column(field_idx);

  // Create the comparison scalar
  arrow::compute::Expression scalar_value;
  arrow::compute::Expression field = arrow::compute::field_ref("field_name");

  switch (value.type()) {
    case ValueType::Int64:
      scalar_value = arrow::compute::literal(value.get<int64_t>());
      break;
    case ValueType::String:
      scalar_value = arrow::compute::literal(value.get<std::string>());
      break;
    // Add other types as needed
    default:
      return arrow::Status::Invalid("Unsupported value type");
  }

  arrow::compute::Expression op_exp;

  switch (op) {
    case CompareOp::Eq:
      op_exp = arrow::compute::equal(field, scalar_value);
      break;
    case CompareOp::NotEq:
      op_exp = arrow::compute::not_equal(field, scalar_value);
      break;
    case CompareOp::Gt:
      op_exp = arrow::compute::greater(field, scalar_value);
      break;
    case CompareOp::Lt:
      op_exp = arrow::compute::less(field, scalar_value);
      break;
    default:
      return arrow::Status::Invalid("Unsupported operation");
  }

  auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);

  // Create scanner builder
  ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());

  ARROW_RETURN_NOT_OK(scan_builder->Filter(op_exp));
  ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
  return scanner->ToTable();
}

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) {
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> front_tables;

  // initialize front
  front_tables[query.from_schema()] =
      get_table(query.from_schema()).ValueOrDie();

  std::unordered_map<std::string, std::set<int64_t>> front_ids;
  front_ids[query.from_schema()] =
      get_ids(get_table(query.from_schema()).ValueOrDie()).ValueOrDie();
  std::map<int64_t, std::vector<int64_t>> connections;  // node id -> [node id]
  // All nodes that are part of the query result, by schema
  std::unordered_map<std::string, std::set<int64_t>> selected;

  for (const auto& clause : query.clauses()) {
    switch (clause->type()) {
      // note: consecutive where clauses should be combined into one
      case Clause::Type::WHERE: {
        // applies to ALL tables in the active front that have the given field
        // then we need add filtered tables ids selected
        auto where = std::static_pointer_cast<Where>(clause);
        std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
        for (auto& [schema_name, table] : front_tables) {
          if (table->schema()->GetFieldIndex(where->field()) != -1) {
            front_tables[schema_name] = filter(table, where).ValueOrDie();
            auto filtered_ids = get_ids(front_tables[schema_name]).ValueOrDie();
            new_front_ids[schema_name].insert(filtered_ids.begin(),
                                              filtered_ids.end());
          }
        }
        front_ids = std::move(new_front_ids);
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        front_tables.clear();  // todo: do we need to clear front_tables ?
        std::unordered_map<std::string, std::vector<std::shared_ptr<Node>>>
            traversed_nodes;
        std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
        for (auto& [schema_name, ids] : front_ids) {
          if (traverse->target_schema().empty() ||
              traverse->target_schema().contains(schema_name)) {
            for (auto id : ids) {
              for (int64_t target_id :
                   edge_store->get_outgoing_edges(id, traverse->edge_type())) {
                auto node = node_manager->get_node(target_id).ValueOrDie();
                traversed_nodes[node->schema_name].push_back(node);
                new_front_ids[node->schema_name].insert(node->id);
              }
            }
          }
        }

        for (const auto& [schema, nodes] : traversed_nodes) {
          if (!nodes.empty()) {
            auto table_result = create_table_from_nodes(schema_registry, nodes);
            if (table_result.ok()) {
              front_tables[schema] = table_result.ValueOrDie();
            }
          }
        }

        front_ids = std::move(new_front_ids);
      }
      default:
        return arrow::Status::NotImplemented(
            "Database::query unsupported clause");
        // Other clause types
    }
  }

  return arrow::Status::NotImplemented("Database::query");
}

}  // namespace tundradb
