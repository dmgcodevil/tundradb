#include <llvm/ADT/DenseSet.h>

#include <set>

#include "main/database.hpp"

namespace tundradb {

/// Dispatch an update request to either the ID-based or MATCH-based execution
/// path.
arrow::Result<UpdateResult> Database::update(const UpdateQuery& uq) {
  if (uq.node_id().has_value()) {
    return update_by_id(uq);
  }
  if (uq.has_match()) {
    return update_by_match(uq);
  }
  return arrow::Status::Invalid(
      "UpdateQuery must specify a node ID or a MATCH query");
}

/// Resolve and apply assignments to a single node addressed by schema and ID.
arrow::Result<UpdateResult> Database::update_by_id(const UpdateQuery& uq) {
  UpdateResult result;

  auto schema_result = schema_registry_->get(uq.schema());
  if (!schema_result.ok()) {
    return arrow::Status::KeyError("Schema '", uq.schema(), "' not found");
  }
  const auto& schema = schema_result.ValueOrDie();

  // Resolve fields upfront - fail early on bad field names
  std::vector<FieldUpdate> resolved;
  resolved.reserve(uq.assignments().size());
  for (const auto& a : uq.assignments()) {
    const size_t first_dot = a.field_name.find('.');
    const std::string field_name = first_dot == std::string::npos
                                       ? a.field_name
                                       : a.field_name.substr(0, first_dot);
    if (field_name.empty()) {
      return arrow::Status::Invalid("Invalid SET field '", a.field_name,
                                    "' for ID-based update");
    }
    std::vector<std::string> nested_path{};
    if (first_dot != std::string::npos) {
      size_t start = first_dot + 1;
      while (start < a.field_name.size()) {
        const size_t end = a.field_name.find('.', start);
        std::string segment = a.field_name.substr(
            start, end == std::string::npos ? std::string::npos : end - start);
        if (segment.empty()) {
          return arrow::Status::Invalid("Invalid SET field '", a.field_name,
                                        "' for ID-based update");
        }
        nested_path.push_back(std::move(segment));
        if (end == std::string::npos) break;
        start = end + 1;
      }
    }

    auto field = schema->get_field(field_name);
    if (!field) {
      return arrow::Status::Invalid(
          "Field '", field_name, "' not found in schema '", uq.schema(), "'");
    }
    resolved.push_back(
        FieldUpdate{field, a.value, uq.update_type(), std::move(nested_path)});
  }

  const int64_t id = uq.node_id().value();
  if (const auto r =
          update_node_fields(uq.schema(), id, resolved, uq.update_type());
      !r.ok()) {
    result.failed_count++;
    result.errors.push_back(uq.schema() + "(" + std::to_string(id) +
                            "): " + r.status().ToString());
  } else {
    result.updated_count = 1;
  }
  return result;
}

/// Run the MATCH query once, group assignments by alias, and apply the updates
/// to each matching node or edge.
arrow::Result<UpdateResult> Database::update_by_match(const UpdateQuery& uq) {
  UpdateResult result;
  const auto& match_query = uq.match_query().value();

  // 1. Build alias -> schema from node declarations
  std::unordered_map<std::string, std::string> alias_to_schema;
  if (match_query.from().is_declaration())
    alias_to_schema[match_query.from().value()] = match_query.from().schema();
  for (const auto& clause : match_query.clauses()) {
    if (clause->type() != Clause::Type::TRAVERSE) continue;
    auto t = std::static_pointer_cast<Traverse>(clause);
    if (t->source().is_declaration())
      alias_to_schema[t->source().value()] = t->source().schema();
    if (t->target().is_declaration())
      alias_to_schema[t->target().value()] = t->target().schema();
  }

  // 2. Group SET assignments by alias: { alias -> [FieldUpdate] }
  std::unordered_map<std::string, std::vector<FieldUpdate>> updates_by_alias;
  for (const auto& a : uq.assignments()) {
    const auto parsed = FieldRef::from_string(a.field_name);
    if (parsed.variable().empty()) {
      return arrow::Status::Invalid(
          "SET field '", a.field_name,
          "' must be alias-qualified (e.g. u.age) in a MATCH-based update");
    }
    const std::string& alias = parsed.variable();
    const std::string& bare_field = parsed.field_name();

    std::shared_ptr<Field> field;
    if (auto trav = match_query.find_traverse(alias); trav != nullptr) {
      auto edge_schema = edge_store_->get_edge_schema(trav->edge_type());
      if (!edge_schema) {
        return arrow::Status::KeyError("Edge schema '", trav->edge_type(),
                                       "' not found");
      }
      field = edge_schema->get_field(bare_field);
      if (!field) {
        return arrow::Status::Invalid("Field '", bare_field,
                                      "' not found in edge schema '",
                                      trav->edge_type(), "'");
      }
    } else {
      auto it = alias_to_schema.find(alias);
      if (it == alias_to_schema.end()) {
        return arrow::Status::Invalid("Alias '", alias,
                                      "' not found in MATCH query");
      }
      ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry_->get(it->second));
      field = schema->get_field(bare_field);
      if (!field) {
        return arrow::Status::Invalid(
            "Field '", bare_field, "' not found in schema '", it->second, "'");
      }
    }
    updates_by_alias[alias].push_back(
        FieldUpdate{field, a.value, uq.update_type(), parsed.nested_path()});
  }

  // 3. Build SELECT with node IDs needed for updates and edge lookups.
  std::set<std::string> id_column_set;
  for (const auto& [alias, _] : updates_by_alias) {
    if (auto trav = match_query.find_traverse(alias)) {
      id_column_set.insert(trav->source().value() + ".id");
      id_column_set.insert(trav->target().value() + ".id");
    } else {
      id_column_set.insert(alias + ".id");
    }
  }
  Query id_query(match_query.from(), match_query.clauses(),
                 std::make_shared<Select>(std::vector<std::string>(
                     id_column_set.begin(), id_column_set.end())),
                 match_query.inline_where(), match_query.execution_config(),
                 match_query.temporal_snapshot());

  // 4. Run the MATCH query once
  ARROW_ASSIGN_OR_RAISE(auto query_result, this->query(id_query));
  auto table = query_result->table();
  if (!table || table->num_rows() == 0) {
    return result;
  }

  // 5. Apply updates per alias
  for (const auto& [alias, fields] : updates_by_alias) {
    if (auto trav = match_query.find_traverse(alias); !trav) {
      auto id_column = table->GetColumnByName(alias + ".id");
      if (!id_column) {
        return arrow::Status::Invalid("Could not find '", alias,
                                      ".id' column in query results");
      }
      apply_updates(alias_to_schema.at(alias), id_column, fields,
                    uq.update_type(), result);
    } else {
      auto src_col = table->GetColumnByName(trav->source().value() + ".id");
      auto tgt_col = table->GetColumnByName(trav->target().value() + ".id");
      if (!src_col || !tgt_col) {
        return arrow::Status::Invalid(
            "Could not find source/target ID columns for edge alias '", alias,
            "'");
      }
      llvm::DenseSet<int64_t> updated_edge_ids;
      for (int ci = 0; ci < src_col->num_chunks(); ci++) {
        const auto src_chunk =
            std::static_pointer_cast<arrow::Int64Array>(src_col->chunk(ci));
        const auto tgt_chunk =
            std::static_pointer_cast<arrow::Int64Array>(tgt_col->chunk(ci));
        for (int64_t i = 0; i < src_chunk->length(); i++) {
          if (src_chunk->IsNull(i) || tgt_chunk->IsNull(i)) continue;
          auto edges_res = edge_store_->get_outgoing_edges(src_chunk->Value(i),
                                                           trav->edge_type());
          if (!edges_res.ok()) continue;
          for (const auto& edge : edges_res.ValueOrDie()) {
            if (edge->get_target_id() != tgt_chunk->Value(i)) continue;
            if (!updated_edge_ids.insert(edge->get_id()).second) continue;
            if (auto upd = edge->update_fields(fields); !upd.ok()) {
              result.failed_count++;
              result.errors.push_back("edge(" + std::to_string(edge->get_id()) +
                                      "): " + upd.status().ToString());
            } else {
              result.updated_count++;
            }
          }
        }
      }
    }
  }

  return result;
}

/// Apply the same field updates to every non-null node ID found in the given
/// ID column.
void Database::apply_updates(
    const std::string& schema_name,
    const std::shared_ptr<arrow::ChunkedArray>& id_column,
    const std::vector<FieldUpdate>& fields, UpdateType update_type,
    UpdateResult& result) {
  for (int ci = 0; ci < id_column->num_chunks(); ci++) {
    const auto chunk =
        std::static_pointer_cast<arrow::Int64Array>(id_column->chunk(ci));
    for (int64_t i = 0; i < chunk->length(); i++) {
      if (chunk->IsNull(i)) continue;
      const int64_t node_id = chunk->Value(i);

      if (auto r =
              update_node_fields(schema_name, node_id, fields, update_type);
          !r.ok()) {
        result.failed_count++;
        result.errors.push_back(schema_name + "(" + std::to_string(node_id) +
                                "): " + r.status().ToString());
      } else {
        result.updated_count++;
      }
    }
  }
}

}  // namespace tundradb
