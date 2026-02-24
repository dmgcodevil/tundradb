#include "query_execution.hpp"

#include "arrow_utils.hpp"
#include "logger.hpp"

namespace tundradb {

// SchemaContext implementation

arrow::Result<std::string> SchemaContext::register_schema(
    const SchemaRef& schema_ref) {
  if (aliases_.contains(schema_ref.value()) && schema_ref.is_declaration()) {
    IF_DEBUG_ENABLED {
      log_debug("Schema alias '{}' already assigned to '{}'",
                schema_ref.value(), aliases_.at(schema_ref.value()));
    }
    return aliases_[schema_ref.value()];
  }

  if (schema_ref.is_declaration()) {
    aliases_[schema_ref.value()] = schema_ref.schema();
    return schema_ref.schema();
  }

  return aliases_[schema_ref.value()];
}

arrow::Result<std::string> SchemaContext::resolve(
    const SchemaRef& schema_ref) const {
  if (schema_ref.is_declaration()) {
    return schema_ref.schema();
  }

  auto it = aliases_.find(schema_ref.value());
  if (it == aliases_.end()) {
    return arrow::Status::KeyError("No alias for '{}'", schema_ref.value());
  }

  return it->second;
}

// FieldIndexer implementation

arrow::Result<bool> FieldIndexer::compute_fq_names(
    const SchemaRef& schema_ref, const std::string& resolved_schema,
    SchemaRegistry* registry) {
  const std::string& alias = schema_ref.value();
  if (fq_field_names_.contains(alias)) {
    return false;  // Already computed
  }

  auto schema_res = registry->get(resolved_schema);
  if (!schema_res.ok()) {
    return schema_res.status();
  }

  const auto& schema = schema_res.ValueOrDie();
  std::vector<std::string> names;
  std::vector<int> indices;
  names.reserve(schema->num_fields());
  indices.reserve(schema->num_fields());

  for (const auto& field : schema->fields()) {
    std::string fq_name = alias + "." + field->name();
    int field_id = next_field_id_.fetch_add(1);

    names.emplace_back(fq_name);
    indices.emplace_back(field_id);

    field_id_to_name_[field_id] = fq_name;
    field_name_to_index_[fq_name] = field_id;
  }

  fq_field_names_[alias] = std::move(names);
  schema_field_indices_[alias] = std::move(indices);

  return true;
}

// QueryState implementation

QueryState::QueryState(std::shared_ptr<SchemaRegistry> registry)
    : schemas(std::move(registry)) {}

void QueryState::reserve_capacity(const Query& query) {
  // Estimate schema count from FROM + TRAVERSE clauses
  size_t estimated_schemas = 1;  // FROM clause
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::TRAVERSE) {
      estimated_schemas += 2;  // source + target schemas
    }
  }

  // Pre-size standard containers
  tables.reserve(estimated_schemas);
}

arrow::Result<bool> QueryState::compute_fully_qualified_names(
    const SchemaRef& schema_ref) {
  const auto& aliases_map = schemas.get_aliases();
  const auto it = aliases_map.find(schema_ref.value());
  if (it == aliases_map.end()) {
    return arrow::Status::KeyError("keyset does not contain alias '{}'",
                                   schema_ref.value());
  }
  return compute_fully_qualified_names(schema_ref, it->second);
}

arrow::Result<bool> QueryState::update_table(
    const std::shared_ptr<arrow::Table>& table, const SchemaRef& schema_ref) {
  this->tables[schema_ref.value()] = table;
  auto ids_result = get_ids_from_table(table);
  if (!ids_result.ok()) {
    log_error("Failed to get IDs from table: {}", schema_ref.value());
    return ids_result.status();
  }
  graph.ids(schema_ref.value()) = ids_result.ValueOrDie();
  return true;
}

std::string QueryState::ToString() const {
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

  ss << "  Aliases (" << schemas.get_aliases().size() << "):\n";
  for (const auto& [alias, schema_name] : schemas.get_aliases()) {
    ss << "    - " << alias << " -> " << schema_name << "\n";
  }

  ss << "  Connections (Outgoing) (" << graph.outgoing().size()
     << " source nodes):";
  for (const auto& [from_schema, conns] : graph.outgoing()) {
    for (const auto& [from_id, conn_vec] : conns) {
      ss << "from " << from_schema.str() << ":" << from_id << ":\n";
      for (const auto& conn : conn_vec) {
        ss << "    - " << conn.target.value() << ":" << conn.target_id << "\n";
      }
    }
  }

  ss << "  Connections (Incoming) (" << graph.incoming().size()
     << " target nodes):";
  int target_nodes_printed = 0;
  for (const auto& [target_id, conns_vec] : graph.incoming()) {
    if (target_nodes_printed >= 3 && graph.incoming().size() > 5) {
      ss << "      ... and " << (graph.incoming().size() - target_nodes_printed)
         << " more target nodes ...\n";
      break;
    }
    ss << "    - Target ID " << target_id << " (" << conns_vec.size()
       << " incoming):";
    int conns_printed_for_target = 0;
    for (const auto& conn : conns_vec) {
      if (conns_printed_for_target >= 3 && conns_vec.size() > 5) {
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

}  // namespace tundradb
