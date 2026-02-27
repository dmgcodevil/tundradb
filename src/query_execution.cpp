#include "query_execution.hpp"

#include <ranges>
#include <unordered_set>

#include "arrow_utils.hpp"
#include "logger.hpp"
#include "utils.hpp"

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
  IF_DEBUG_ENABLED {
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
      for (auto const& next : connections.at(curr)) {
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

arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema(
    const QueryState& query_state) {
  IF_DEBUG_ENABLED { log_debug("Building schema for denormalized table"); }

  std::set<std::string> processed_fields;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::set<std::string> processed_schemas;

  std::string from_schema = query_state.from.value();

  IF_DEBUG_ENABLED {
    log_debug("Adding fields from FROM schema '{}'", from_schema);
  }

  auto schema_result =
      query_state.schema_registry()->get(query_state.aliases().at(from_schema));
  if (!schema_result.ok()) {
    return schema_result.status();
  }

  auto schema = schema_result.ValueOrDie();
  auto arrow_schema = schema->arrow();
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
    IF_DEBUG_ENABLED {
      log_debug("Adding fields from schema '{}'", schema_ref.value());
    }

    schema_result = query_state.schema_registry()->get(
        query_state.aliases().at(schema_ref.value()));
    if (!schema_result.ok()) {
      return schema_result.status();
    }

    arrow_schema = schema_result.ValueOrDie()->arrow();
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

void log_grouped_connections(
    int64_t node_id,
    const llvm::SmallDenseMap<llvm::StringRef,
                              llvm::SmallVector<GraphConnection, 4>, 4>&
        grouped_connections) {
  IF_DEBUG_ENABLED {
    if (grouped_connections.empty()) {
      log_debug("Node {} has no grouped connections", node_id);
      return;
    }

    log_debug("Node {} has connections to {} target schemas:", node_id,
              grouped_connections.size());

    for (const auto& it : grouped_connections) {
      auto target_schema = it.first;
      const auto& connections = it.second;
      log_debug("  To schema '{}': {} connections", target_schema.str(),
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

std::shared_ptr<arrow::Table> apply_select(
    const std::shared_ptr<Select>& select,
    const std::shared_ptr<arrow::Table>& table) {
  if (!select || select->fields().empty()) {
    return table;
  }

  std::vector<std::string> all_columns;
  for (const auto& field : table->schema()->fields()) {
    all_columns.push_back(field->name());
  }

  std::unordered_set<int> columns_to_keep;

  for (const auto& field : select->fields()) {
    bool is_prefix = true;

    if (field.find('.') != std::string::npos) {
      for (int i = 0; i < static_cast<int>(all_columns.size()); ++i) {
        if (all_columns[i] == field) {
          columns_to_keep.insert(i);
          break;
        }
      }
      is_prefix = false;
    }

    if (is_prefix) {
      std::string prefix = field + ".";
      for (int i = 0; i < static_cast<int>(all_columns.size()); ++i) {
        if (all_columns[i].find(prefix) == 0) {
          columns_to_keep.insert(i);
        }
      }
    }
  }

  std::vector<int> column_indices(columns_to_keep.begin(),
                                  columns_to_keep.end());
  std::ranges::sort(column_indices);

  arrow::Result<std::shared_ptr<arrow::Table>> result =
      table->SelectColumns(column_indices);

  if (!result.ok()) {
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
        IF_DEBUG_ENABLED {
          log_debug("inline where: '{}'", where_expr->toString());
        }
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
    IF_DEBUG_ENABLED { log_debug("inline where '{}'", exp->toString()); }
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

arrow::Status prepare_query(Query& query, QueryState& query_state) {
  // Phase 1: Process FROM clause to populate aliases
  {
    ARROW_ASSIGN_OR_RAISE(auto from_schema,
                          query_state.register_schema(query.from()));
  }

  // Phase 2: Process TRAVERSE clauses to populate aliases and traversals
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::TRAVERSE) {
      auto traverse = std::dynamic_pointer_cast<Traverse>(clause);

      ARROW_ASSIGN_OR_RAISE(auto source_schema,
                            query_state.register_schema(traverse->source()));
      ARROW_ASSIGN_OR_RAISE(auto target_schema,
                            query_state.register_schema(traverse->target()));

      if (!traverse->source().is_declaration()) {
        traverse->mutable_source().set_schema(source_schema);
      }

      if (!traverse->target().is_declaration()) {
        traverse->mutable_target().set_schema(target_schema);
      }

      traverse->mutable_source().set_tag(compute_tag(traverse->source()));
      traverse->mutable_target().set_tag(compute_tag(traverse->target()));

      query_state.traversals.push_back(*traverse);
    }
  }

  // Phase 3: Resolve all ComparisonExpr field references
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::WHERE) {
      auto where_expr = std::dynamic_pointer_cast<WhereExpr>(clause);
      auto res = where_expr->resolve_field_ref(
          query_state.aliases(), query_state.schema_registry().get());
      if (!res.ok()) {
        return res.status();
      }
    }
  }

  return arrow::Status::OK();
}

}  // namespace tundradb
