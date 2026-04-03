#include "query_execution.hpp"

#include <ranges>
#include <unordered_set>

#include "arrow_map_union_types.hpp"
#include "arrow_utils.hpp"
#include "constants.hpp"
#include "edge_store.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace tundradb {

namespace {

/**
 * Returns the canonical shadow-schema name for an edge type.
 *
 * Edge types like "WORKS_AT" are not node schemas, so they cannot be
 * stored directly in the SchemaRegistry.  Instead we create a synthetic
 * "shadow" schema (prefixed with "__edge__") that merges the structural
 * edge columns (_edge_id, source_id, target_id, created_ts) with the
 * user-defined edge properties.  This name is the key under which that
 * shadow schema is registered.
 */
std::string edge_shadow_schema_name(const std::string& edge_type) {
  return std::string(schema::kEdgeShadowPrefix) + edge_type;
}

/**
 * Lazily creates and registers a shadow Arrow schema for the given edge type.
 *
 * On first call for a given edge_type, this function:
 *  1. Looks up the edge schema from `EdgeStore` (user-defined fields like
 *     "since", "role").
 *  2. Prepends the four structural edge columns (_edge_id, source_id,
 *     target_id, created_ts).
 *  3. Registers the combined Arrow schema in the SchemaRegistry under the
 *     shadow name (e.g. "__edge__WORKS_AT").
 *
 * Subsequent calls for the same edge_type return the cached shadow name.
 *
 * The shadow schema allows the query engine to resolve edge field
 * references (e.g. `e.since`) through the same SchemaRegistry /
 * FieldRef resolution pipeline used for node fields.
 *
 * @param query_state  Mutable query state (provides schema registry and edge
 *                     store).
 * @param edge_type    The edge type name (e.g. "WORKS_AT").
 * @return The shadow schema name, or an error if the edge schema is missing.
 */
arrow::Result<std::string> ensure_edge_shadow_schema(
    QueryState& query_state, const std::string& edge_type) {
  const std::string shadow = edge_shadow_schema_name(edge_type);
  auto registry = query_state.schema_registry();
  if (registry->get(shadow).ok()) {
    return shadow;
  }
  if (!query_state.edge_store) {
    return arrow::Status::Invalid("Edge store is not available in query state");
  }
  auto edge_schema = query_state.edge_store->get_edge_schema(edge_type);
  if (!edge_schema) {
    return arrow::Status::KeyError("Edge schema '", edge_type, "' not found");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields{
      arrow::field(std::string(field_names::kEdgeId), arrow::int64()),
      arrow::field(std::string(field_names::kSourceId), arrow::int64()),
      arrow::field(std::string(field_names::kTargetId), arrow::int64()),
      arrow::field(std::string(field_names::kCreatedTs), arrow::int64())};
  std::unordered_set<std::string> seen{
      std::string(field_names::kEdgeId), std::string(field_names::kSourceId),
      std::string(field_names::kTargetId), std::string(field_names::kCreatedTs)};
  for (const auto& f : edge_schema->arrow()->fields()) {
    if (seen.insert(f->name()).second) {
      fields.push_back(f);
    }
  }
  ARROW_RETURN_NOT_OK(registry->create(shadow, arrow::schema(fields)));
  return shadow;
}

/**
 * @brief Project one key from a MAP column into a flat Arrow array.
 *
 * For each row in @p map_column, this function finds `key_name` in the row's
 * map entries, decodes the corresponding dense-union item, and emits one output
 * value (or null if key not present / value null / row map null).
 *
 * Output typing:
 * - Chooses a dominant `ValueType` from observed non-null values.
 * - If mixed concrete types are observed, falls back to string output.
 * - If all outputs are null/missing, returns a Null array.
 *
 * Preconditions:
 * - `map_column` chunks must have `arrow::Type::MAP`.
 * - MAP item type must be `arrow::Type::DENSE_UNION`.
 */
arrow::Result<std::shared_ptr<arrow::Array>> extract_map_values_for_key(
    const std::shared_ptr<arrow::ChunkedArray>& map_column,
    const std::string& key_name) {
  std::vector<std::optional<Value>> decoded_values;
  decoded_values.reserve(map_column->length());
  std::optional<ValueType> dominant_type;

  for (int ci = 0; ci < map_column->num_chunks(); ++ci) {
    auto chunk = map_column->chunk(ci);
    if (chunk->type_id() != arrow::Type::MAP) {
      return arrow::Status::Invalid("Base column is not a MAP column");
    }
    auto map_arr = std::static_pointer_cast<arrow::MapArray>(chunk);
    auto keys = std::static_pointer_cast<arrow::StringArray>(map_arr->keys());
    auto items = map_arr->items();
    if (items->type_id() != arrow::Type::DENSE_UNION) {
      return arrow::Status::Invalid(
          "Unsupported MAP item type for nested projection: ",
          items->type()->ToString());
    }

    for (int64_t row = 0; row < map_arr->length(); ++row) {
      if (map_arr->IsNull(row)) {
        decoded_values.emplace_back(std::nullopt);
        continue;
      }
      const int64_t begin = map_arr->value_offset(row);
      const int64_t end = begin + map_arr->value_length(row);
      std::optional<Value> hit;
      for (int64_t idx = begin; idx < end; ++idx) {
        if (keys->IsNull(idx)) continue;
        if (keys->GetString(idx) != key_name) continue;
        if (items->IsNull(idx)) {
          hit = std::nullopt;
        } else {
          ARROW_ASSIGN_OR_RAISE(hit, map_item_to_value(items, idx));
        }
        break;
      }
      decoded_values.push_back(hit);
      if (hit.has_value()) {
        const auto t = hit->type();
        if (!dominant_type.has_value()) {
          dominant_type = t;
        } else if (dominant_type.value() != t) {
          dominant_type = ValueType::STRING;  // fallback for mixed types
        }
      }
    }
  }

  if (!dominant_type.has_value()) {
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(arrow::NullBuilder().Finish(&arr));
    return arr;
  }

  switch (dominant_type.value()) {
    case ValueType::INT32: {
      arrow::Int32Builder b;
      for (const auto& v : decoded_values) {
        if (!v.has_value())
          ARROW_RETURN_NOT_OK(b.AppendNull());
        else
          ARROW_RETURN_NOT_OK(b.Append(v->as_int32()));
      }
      std::shared_ptr<arrow::Array> arr;
      ARROW_RETURN_NOT_OK(b.Finish(&arr));
      return arr;
    }
    case ValueType::INT64: {
      arrow::Int64Builder b;
      for (const auto& v : decoded_values) {
        if (!v.has_value())
          ARROW_RETURN_NOT_OK(b.AppendNull());
        else
          ARROW_RETURN_NOT_OK(b.Append(v->as_int64()));
      }
      std::shared_ptr<arrow::Array> arr;
      ARROW_RETURN_NOT_OK(b.Finish(&arr));
      return arr;
    }
    case ValueType::FLOAT: {
      arrow::FloatBuilder b;
      for (const auto& v : decoded_values) {
        if (!v.has_value())
          ARROW_RETURN_NOT_OK(b.AppendNull());
        else
          ARROW_RETURN_NOT_OK(b.Append(v->as_float()));
      }
      std::shared_ptr<arrow::Array> arr;
      ARROW_RETURN_NOT_OK(b.Finish(&arr));
      return arr;
    }
    case ValueType::DOUBLE: {
      arrow::DoubleBuilder b;
      for (const auto& v : decoded_values) {
        if (!v.has_value())
          ARROW_RETURN_NOT_OK(b.AppendNull());
        else
          ARROW_RETURN_NOT_OK(b.Append(v->as_double()));
      }
      std::shared_ptr<arrow::Array> arr;
      ARROW_RETURN_NOT_OK(b.Finish(&arr));
      return arr;
    }
    case ValueType::BOOL: {
      arrow::BooleanBuilder b;
      for (const auto& v : decoded_values) {
        if (!v.has_value())
          ARROW_RETURN_NOT_OK(b.AppendNull());
        else
          ARROW_RETURN_NOT_OK(b.Append(v->as_bool()));
      }
      std::shared_ptr<arrow::Array> arr;
      ARROW_RETURN_NOT_OK(b.Finish(&arr));
      return arr;
    }
    case ValueType::STRING:
    default: {
      arrow::StringBuilder b;
      for (const auto& v : decoded_values) {
        if (!v.has_value())
          ARROW_RETURN_NOT_OK(b.AppendNull());
        else
          ARROW_RETURN_NOT_OK(b.Append(v->to_string()));
      }
      std::shared_ptr<arrow::Array> arr;
      ARROW_RETURN_NOT_OK(b.Finish(&arr));
      return arr;
    }
  }
}

/**
 * @brief Materialize one-level nested MAP projections requested by SELECT.
 *
 * Scans SELECT fields for dotted paths that are not already present in the
 * result table (e.g. `u.props.role`), extracts values from base MAP columns,
 * and appends synthesized columns to the table.
 *
 * Current scope:
 * - Supports one-level nested map lookup only (`nested_path().size() == 1`).
 * - Ignores unsupported/select fields that are already materialized.
 *
 * @param select Parsed SELECT clause.
 * @param table Base result table.
 * @return Table enriched with derived nested MAP columns.
 */
arrow::Result<std::shared_ptr<arrow::Table>> enrich_nested_select_fields(
    const std::shared_ptr<Select>& select,
    std::shared_ptr<arrow::Table> table) {
  if (!select || select->fields().empty()) return table;

  for (const auto& sf : select->fields()) {
    if (sf.find('.') == std::string::npos) continue;
    if (table->GetColumnByName(sf) != nullptr) continue;

    const auto parsed = FieldRef::from_string(sf);
    if (parsed.nested_path().empty() || parsed.nested_path().size() != 1) {
      continue;  // currently support one-level map projection
    }
    const std::string base_col = parsed.variable() + "." + parsed.field_name();
    auto base = table->GetColumnByName(base_col);
    if (!base || base->type()->id() != arrow::Type::MAP) continue;

    ARROW_ASSIGN_OR_RAISE(
        auto arr, extract_map_values_for_key(base, parsed.nested_path()[0]));
    auto add_res =
        table->AddColumn(table->num_columns(), arrow::field(sf, arr->type()),
                         std::make_shared<arrow::ChunkedArray>(arr));
    if (!add_res.ok()) return add_res.status();
    table = add_res.ValueOrDie();
  }
  return table;
}
}  // namespace

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
         << " (via '" << conn.edge_type << "'";
      if (conn.edge_alias.has_value()) {
        ss << " as " << conn.edge_alias.value();
      }
      if (conn.edge_id >= 0) {
        ss << " #" << conn.edge_id;
      }
      ss << ")\n";
      conns_printed_for_target++;
    }
    target_nodes_printed++;
  }

  ss << "  Traversals (" << traversals.size() << "):\n";
  for (size_t i = 0; i < traversals.size(); ++i) {
    const auto& trav = traversals[i];
    ss << "    - [" << i << "]: " << trav.source().value() << " -["
       << (trav.edge_alias().has_value() ? trav.edge_alias().value() + ":" : "")
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
    if (traverse.edge_alias().has_value() &&
        processed_schemas.insert(traverse.edge_alias().value()).second) {
      unique_schemas.push_back(SchemaRef::parse(traverse.edge_alias().value()));
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
        continue;
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
        log_debug(
            "    [{}] {} -[{}{}{}]-> {}.{} (target_id: {})", i,
            conn.source.value(),
            conn.edge_alias.has_value() ? conn.edge_alias.value() + ":" : "",
            conn.edge_type,
            conn.edge_id >= 0 ? "#" + std::to_string(conn.edge_id) : "",
            conn.target.value(), conn.target.schema(), conn.target_id);
      }
    }
  }
}

std::shared_ptr<arrow::Table> apply_select(
    const std::shared_ptr<Select>& select,
    const std::shared_ptr<arrow::Table>& table) {
  if (!select || select->fields().empty()) return table;

  auto enriched_res = enrich_nested_select_fields(select, table);
  auto working_table = enriched_res.ok() ? enriched_res.ValueOrDie() : table;

  std::vector<std::string> all_columns;
  for (const auto& field : working_table->schema()->fields()) {
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
      working_table->SelectColumns(column_indices);

  if (!result.ok()) {
    return working_table;
  }

  return result.ValueOrDie();
}

/**
 * Collects WHERE expressions that can be inlined (pushed down) into a
 * single-table scan for @p target_var.
 *
 * Starting from clause index @p i, this scans forward through the clause
 * list and returns every WhereExpr whose predicate references only
 * @p target_var.  These predicates can be applied as early filters before
 * the traversal join, reducing the number of rows that enter the BFS.
 *
 * @param target_var  The variable to check inlinability against (e.g. "u").
 * @param i           Starting clause index in @p clauses.
 * @param clauses     The full clause list from the query.
 * @return A (possibly empty) vector of inlinable WHERE expressions.
 */
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

/**
 * Applies inlined WHERE predicates to a table, filtering rows early.
 *
 * Each expression in @p where_exprs is applied sequentially as an Arrow
 * compute filter on @p table.  After each successful filter the
 * QueryState's cached table is updated, and the expression is marked as
 * inlined so it won't be re-evaluated later during the main WHERE pass.
 *
 * @param ref          Schema reference identifying the table in QueryState.
 * @param table        The table to filter.
 * @param query_state  Mutable query state (table cache is updated).
 * @param where_exprs  Predicates to apply (from `get_where_to_inline`).
 * @return The filtered table, or an error status.
 */
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

/**
 * Prepares a Query for execution by resolving all symbolic references.
 *
 * This is the first step before any data is read.  It runs three phases
 * that progressively bind abstract query syntax to concrete schemas and
 * fields:
 *
 *  Phase 1 — FROM: registers the root schema alias (e.g. "u" -> "User").
 *
 *  Phase 2 — TRAVERSE: for each traversal clause:
 *    - Registers source / target node aliases.
 *    - If an edge alias is present (e.g. `[e:WORKS_AT]`), creates a
 *      shadow schema via `ensure_edge_shadow_schema` and registers the
 *      alias (e.g. "e" -> "__edge__WORKS_AT") so that edge fields can
 *      be resolved through the standard schema registry.
 *    - Records the edge alias -> raw edge type mapping separately in
 *      `query_state.edge_aliases` (used later by the traversal engine
 *      to look up edges in EdgeStore).
 *    - Computes BFS tags for source/target.
 *
 *  Phase 3 — WHERE: resolves every `FieldRef` inside `ComparisonExpr`
 *    nodes.  Each symbolic reference like "e.since" is looked up in the
 *    alias map ("e" -> "__edge__WORKS_AT"), then the field "since" is
 *    resolved from that schema's `Field` object and bound to the
 *    `FieldRef`.  After this phase, every `FieldRef::is_resolved()`
 *    returns true.
 *
 * @param query        The query to prepare (may be mutated: tags set on
 *                     traversal sources/targets).
 * @param query_state  Mutable query state accumulating aliases, traversals,
 *                     and the schema registry.
 * @return OK on success, or a KeyError / Invalid status if resolution fails.
 */
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
      if (traverse->edge_alias().has_value()) {
        ARROW_ASSIGN_OR_RAISE(
            auto edge_shadow_schema,
            ensure_edge_shadow_schema(query_state, traverse->edge_type()));
        ARROW_ASSIGN_OR_RAISE(
            auto _edge_schema_name,
            query_state.register_schema(SchemaRef::parse(
                traverse->edge_alias().value() + ":" + edge_shadow_schema)));
        (void)_edge_schema_name;
        ARROW_RETURN_NOT_OK(query_state.register_edge_alias(
            traverse->edge_alias().value(), traverse->edge_type()));
      }

      traverse->mutable_source().set_tag(compute_tag(traverse->source()));
      traverse->mutable_target().set_tag(compute_tag(traverse->target()));

      query_state.traversals.push_back(*traverse);
    }
  }

  // Phase 3: Resolve all ComparisonExpr field references.
  // query_state.aliases() already maps edge aliases to their shadow schema
  // names (e.g. "e" -> "__edge__WORKS_AT") from the register_schema call in
  // Phase 2, so no override is needed here.
  const auto& where_aliases = query_state.aliases();
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::WHERE) {
      auto where_expr = std::dynamic_pointer_cast<WhereExpr>(clause);
      auto res = where_expr->resolve_field_ref(
          where_aliases, query_state.schema_registry().get());
      if (!res.ok()) {
        return res.status();
      }
    }
  }

  return arrow::Status::OK();
}

}  // namespace tundradb
