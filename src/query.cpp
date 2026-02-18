#include "query.hpp"

#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>

#include "arrow_utils.hpp"
#include "logger.hpp"

namespace tundradb {

// QueryState method implementations

void QueryState::reserve_capacity(const Query& query) {
  // Estimate schema count from FROM + TRAVERSE clauses
  size_t estimated_schemas = 1;  // FROM clause
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::TRAVERSE) {
      estimated_schemas += 2;  // source + target schemas
    }
  }

  // Pre-size standard containers (LLVM containers don't support reserve)
  tables.reserve(estimated_schemas);
  aliases.reserve(estimated_schemas);

  // Estimate nodes per schema (conservative estimate)
  size_t estimated_nodes_per_schema = 1000;
  incoming.reserve(estimated_nodes_per_schema);

  // Pre-size field mappings
  field_id_to_name.reserve(estimated_schemas * 8);  // ~8 fields per schema
}

arrow::Result<std::string> QueryState::register_schema(
    const SchemaRef& schema_ref) {
  if (aliases.contains(schema_ref.value()) && schema_ref.is_declaration()) {
    IF_DEBUG_ENABLED {
      log_debug("duplicated schema alias '{}' already assigned to '{}'",
                schema_ref.value(), aliases.at(schema_ref.value()));
    }
    return aliases[schema_ref.value()];
  }
  if (schema_ref.is_declaration()) {
    aliases[schema_ref.value()] = schema_ref.schema();
    return schema_ref.schema();
  }
  return aliases[schema_ref.value()];
}

arrow::Result<std::string> QueryState::resolve_schema(
    const SchemaRef& schema_ref) const {
  if (schema_ref.is_declaration()) {
    return schema_ref.schema();
  }

  if (!aliases.contains(schema_ref.value())) {
    return arrow::Status::KeyError("no alias for '{}'", schema_ref.value());
  }
  return aliases.at(schema_ref.value());
}

arrow::Result<bool> QueryState::compute_fully_qualified_names(
    const SchemaRef& schema_ref) {
  const auto it = aliases.find(schema_ref.value());
  if (it == aliases.end()) {
    return arrow::Status::KeyError("keyset does not contain alias '{}'",
                                   schema_ref.value());
  }
  return compute_fully_qualified_names(schema_ref, it->second);
}

arrow::Result<bool> QueryState::compute_fully_qualified_names(
    const SchemaRef& schema_ref, const std::string& resolved_schema) {
  const std::string& alias = schema_ref.value();
  if (fq_field_names.contains(alias)) {
    return false;
  }
  auto schema_res = schema_registry->get(resolved_schema);
  if (!schema_res.ok()) {
    return schema_res.status();
  }
  const auto& schema = schema_res.ValueOrDie();
  std::vector<std::string> names;
  std::vector<int> indices;
  names.reserve(schema->num_fields());
  indices.reserve(schema->num_fields());

  for (const auto& f : schema->fields()) {
    std::string fq_name = alias + "." + f->name();
    int field_id = next_field_id.fetch_add(1);
    names.emplace_back(fq_name);
    indices.emplace_back(field_id);
    field_id_to_name[field_id] = fq_name;
    field_name_to_index[fq_name] = field_id;
  }

  fq_field_names[alias] = std::move(names);
  schema_field_indices[alias] = std::move(indices);
  return true;
}

void QueryState::remove_node(int64_t node_id, const SchemaRef& schema_ref) {
  ids[schema_ref.value()].erase(node_id);
}

arrow::Result<bool> QueryState::update_table(
    const std::shared_ptr<arrow::Table>& table, const SchemaRef& schema_ref) {
  this->tables[schema_ref.value()] = table;
  auto ids_result = get_ids_from_table(table);
  if (!ids_result.ok()) {
    log_error("Failed to get IDs from table: {}", schema_ref.value());
    return ids_result.status();
  }
  ids[schema_ref.value()] = ids_result.ValueOrDie();
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

  ss << "  IDs (" << ids.size() << "):\n";
  for (const auto& [alias, id_set] : ids) {
    ss << "    - " << alias.str() << ": " << id_set.size() << " IDs\n";
  }

  ss << "  Aliases (" << aliases.size() << "):\n";
  for (const auto& [alias, schema_name] : aliases) {
    ss << "    - " << alias << " -> " << schema_name << "\n";
  }

  ss << "  Connections (Outgoing) (" << connections.size() << " source nodes):";
  for (const auto& [from, conns] : connections) {
    for (const auto& [from_id, conn_vec] : conns) {
      ss << "from " << from.str() << ":" << from_id << ":\n";
      for (const auto& conn : conn_vec) {
        ss << "    - " << conn.target.value() << ":" << conn.target_id << "\n";
      }
    }
  }

  ss << "  Connections (Incoming) (" << incoming.size() << " target nodes):";
  int target_nodes_printed = 0;
  for (const auto& [target_id, conns_vec] : incoming) {
    if (target_nodes_printed >= 3 && incoming.size() > 5) {
      ss << "      ... and " << (incoming.size() - target_nodes_printed)
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

// FieldRef implementation
FieldRef FieldRef::from_string(const std::string& field_str) {
  const size_t dot_pos = field_str.find('.');
  if (dot_pos != std::string::npos) {
    std::string variable = field_str.substr(0, dot_pos);
    std::string field_name = field_str.substr(dot_pos + 1);

    // Return unresolved FieldRef - will be resolved later in query processing
    return {variable, field_name};
  } else {
    // No variable prefix, treat entire string as field name
    return {"", field_str};
  }
}

// Convert CompareOp to appropriate Arrow compute function
arrow::compute::Expression apply_comparison_op(
    const arrow::compute::Expression& field,
    const arrow::compute::Expression& value, CompareOp op) {
  switch (op) {
    case CompareOp::Eq:
      return arrow::compute::equal(field, value);
    case CompareOp::NotEq:
      return arrow::compute::not_equal(field, value);
    case CompareOp::Gt:
      return arrow::compute::greater(field, value);
    case CompareOp::Lt:
      return arrow::compute::less(field, value);
    case CompareOp::Gte:
      return arrow::compute::greater_equal(field, value);
    case CompareOp::Lte:
      return arrow::compute::less_equal(field, value);
    case CompareOp::Contains:
      // For string operations, we'd need to use match_substring_regex or
      // similar For now, fall back to equal (this would need more sophisticated
      // handling)
      log_warn(
          "CONTAINS operator not fully implemented for Arrow expressions, "
          "using equality");
      return arrow::compute::equal(field, value);
    case CompareOp::StartsWith:
      log_warn(
          "STARTS_WITH operator not fully implemented for Arrow expressions, "
          "using equality");
      return arrow::compute::equal(field, value);
    case CompareOp::EndsWith:
      log_warn(
          "ENDS_WITH operator not fully implemented for Arrow expressions, "
          "using equality");
      return arrow::compute::equal(field, value);
    default:
      throw std::runtime_error(
          "Unsupported comparison operator for Arrow expression");
  }
}

// Convert Value to Arrow compute scalar for expressions
arrow::compute::Expression value_to_expression(const Value& value) {
  switch (value.type()) {
    case ValueType::INT32:
      return arrow::compute::literal(value.get<int32_t>());
    case ValueType::INT64:
      return arrow::compute::literal(value.get<int64_t>());
    case ValueType::STRING:
      return arrow::compute::literal(value.get<std::string>());
    case ValueType::FLOAT:
      return arrow::compute::literal(value.get<float>());
    case ValueType::DOUBLE:
      return arrow::compute::literal(value.get<double>());
    case ValueType::BOOL:
      return arrow::compute::literal(value.get<bool>());
    case ValueType::NA:
      return arrow::compute::literal(
          arrow::Datum(arrow::MakeNullScalar(arrow::null())));
    default:
      throw std::runtime_error("Unsupported value type for Arrow expression");
  }
}

}  // namespace tundradb