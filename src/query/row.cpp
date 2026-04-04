#include "query/row.hpp"

#include "common/logger.hpp"

namespace tundradb {

std::vector<std::shared_ptr<Row>> RowNode::merge_rows(
    const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name) {
  if (this->leaf()) {
    return {this->row.value()};
  }

  // Collect all records from child nodes and group them by schema
  llvm::SmallDenseMap<uint16_t, std::vector<std::shared_ptr<Row>>, 8> grouped;
  for (const auto& c : children) {
    auto child_rows = c->merge_rows(field_id_to_name);
    IF_DEBUG_ENABLED {
      log_debug("Child node {} returned {} rows", c->path_segment.toString(),
                child_rows.size());
      for (size_t i = 0; i < child_rows.size(); ++i) {
        log_debug("  Child row [{}]: {}", i, child_rows[i]->ToString());
      }
    }
    uint16_t tag = c->path_segment.schema_tag;
    if (tag == 0) {
      tag = static_cast<uint16_t>(
          std::hash<std::string>{}(c->path_segment.schema) & 0xFFFFu);
    }
    grouped[tag].insert(grouped[tag].end(), child_rows.begin(),
                        child_rows.end());
  }

  std::vector<std::vector<std::shared_ptr<Row>>> groups_for_product;

  if (this->row.has_value()) {
    std::shared_ptr<Row> node_self_row =
        std::make_shared<Row>(*this->row.value());
    node_self_row->path = {this->path_segment};
    IF_DEBUG_ENABLED {
      log_debug("Adding node self row: {}", node_self_row->ToString());
    }
    groups_for_product.push_back({node_self_row});
  }

  for (const auto& pair : grouped) {
    if (!pair.second.empty()) {
      IF_DEBUG_ENABLED {
        log_debug("Adding group for schema '{}' with {} rows", pair.first,
                  pair.second.size());
        for (size_t i = 0; i < pair.second.size(); ++i) {
          log_debug("  Group row [{}]: {}", i, pair.second[i]->ToString());
        }
      }
      groups_for_product.push_back(pair.second);
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Total groups for Cartesian product: {}",
              groups_for_product.size());
  }

  if (groups_for_product.empty()) {
    return {};
  }

  if (groups_for_product.size() == 1) {
    return groups_for_product[0];
  }

  std::vector<std::shared_ptr<Row>> final_merged_rows =
      groups_for_product.back();
  IF_DEBUG_ENABLED {
    log_debug("Starting Cartesian product with final group ({} rows)",
              final_merged_rows.size());
    for (size_t i = 0; i < final_merged_rows.size(); ++i) {
      log_debug("  Final group row [{}]: {}", i,
                final_merged_rows[i]->ToString());
    }
  }

  for (int i = static_cast<int>(groups_for_product.size()) - 2; i >= 0; --i) {
    IF_DEBUG_ENABLED {
      log_debug("Processing group {} with {} rows", i,
                groups_for_product[i].size());
      for (size_t j = 0; j < groups_for_product[i].size(); ++j) {
        log_debug("  Current group row [{}]: {}", j,
                  groups_for_product[i][j]->ToString());
      }
    }

    std::vector<std::shared_ptr<Row>> temp_product_accumulator;
    for (const auto& r1_from_current_group : groups_for_product[i]) {
      for (const auto& r2_from_previous_product : final_merged_rows) {
        bool can_merge = true;

        std::unordered_map<std::string, int64_t> schema_ids_r1 =
            r1_from_current_group->extract_schema_ids(field_id_to_name);
        std::unordered_map<std::string, int64_t> schema_ids_r2 =
            r2_from_previous_product->extract_schema_ids(field_id_to_name);

        for (const auto& [schema, id1] : schema_ids_r1) {
          if (schema_ids_r2.contains(schema) && schema_ids_r2[schema] != id1) {
            IF_DEBUG_ENABLED {
              log_debug(
                  "Conflict detected: Schema '{}' has different IDs: {} vs {}",
                  schema, id1, schema_ids_r2[schema]);
            }
            can_merge = false;
            break;
          }
        }

        if (can_merge) {
          for (size_t field_index = 0;
               field_index < r1_from_current_group->cells.size();
               ++field_index) {
            if (r1_from_current_group->has_value(static_cast<int>(i)) &&
                r2_from_previous_product->has_value(static_cast<int>(i))) {
              if (!r1_from_current_group->cells[i].equals(
                      r2_from_previous_product->cells[i])) {
                IF_DEBUG_ENABLED {
                  log_debug(
                      "Conflict detected: Field '{}' has different values",
                      field_id_to_name.at(i));
                }
                can_merge = false;
                break;
              }
            }
          }
        }

        if (can_merge) {
          std::shared_ptr<Row> merged_r =
              r1_from_current_group->merge(r2_from_previous_product);
          merged_r->path = {this->path_segment};
          IF_DEBUG_ENABLED {
            log_debug("Merged row: {}", merged_r->ToString());
          }
          temp_product_accumulator.push_back(merged_r);
        } else {
          IF_DEBUG_ENABLED { log_debug("Cannot merge rows due to conflicts"); }
        }
      }
    }
    final_merged_rows = std::move(temp_product_accumulator);
    if (final_merged_rows.empty()) {
      IF_DEBUG_ENABLED {
        log_debug("product_accumulator is empty. stop merge");
      }
      break;
    }
  }
  return final_merged_rows;
}

std::string RowNode::toString(bool recursive, int indent_level) const {
  auto get_indent = [](int level) { return std::string(level * 2, ' '); };

  std::stringstream ss;
  std::string indent = get_indent(indent_level);

  ss << indent << "RowNode [path=" << path_segment.toString()
     << ", depth=" << depth << "] {\n";

  if (row.has_value()) {
    ss << indent << "  Path: ";
    if (row.value()->path.empty()) {
      ss << "(empty)";
    } else {
      for (size_t i = 0; i < row.value()->path.size(); ++i) {
        if (i > 0) ss << " -> ";
        ss << row.value()->path[i].schema << ":"
           << row.value()->path[i].node_id;
      }
    }
    ss << "\n";

    ss << indent << "  Cells: ";
    if (row.value()->cells.empty()) {
      ss << "(empty)";
    } else {
      size_t count = 0;
      ss << "{ ";
      for (size_t i = 0; i < row.value()->cells.size(); i++) {
        if (count++ > 0) ss << ", ";
        if (count > 5) {
          ss << "... +" << (row.value()->cells.size() - 5) << " more";
          break;
        }

        ss << i << ": ";
        if (!row.value()->has_value(static_cast<int>(i))) {
          ss << "NULL";
        } else {
          ss << row.value()->cells[i].ToString();
        }
      }
      ss << " }";
    }
  }

  ss << "\n";

  ss << indent << "  Children: " << children.size() << "\n";

  if (recursive && !children.empty()) {
    ss << indent << "  [\n";
    for (const auto& child : children) {
      if (child) {
        ss << child->toString(true, indent_level + 2);
      } else {
        ss << get_indent(indent_level + 2) << "(null child)\n";
      }
    }
    ss << indent << "  ]\n";
  }

  ss << indent << "}\n";
  return ss.str();
}

// ---------------------------------------------------------------------------
// Standalone functions moved from row.hpp
// ---------------------------------------------------------------------------

bool is_prefix(const std::vector<PathSegment>& prefix,
               const std::vector<PathSegment>& path) {
  if (prefix.size() > path.size()) return false;
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (!(prefix[i] == path[i])) return false;
  }
  return true;
}

std::string join_schema_path(const std::vector<PathSegment>& schema_path) {
  std::ostringstream oss;
  for (size_t i = 0; i < schema_path.size(); ++i) {
    if (i != 0) oss << "->";
    oss << schema_path[i].toString();
  }
  return oss.str();
}

Row create_empty_row_from_schema(
    const std::shared_ptr<arrow::Schema>& final_output_schema) {
  Row new_row(final_output_schema->num_fields() + 32);
  new_row.id = -1;
  return new_row;
}

std::vector<Row> get_child_rows(const Row& parent,
                                const std::vector<Row>& rows) {
  std::vector<Row> child;
  for (const auto& row : rows) {
    if (parent.id != row.id && row.start_with(parent.path)) {
      child.push_back(row);
    }
  }
  return child;
}

// ---------------------------------------------------------------------------
// Row methods moved from row.hpp
// ---------------------------------------------------------------------------

void Row::set_cell_from_node(const std::vector<int>& field_indices,
                             const std::shared_ptr<Node>& node,
                             TemporalContext* temporal_context) {
  auto view = node->view(temporal_context);
  const auto& fields = node->get_schema()->fields();
  const size_t n = std::min(fields.size(), field_indices.size());
  for (size_t i = 0; i < n; ++i) {
    const auto& field = fields[i];
    const int field_id = field_indices[i];
    auto value_ref_result = view.get_value_ref(field);
    if (value_ref_result.ok()) {
      this->set_cell(field_id, value_ref_result.ValueOrDie());
    }
  }
}

void Row::set_cell_from_edge(
    const std::vector<int>& field_indices, const std::shared_ptr<Edge>& edge,
    const llvm::SmallVector<std::shared_ptr<Field>, 4>& fields,
    TemporalContext* temporal_context) {
  auto view = edge->view(temporal_context);
  const auto edge_schema = edge->get_schema();
  const size_t n = std::min(fields.size(), field_indices.size());
  for (size_t i = 0; i < n; ++i) {
    auto field = fields[i];
    if (!field) continue;
    const auto& name = field->name();
    const bool structural =
        (name == field_names::kId || name == field_names::kEdgeId ||
         name == field_names::kSourceId || name == field_names::kTargetId ||
         name == field_names::kCreatedTs);
    if (!structural && edge_schema) {
      auto real_field = edge_schema->get_field(name);
      if (!real_field) continue;
      field = real_field;
    }
    const int field_id = field_indices[i];
    auto value_ref_result = view.get_value_ref(field);
    if (value_ref_result.ok()) {
      this->set_cell(field_id, value_ref_result.ValueOrDie());
    }
  }
}

const std::unordered_map<std::string, int64_t>& Row::extract_schema_ids(
    const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name) {
  if (ids_populated) return ids;
  for (size_t i = 0; i < cells.size(); ++i) {
    const auto& value = cells[i];
    if (!value.data) continue;
    const auto& field_name = field_id_to_name.at(static_cast<int>(i));
    size_t dot_pos = field_name.find('.');
    if (dot_pos != std::string::npos) {
      std::string schema = field_name.substr(0, dot_pos);
      if (field_name.substr(dot_pos + 1) == field_names::kId) {
        ids[schema] = value.as_int64();
      }
    }
  }
  return ids;
}

std::shared_ptr<Row> Row::merge(const std::shared_ptr<Row>& other) const {
  std::shared_ptr<Row> merged = std::make_shared<Row>(*this);
  IF_DEBUG_ENABLED {
    log_debug("Row::merge() - this: {}", this->ToString());
    log_debug("Row::merge() - other: {}", other->ToString());
  }
  for (size_t i = 0; i < other->cells.size(); ++i) {
    if (!merged->has_value(static_cast<int>(i))) {
      IF_DEBUG_ENABLED {
        log_debug("Row::merge() - adding field '{}' with value: {}", i,
                  cells[i].ToString());
      }
      merged->cells[i] = other->cells[i];
    } else {
      IF_DEBUG_ENABLED {
        log_debug("Row::merge() - skipping field '{}' (already has value)", i);
      }
    }
  }
  IF_DEBUG_ENABLED {
    log_debug("Row::merge() - result: {}", merged->ToString());
  }
  return merged;
}

std::string Row::ToString() const {
  std::stringstream ss;
  ss << "Row{";
  ss << "path='" << join_schema_path(path) << "', ";
  bool first = true;
  for (size_t i = 0; i < cells.size(); i++) {
    if (!first) ss << ", ";
    first = false;
    ss << i << ": ";
    const auto value_ref = cells[i];
    if (!value_ref.data) {
      ss << "NULL";
    } else {
      switch (value_ref.type) {
        case ValueType::INT64:
          ss << value_ref.as_int64();
          break;
        case ValueType::INT32:
          ss << value_ref.as_int32();
          break;
        case ValueType::DOUBLE:
          ss << value_ref.as_double();
          break;
        case ValueType::STRING:
          ss << "\"" << value_ref.as_string_ref().to_string() << "\"";
          break;
        case ValueType::BOOL:
          ss << (value_ref.as_bool() ? "true" : "false");
          break;
        default:
          ss << "unknown";
          break;
      }
    }
  }
  ss << "}";
  return ss.str();
}

// ---------------------------------------------------------------------------
// RowNode methods moved from row.hpp
// ---------------------------------------------------------------------------

void RowNode::insert_row_dfs(size_t path_idx,
                             const std::shared_ptr<Row>& new_row) {
  if (path_idx == new_row->path.size()) {
    this->row = new_row;
    return;
  }
  for (const auto& n : children) {
    if (n->path_segment == new_row->path[path_idx]) {
      n->insert_row_dfs(path_idx + 1, new_row);
      return;
    }
  }
  auto new_node = std::make_unique<RowNode>();
  new_node->depth = depth + 1;
  new_node->path_segment = new_row->path[path_idx];
  new_node->insert_row_dfs(path_idx + 1, new_row);
  children.emplace_back(std::move(new_node));
}

void RowNode::insert_row(const std::shared_ptr<Row>& new_row) {
  insert_row_dfs(0, new_row);
}

}  // namespace tundradb
