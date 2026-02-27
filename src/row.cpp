#include "row.hpp"

#include "logger.hpp"

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

}  // namespace tundradb
