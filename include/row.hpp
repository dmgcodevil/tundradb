#ifndef ROW_HPP
#define ROW_HPP

#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>

#include <memory>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "logger.hpp"
#include "node.hpp"
#include "query.hpp"
#include "types.hpp"

namespace tundradb {

/**
 * @brief A single segment of a BFS traversal path (schema + node ID).
 *
 * Carries an optional 16-bit tag for fast equality checks without
 * string comparison.
 */
struct PathSegment {
  uint16_t schema_tag;  ///< 16-bit hash of the schema name (0 = unset).
  std::string schema;   ///< Human-readable schema name.
  int64_t node_id;      ///< Node identifier within the schema.

  PathSegment(uint16_t tag, const std::string& schema_name, int64_t id)
      : schema_tag(tag), schema(schema_name), node_id(id) {}

  PathSegment(const std::string& schema_name, int64_t id)
      : schema_tag(0), schema(schema_name), node_id(id) {}

  /** @brief Returns "schema:node_id" for debugging. */
  std::string toString() const {
    return schema + ":" + std::to_string(node_id);
  }

  /** @brief Equality: compares tags when available, else falls back to string.
   */
  bool operator==(const PathSegment& other) const {
    if (schema_tag != 0 && other.schema_tag != 0) {
      return schema_tag == other.schema_tag && node_id == other.node_id;
    }
    return schema == other.schema && node_id == other.node_id;
  }
};

/**
 * @brief Checks whether @p prefix is a prefix of @p path.
 *
 * @param prefix The candidate prefix path.
 * @param path The full path to test against.
 * @return True if every element of @p prefix matches the corresponding element
 * of @p path.
 */
inline bool is_prefix(const std::vector<PathSegment>& prefix,
                      const std::vector<PathSegment>& path) {
  if (prefix.size() > path.size()) {
    return false;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (!(prefix[i] == path[i])) return false;
  }
  return true;
}

/**
 * @brief Joins path segments into an arrow-delimited string (e.g.
 * "users:0->companies:1").
 *
 * @param schema_path The segments to join.
 * @return The formatted path string.
 */
inline std::string join_schema_path(
    const std::vector<PathSegment>& schema_path) {
  std::ostringstream oss;
  for (size_t i = 0; i < schema_path.size(); ++i) {
    if (i != 0) oss << "->";
    oss << schema_path[i].toString();
  }
  return oss.str();
}

/**
 * @brief A single denormalised result row produced during BFS traversal.
 *
 * Stores cell values by flat field-ID index for O(1) access,
 * and tracks the BFS path that produced the row.
 */
struct Row {
  int64_t id;                     ///< Root node ID for this row.
  std::vector<ValueRef> cells;    ///< Cell values indexed by field ID.
  std::vector<PathSegment> path;  ///< BFS path that produced this row.
  std::unordered_map<std::string, int64_t>
      ids;                     ///< Lazily-populated schema→node-ID map.
  bool ids_populated = false;  ///< Whether @ref ids has been built.

  /** @brief Constructs an empty row with space for @p max_field_count fields.
   */
  explicit Row(size_t max_field_count = 64) : id(0), cells(max_field_count) {}

  /**
   * @brief Sets a cell value at the given field index.
   *
   * @param field_id The zero-based field index.
   * @param value_ref The value reference to store.
   */
  void set_cell(int field_id, ValueRef value_ref) {
    if (field_id >= 0 && field_id < static_cast<int>(cells.size())) {
      cells[field_id] = value_ref;
    }
  }

  /** @brief Returns true if the cell at @p field_id contains a non-null value.
   */
  [[nodiscard]] bool has_value(int field_id) const {
    return field_id >= 0 && field_id < static_cast<int>(cells.size()) &&
           cells[field_id].data != nullptr;
  }

  /**
   * @brief Populates cells from a node's fields using the given index mapping.
   *
   * @param field_indices Maps each node field position to a row cell index.
   * @param node The source node.
   * @param temporal_context Temporal snapshot for versioned reads (may be
   * nullptr).
   */
  void set_cell_from_node(const std::vector<int>& field_indices,
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

  /** @brief Returns true if this row's path starts with @p prefix. */
  [[nodiscard]] bool start_with(const std::vector<PathSegment>& prefix) const {
    return is_prefix(prefix, this->path);
  }

  /**
   * @brief Lazily extracts a schema-name→node-ID map from the "*.id" cells.
   *
   * @param field_id_to_name Mapping from field index to fully-qualified name.
   * @return A reference to the cached schema-ID map.
   */
  const std::unordered_map<std::string, int64_t>& extract_schema_ids(
      const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name) {
    if (ids_populated) {
      return ids;
    }
    for (size_t i = 0; i < cells.size(); ++i) {
      const auto& value = cells[i];
      if (!value.data) continue;
      const auto& field_name = field_id_to_name.at(static_cast<int>(i));
      size_t dot_pos = field_name.find('.');
      if (dot_pos != std::string::npos) {
        std::string schema = field_name.substr(0, dot_pos);
        if (field_name.substr(dot_pos + 1) == "id") {
          ids[schema] = value.as_int64();
        }
      }
    }
    return ids;
  }

  /**
   * @brief Merges another row into this one (non-destructive).
   *
   * Fields present in @p other but absent in this row are copied.
   * Existing values are kept.
   *
   * @param other The row to merge from.
   * @return A new Row combining both.
   */
  [[nodiscard]] std::shared_ptr<Row> merge(
      const std::shared_ptr<Row>& other) const {
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
          log_debug("Row::merge() - skipping field '{}' (already has value)",
                    i);
        }
      }
    }
    IF_DEBUG_ENABLED {
      log_debug("Row::merge() - result: {}", merged->ToString());
    }
    return merged;
  }

  /** @brief Returns a debug string listing the path and all cell values. */
  [[nodiscard]] std::string ToString() const {
    std::stringstream ss;
    ss << "Row{";
    ss << "path='" << join_schema_path(path) << "', ";

    bool first = true;
    for (size_t i = 0; i < cells.size(); i++) {
      if (!first) {
        ss << ", ";
      }
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
};

/**
 * @brief Creates a blank Row sized to fit the given output schema.
 *
 * @param final_output_schema The Arrow schema of the final query output.
 * @return A Row with id = −1 and all cells null.
 */
inline Row create_empty_row_from_schema(
    const std::shared_ptr<arrow::Schema>& final_output_schema) {
  Row new_row(final_output_schema->num_fields() + 32);
  new_row.id = -1;
  return new_row;
}

/**
 * @brief Collects rows whose path starts with @p parent's path (excluding @p
 * parent itself).
 *
 * @param parent The parent row.
 * @param rows All candidate rows.
 * @return The subset of @p rows that are children of @p parent.
 */
inline std::vector<Row> get_child_rows(const Row& parent,
                                       const std::vector<Row>& rows) {
  std::vector<Row> child;
  for (const auto& row : rows) {
    if (parent.id != row.id && row.start_with(parent.path)) {
      child.push_back(row);
    }
  }
  return child;
}

/**
 * @brief Tree node used to group and merge rows during BFS result assembly.
 *
 * Rows with shared path prefixes are placed in the same subtree.
 * The tree is then folded bottom-up via merge_rows() to produce the
 * Cartesian product of child branches (denormalisation).
 */
struct RowNode {
  std::optional<std::shared_ptr<Row>>
      row;                   ///< The row payload (set on leaf nodes).
  int depth;                 ///< Depth in the tree.
  PathSegment path_segment;  ///< Schema + node ID for this tree level.
  std::vector<std::unique_ptr<RowNode>> children;  ///< Child branches.

  RowNode() : depth(0), path_segment{"", -1} {}

  RowNode(std::optional<std::shared_ptr<Row>> r, int d,
          std::vector<std::unique_ptr<RowNode>> c = {})
      : row(std::move(r)),
        depth(d),
        path_segment{"", -1},
        children(std::move(c)) {}

  /** @brief Returns true if this node carries a row (i.e. is a leaf). */
  bool leaf() const { return row.has_value(); }

  /**
   * @brief Recursively inserts a row into the tree following its path segments.
   *
   * @param path_idx Current index into new_row->path.
   * @param new_row The row to insert.
   */
  void insert_row_dfs(size_t path_idx, const std::shared_ptr<Row>& new_row) {
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

  /** @brief Inserts a row starting from the root of its path. */
  void insert_row(const std::shared_ptr<Row>& new_row) {
    insert_row_dfs(0, new_row);
  }

  /**
   * @brief Recursively merges child rows via Cartesian product to produce
   * denormalised output.
   *
   * @param field_id_to_name Mapping used to populate schema-ID metadata on
   * merged rows.
   * @return A vector of fully merged rows.
   */
  std::vector<std::shared_ptr<Row>> merge_rows(
      const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name);

  /** @brief Returns a human-readable tree representation for debugging. */
  std::string toString(bool recursive = true, int indent_level = 0) const;

  friend std::ostream& operator<<(std::ostream& os, const RowNode& node) {
    return os << node.toString();
  }

  /** @brief Logs the tree via log_debug. */
  void print(const bool recursive = true) const {
    log_debug(toString(recursive));
  }
};

/**
 * @brief An item in the BFS queue used by populate_rows_bfs().
 *
 * Carries the current node, its traversal level, the partial row
 * accumulated so far, and a per-path visited set to detect cycles.
 */
struct QueueItem {
  int64_t node_id;           ///< Current node being visited.
  SchemaRef schema_ref;      ///< Schema of the current node.
  int level;                 ///< Traversal depth (0 = root).
  std::shared_ptr<Row> row;  ///< Partial row built along the path.
  llvm::SmallDenseSet<uint64_t, 8>
      path_visited_nodes;         ///< Packed hashes of nodes on this path.
  std::vector<PathSegment> path;  ///< Full BFS path from root.

  /**
   * @brief Constructs a queue item and records the starting node in the path.
   *
   * @param id The node ID.
   * @param schema The schema reference.
   * @param l The traversal level.
   * @param r The partial row accumulated so far.
   */
  QueueItem(int64_t id, const SchemaRef& schema, int l, std::shared_ptr<Row> r)
      : node_id(id), schema_ref(schema), level(l), row(std::move(r)) {
    path.emplace_back(schema.tag(), schema.value(), id);
  }
};

}  // namespace tundradb

#endif  // ROW_HPP
