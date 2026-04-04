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

#include "common/constants.hpp"
#include "common/logger.hpp"
#include "common/types.hpp"
#include "core/edge.hpp"
#include "core/node.hpp"
#include "query/query.hpp"

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

/// Checks whether @p prefix is a prefix of @p path.
bool is_prefix(const std::vector<PathSegment>& prefix,
               const std::vector<PathSegment>& path);

/// Joins path segments into an arrow-delimited string.
std::string join_schema_path(const std::vector<PathSegment>& schema_path);

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
      ids;                     ///< Lazily-populated schema->node-ID map.
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

  /// Populates cells from a node's fields using the given index mapping.
  void set_cell_from_node(const std::vector<int>& field_indices,
                          const std::shared_ptr<Node>& node,
                          TemporalContext* temporal_context);

  /// Populates row cells from an edge projection.
  void set_cell_from_edge(
      const std::vector<int>& field_indices, const std::shared_ptr<Edge>& edge,
      const llvm::SmallVector<std::shared_ptr<Field>, 4>& fields,
      TemporalContext* temporal_context);

  /** @brief Returns true if this row's path starts with @p prefix. */
  [[nodiscard]] bool start_with(const std::vector<PathSegment>& prefix) const {
    return is_prefix(prefix, this->path);
  }

  /// Lazily extracts a schema-name->node-ID map from the "*.id" cells.
  const std::unordered_map<std::string, int64_t>& extract_schema_ids(
      const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name);

  /// Merges another row into this one (non-destructive).
  [[nodiscard]] std::shared_ptr<Row> merge(
      const std::shared_ptr<Row>& other) const;

  /// Returns a debug string listing the path and all cell values.
  [[nodiscard]] std::string ToString() const;
};

/// Creates a blank Row sized to fit the given output schema.
Row create_empty_row_from_schema(
    const std::shared_ptr<arrow::Schema>& final_output_schema);

/// Collects rows whose path starts with @p parent's path (excluding parent).
std::vector<Row> get_child_rows(const Row& parent,
                                const std::vector<Row>& rows);

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

  /// Recursively inserts a row into the tree following its path segments.
  void insert_row_dfs(size_t path_idx, const std::shared_ptr<Row>& new_row);

  /// Inserts a row starting from the root of its path.
  void insert_row(const std::shared_ptr<Row>& new_row);

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
