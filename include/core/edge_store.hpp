#ifndef EDGE_STORE_HPP
#define EDGE_STORE_HPP

#include <arrow/api.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/StringMap.h>

#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/concurrency.hpp"
#include "common/utils.hpp"
#include "core/edge.hpp"
#include "memory/node_arena.hpp"
#include "memory/schema_layout.hpp"
#include "schema/schema.hpp"

namespace tundradb {

/// Central registry and index for all edges in the database.
///
/// Maintains directional adjacency indexes (outgoing/incoming), a
/// type-based grouping, and an optional per-type Arrow Table cache for
/// query materialisation.  Edge schemas may be registered to add
/// arena-backed property fields to edges of a given type.
class EdgeStore {
  struct TableCache;

 private:
  mutable std::shared_mutex edges_mutex_;
  llvm::DenseMap<int64_t, std::shared_ptr<Edge>> edges;
  llvm::StringMap<std::unordered_set<int64_t>> edges_by_type_;
  llvm::DenseMap<int64_t, std::unordered_set<int64_t>> outgoing_edges_;
  llvm::DenseMap<int64_t, std::unordered_set<int64_t>> incoming_edges_;

  llvm::StringMap<std::atomic<int64_t>> versions_;  // version
  std::atomic<int64_t> edge_id_counter_{0};

  ConcurrentSet<int64_t> edge_ids_;

  mutable std::shared_mutex tables_mutex_;
  llvm::StringMap<std::shared_ptr<TableCache>> tables_;  // cache
  std::string data_file_;
  int64_t chunk_size_;

  // Edge schema registries
  std::unordered_map<std::string, std::shared_ptr<Schema>> edge_schemas_;
  std::shared_ptr<LayoutRegistry> edge_layout_registry_;
  std::shared_ptr<NodeArena> edge_arena_;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_edges_from_map(
      const llvm::DenseMap<int64_t, std::unordered_set<int64_t>> &edge_map,
      int64_t id, const std::string &type) const;

  arrow::Result<std::shared_ptr<arrow::Table>> generate_table(
      const std::string &edge_type) const;

  arrow::Result<int64_t> get_version_snapshot(
      const std::string &edge_type) const;

  struct TableCache {
    std::shared_ptr<arrow::Table> table;
    std::atomic<int64_t> version{0};
    std::mutex lock;
  };

 public:
  explicit EdgeStore(const int64_t init_edge_id_counter,
                     const int64_t chunk_size = 1000)
      : edge_id_counter_(init_edge_id_counter),
        chunk_size_(chunk_size),
        edge_layout_registry_(std::make_shared<LayoutRegistry>()),
        edge_arena_(
            node_arena_factory::create_free_list_arena(edge_layout_registry_)) {
  }

  ~EdgeStore() {
    edges.clear();
    edges_by_type_.clear();
    outgoing_edges_.clear();
    incoming_edges_.clear();
    versions_.clear();
    edge_ids_.clear();
    tables_.clear();
  }

  /// Current value of the edge ID sequence.
  int64_t get_edge_id_counter() const { return edge_id_counter_; }

  /// Register a typed property schema for edges of @p edge_type.
  arrow::Result<bool> register_edge_schema(
      const std::string &edge_type,
      const std::vector<std::shared_ptr<Field>> &fields);

  /// True when a property schema has been registered for this edge type.
  [[nodiscard]] bool has_edge_schema(const std::string &edge_type) const;

  /// Return the Schema for a registered edge type (nullptr if none).
  [[nodiscard]] std::shared_ptr<Schema> get_edge_schema(
      const std::string &edge_type) const;

  /// Return the memory layout for a registered edge type.
  [[nodiscard]] std::shared_ptr<SchemaLayout> get_edge_layout(
      const std::string &edge_type) const;

  /**
   * Creates a new edge at runtime (user API / `connect`).
   *
   * - Assigns a fresh edge id from the store counter.
   * - Sets `created_ts` to the current time.
   * - If an edge schema is registered for `type`, allocates arena-backed
   *   storage and writes `properties` into v0; otherwise the edge is
   *   structural-only and `properties` must be empty.
   *
   * The returned edge is not in the store until you call `add()`.
   */
  arrow::Result<std::shared_ptr<Edge>> create_edge(
      int64_t source_id, const std::string &type, int64_t target_id,
      std::unordered_map<std::string, Value> properties = {});

  /**
   * Reconstructs an edge from persistence (snapshot / Parquet reload).
   *
   * Use this when you already have the persisted identity and timestamps:
   * - `id`, `source_id`, `target_id`, `created_ts` come from the file.
   * - `properties` is the decoded row payload (same shape as `create_edge`).
   *
   * Differences from `create_edge`:
   * - Does not allocate a new id; uses the given `id`.
   * - Does not stamp `created_ts`; uses the given value.
   * - Bumps the edge id counter if needed so future `create_edge` ids stay
   *   strictly above restored ids (avoids collisions after reload).
   *
   * The returned edge is not in the store until you call `add()` (same as
   * `create_edge`).
   */
  arrow::Result<std::shared_ptr<Edge>> restore_edge(
      int64_t id, int64_t source_id, const std::string &type, int64_t target_id,
      int64_t created_ts, std::unordered_map<std::string, Value> properties);

  /** Inserts an edge produced by `create_edge` or `restore_edge` into indexes.
   */
  arrow::Result<bool> add(const std::shared_ptr<Edge> &edge);

  /// Remove an edge from all indexes.
  arrow::Result<bool> remove(int64_t edge_id);

  /// Look up a single edge by ID.
  arrow::Result<std::shared_ptr<Edge>> get(int64_t edge_id) const;

  /// Bulk-fetch edges by a set of IDs.
  std::vector<std::shared_ptr<Edge>> get(const std::set<int64_t> &ids) const;

  /// Bulk-fetch edges from any iterable container of IDs.
  template <typename Container>
  std::vector<std::shared_ptr<Edge>> get(const Container &ids) const;

  /// Return the number of edges for a given type (0 if type does not exist).
  int64_t get_count_by_type(const std::string &type) const {
    if (auto res = get_by_type(type); res.ok()) {
      return res.ValueOrDie().size();
    }
    return 0;
  }

  /// Return all outgoing edges from a node, optionally filtered by type.
  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_outgoing_edges(
      int64_t id, const std::string &type = "") const;

  /// Return all incoming edges to a node, optionally filtered by type.
  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_incoming_edges(
      int64_t id, const std::string &type = "") const;

  /// Return all edges of a given type.
  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_by_type(
      const std::string &type) const;

  /// Materialise edges of a type into a cached Arrow Table.
  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &edge_type);

  /// Return the current mutation version counter for an edge type.
  arrow::Result<int64_t> get_version(const std::string &edge_type) const;

  /// Return the set of all registered edge type names.
  std::set<std::string> get_edge_types() const;

  int64_t get_chunk_size() const { return chunk_size_; }
  size_t size() const { return edges.size(); }
  bool empty() const { return edges.empty(); }

  /// Override the edge ID sequence (used during snapshot restore).
  void set_id_seq(const int64_t v) {
    edge_id_counter_.store(v, std::memory_order_relaxed);
  }
};
}  // namespace tundradb

#endif  // EDGE_STORE_HPP
