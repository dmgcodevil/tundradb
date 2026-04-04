#pragma once

#include <arrow/result.h>
#include <arrow/table.h>

#include <atomic>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.hpp"
#include "core/field_update.hpp"
#include "core/node.hpp"
#include "schema/schema.hpp"

namespace tundradb {

class TemporalContext;

/// A fixed-capacity bucket of nodes for a single schema, keyed by node ID
/// range [min_id, max_id].
///
/// Nodes are stored in a PMR monotonic-buffer-backed map for cache-friendly
/// allocation.  A shard tracks its dirty flag for incremental persistence
/// and can materialise its contents into an Arrow Table.
class Shard {
 private:
  std::pmr::monotonic_buffer_resource memory_pool_;
  std::pmr::unordered_map<int64_t, std::shared_ptr<Node>> nodes_;
  std::set<int64_t> nodes_ids_;
  std::atomic<bool> dirty_{false};
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  int64_t updated_ts_;
  bool updated_ = true;

 public:
  const int64_t id;
  const int64_t index;
  int64_t min_id;
  int64_t max_id;
  const size_t capacity;
  const size_t chunk_size;
  std::string schema_name;

  Shard(int64_t id, int64_t index, size_t capacity, int64_t min_id,
        int64_t max_id, size_t chunk_size, std::string schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry,
        size_t buffer_size = 10 * 1024 * 1024);

  Shard(int64_t id, int64_t index, const DatabaseConfig &config, int64_t min_id,
        int64_t max_id, std::string schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry);

  ~Shard();

  /// True when the shard has been modified since the last snapshot.
  [[nodiscard]] bool is_updated() const;
  /// Set or clear the dirty flag; returns the previous value.
  bool set_updated(bool v);
  /// Timestamp of the last modification.
  [[nodiscard]] int64_t get_updated_ts() const;
  /// Return "schema_name:shard_id" (used in logging and metadata keys).
  [[nodiscard]] std::string compound_id() const;

  /// Insert a node whose ID falls within [min_id, max_id].
  arrow::Result<bool> add(const std::shared_ptr<Node> &node);
  /// Insert a node and widen max_id if necessary.
  arrow::Result<bool> extend(const std::shared_ptr<Node> &node);
  /// Remove and return a node by ID.
  arrow::Result<std::shared_ptr<Node>> remove(int64_t id);
  /// Remove and return the node with the smallest ID.
  arrow::Result<std::shared_ptr<Node>> poll_first();

  /// Update a single field on a node inside this shard.
  arrow::Result<bool> update(int64_t node_id, std::shared_ptr<Field> field,
                             const Value &value, UpdateType update_type);

  /// Batch-update multiple fields on a node inside this shard.
  arrow::Result<bool> update_fields(
      int64_t node_id, const std::vector<FieldUpdate> &field_updates,
      UpdateType update_type);

  /// Materialise shard contents into an Arrow Table.
  arrow::Result<std::shared_ptr<arrow::Table>> get_table(TemporalContext *ctx);

  [[nodiscard]] size_t size() const;
  /// True when the shard has room for more nodes (size < capacity).
  [[nodiscard]] bool has_space() const;
  [[nodiscard]] bool empty() const;
  /// Return a snapshot of all nodes in insertion order.
  [[nodiscard]] std::vector<std::shared_ptr<Node>> get_nodes() const;
};

/// Manages per-schema collections of Shards.
///
/// When a node is inserted, the manager finds an existing shard with
/// capacity or creates a new one.  Compaction merges under-filled shards
/// to reduce fragmentation.
class ShardManager {
 private:
  std::pmr::monotonic_buffer_resource memory_pool_;
  std::pmr::unordered_map<std::string, std::vector<std::shared_ptr<Shard>>>
      shards_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  const size_t shard_capacity_;
  const size_t chunk_size_;
  const DatabaseConfig config_;
  std::atomic<int64_t> id_counter_{0};
  std::unordered_map<std::string, std::atomic<int64_t>> index_counters_;
  mutable std::mutex index_counter_mutex_;

  void create_new_shard(const std::shared_ptr<Node> &node);

 public:
  explicit ShardManager(std::shared_ptr<SchemaRegistry> schema_registry,
                        const DatabaseConfig &config);

  /// Override the global shard ID counter (used during restore).
  void set_id_counter(int64_t value);
  [[nodiscard]] int64_t get_id_counter() const;
  /// Override the per-schema shard index counter (used during restore).
  void set_index_counter(const std::string &schema_name, int64_t value);
  [[nodiscard]] int64_t get_index_counter(const std::string &schema_name) const;

  /// Look up a shard by schema name and shard ID.
  arrow::Result<std::shared_ptr<Shard>> get_shard(
      const std::string &schema_name, int64_t id);

  /// Return the list of schema names that have at least one shard.
  [[nodiscard]] std::vector<std::string> get_schema_names() const;

  /// Return all shards for a schema (in index order).
  [[nodiscard]] arrow::Result<std::vector<std::shared_ptr<Shard>>> get_shards(
      const std::string &schema_name) const;

  /// True when the shard has not been modified since the last snapshot.
  arrow::Result<bool> is_shard_clean(std::string s, int64_t id);

  /// Merge under-filled shards for a schema.
  arrow::Result<bool> compact(const std::string &schema_name);
  /// Compact all schemas.
  arrow::Result<bool> compact_all();

  /// Insert a node into the appropriate shard (creating one if needed).
  arrow::Result<bool> insert_node(const std::shared_ptr<Node> &node);

  /// Look up a node across all shards for a schema.
  arrow::Result<std::shared_ptr<Node>> get_node(const std::string &schema_name,
                                                int64_t node_id);

  /// Remove a node from its shard.
  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id);

  /// Update a single field on a node (by Field descriptor).
  arrow::Result<bool> update_node(const std::string &schema_name, int64_t id,
                                  const std::shared_ptr<Field> &field,
                                  const Value &value, UpdateType update_type);

  /// Update a single field on a node (by field name).
  arrow::Result<bool> update_node(const std::string &schema_name, int64_t id,
                                  const std::string &field_name,
                                  const Value &value, UpdateType update_type);

  /// Batch-update multiple fields on a node.
  arrow::Result<bool> update_node_fields(
      const std::string &schema_name, int64_t id,
      const std::vector<FieldUpdate> &field_updates, UpdateType update_type);

  /// Collect all nodes across shards for a schema.
  arrow::Result<std::vector<std::shared_ptr<Node>>> get_nodes(
      const std::string &schema_name);

  /// Materialise all shards of a schema into Arrow Tables.
  arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> get_tables(
      const std::string &schema_name, TemporalContext *temporal_context);

  /// True when at least one shard exists for the schema.
  [[nodiscard]] bool has_shards(const std::string &schema_name) const;

  [[nodiscard]] arrow::Result<size_t> get_shard_count(
      const std::string &schema_name) const;

  [[nodiscard]] arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const;

  [[nodiscard]] arrow::Result<std::vector<std::pair<int64_t, int64_t>>>
  get_shard_ranges(const std::string &schema_name) const;

  /// Add a pre-built shard (used during snapshot restore).
  arrow::Result<bool> add_shard(const std::shared_ptr<Shard> &shard);
  /// Clear the dirty flag on all shards across all schemas.
  arrow::Result<bool> reset_all_updated();
};

}  // namespace tundradb
