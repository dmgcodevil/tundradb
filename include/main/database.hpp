#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/table.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/utils.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "common/utils.hpp"
#include "core/edge_store.hpp"
#include "core/field_update.hpp"
#include "core/node.hpp"
#include "query/execution.hpp"
#include "query/query.hpp"
#include "schema/schema.hpp"
#include "storage/metadata.hpp"
#include "storage/shard.hpp"
#include "storage/snapshot_manager.hpp"
#include "storage/storage.hpp"

namespace tundradb {

/// The main entry point for interacting with TundraDB.
///
/// Owns the schema registry, shard manager, node manager, edge store,
/// and (optionally) persistence subsystems.  All public methods are
/// thread-compatible; external synchronisation is required for concurrent
/// access.
class Database {
 private:
  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::shared_ptr<ShardManager> shard_manager_;
  std::shared_ptr<NodeManager> node_manager_;
  DatabaseConfig config_;
  bool persistence_enabled_;
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<MetadataManager> metadata_manager_;
  std::shared_ptr<SnapshotManager> snapshot_manager_;
  std::shared_ptr<EdgeStore> edge_store_;

 public:
  /// Construct a database from the given configuration.
  /// When persistence is enabled, the storage, metadata, and snapshot
  /// subsystems are initialised from `config.get_db_path()`.
  explicit Database(const DatabaseConfig &config = DatabaseConfig());

  /// Return a copy of the configuration used to create this database.
  DatabaseConfig get_config() const { return config_; }
  /// Return the schema registry (shared ownership).
  std::shared_ptr<SchemaRegistry> get_schema_registry() {
    return schema_registry_;
  }
  /// Return the metadata manager (nullptr when persistence is disabled).
  std::shared_ptr<MetadataManager> get_metadata_manager() {
    return metadata_manager_;
  }
  /// Return the node manager (shared ownership).
  std::shared_ptr<NodeManager> get_node_manager() { return node_manager_; }
  /// Return the edge store (shared ownership).
  [[nodiscard]] std::shared_ptr<EdgeStore> get_edge_store() const {
    return edge_store_;
  }
  /// Return the shard manager (shared ownership).
  [[nodiscard]] std::shared_ptr<ShardManager> get_shard_manager() const {
    return shard_manager_;
  }

  /// Initialise persistence subsystems (storage, metadata, snapshots).
  arrow::Result<bool> initialize();

  /// Create a new node in the given schema with the supplied field values.
  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      const std::unordered_map<std::string, Value> &data);

  /// Update a single field on a node (by Field descriptor).
  arrow::Result<bool> update_node(const std::string &schema_name, int64_t id,
                                  const std::shared_ptr<Field> &field,
                                  const Value &value, UpdateType update_type);

  /// Update a single field on a node (by field name).
  arrow::Result<bool> update_node(const std::string &schema_name, int64_t id,
                                  const std::string &field_name,
                                  const Value &value, UpdateType update_type);

  /// Batch-update multiple fields on one node (creates 1 version).
  arrow::Result<bool> update_node_fields(
      const std::string &schema_name, int64_t id,
      const std::vector<FieldUpdate> &field_updates, UpdateType update_type);

  /// Remove a node from both the node manager and its shard.
  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id);

  /// Register a typed edge schema for edges of @p edge_type.
  arrow::Result<bool> register_edge_schema(
      const std::string &edge_type,
      const std::vector<std::shared_ptr<Field>> &fields);

  /// Create an edge from @p source_id to @p target_id with the given type.
  arrow::Result<bool> connect(int64_t source_id, const std::string &type,
                              int64_t target_id);

  /// Create an edge with property values attached.
  arrow::Result<bool> connect(
      int64_t source_id, const std::string &type, int64_t target_id,
      std::unordered_map<std::string, Value> properties);

  /// Remove an edge by its unique ID.
  arrow::Result<bool> remove_edge(int64_t edge_id);

  /// Compact shards for a single schema, merging small shards together.
  arrow::Result<bool> compact(const std::string &schema_name);
  /// Compact shards for every registered schema.
  arrow::Result<bool> compact_all();

  /// Materialise all nodes of a schema into an Arrow Table.
  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &schema_name,
      TemporalContext *temporal_context = nullptr,
      size_t chunk_size = 10000) const;

  /// Return the number of shards backing the given schema.
  arrow::Result<size_t> get_shard_count(const std::string &schema_name) const;
  /// Return the node count in each shard for the given schema.
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const;
  /// Return the [min_id, max_id] range for each shard of the given schema.
  arrow::Result<std::vector<std::pair<int64_t, int64_t>>> get_shard_ranges(
      const std::string &schema_name) const;

  /// Persist the current database state to disk as a new snapshot.
  arrow::Result<Snapshot> create_snapshot();

  /// Execute a read-only query and return the result set as an Arrow Table.
  [[nodiscard]] arrow::Result<std::shared_ptr<QueryResult>> query(
      const Query &query) const;

  /**
   * @brief Execute an UpdateQuery.
   *
   * Mode 1 - by ID (bare field names):
   *   db.update(UpdateQuery::on("User", 0).set("age", Value(31)).build());
   *
   * Mode 2 - by MATCH query (alias-qualified SET, multi-schema):
   *   db.update(UpdateQuery::match(
   *       Query::from("u:User")
   *           .traverse("u", "WORKS_AT", "c:Company")
   *           .where("c.name", CompareOp::Eq, Value("Google"))
   *           .build()
   *   ).set("u.status", Value("employed"))
   *    .set("c.size", Value(int32_t(5001)))
   *    .build());
   */
  [[nodiscard]] arrow::Result<UpdateResult> update(const UpdateQuery &uq);

 private:
  /** Mode 1: update a single node by schema + ID. */
  [[nodiscard]] arrow::Result<UpdateResult> update_by_id(const UpdateQuery &uq);

  /** Mode 2: find nodes via MATCH query, then batch-update each. */
  [[nodiscard]] arrow::Result<UpdateResult> update_by_match(
      const UpdateQuery &uq);

  /**
   * Apply field updates to every node whose ID appears in @p id_column.
   * One call to update_node_fields() per unique node ID (1 version each).
   */
  void apply_updates(const std::string &schema_name,
                     const std::shared_ptr<arrow::ChunkedArray> &id_column,
                     const std::vector<FieldUpdate> &fields,
                     UpdateType update_type, UpdateResult &result);

  /**
   * Build an alias->schema mapping from a Query's FROM + TRAVERSE clauses.
   * Only declarations ("alias:Schema") are recorded; bare references ("alias")
   * are skipped.  Returns an error if the same alias is bound to two different
   * schemas.
   */
};

}  // namespace tundradb
