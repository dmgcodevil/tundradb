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
  explicit Database(const DatabaseConfig &config = DatabaseConfig())
      : schema_registry_(std::make_shared<SchemaRegistry>()),
        shard_manager_(
            std::make_shared<ShardManager>(schema_registry_, config)),
        node_manager_(std::make_shared<NodeManager>(
            schema_registry_, config.is_validation_enabled(), true,
            config.is_versioning_enabled())),
        config_(config),
        persistence_enabled_(config.is_persistence_enabled()),
        edge_store_(std::make_shared<EdgeStore>(0, config.get_chunk_size())) {
    // Initialize Arrow Compute module early in database lifecycle
    if (!initialize_arrow_compute()) {
      log_error("Failed to initialize Arrow Compute module");
      // Continue anyway, some operations might still work
    }

    if (persistence_enabled_) {
      const std::string &db_path = config.get_db_path();
      if (db_path.empty()) {
        log_error("Database path is empty but persistence is enabled");
        persistence_enabled_ = false;
        return;
      }

      std::string data_path = db_path + "/data";
      storage_ = std::make_shared<Storage>(
          std::move(data_path), schema_registry_, node_manager_, config);
      metadata_manager_ = std::make_shared<MetadataManager>(db_path);
      snapshot_manager_ = std::make_shared<SnapshotManager>(
          metadata_manager_, storage_, shard_manager_, edge_store_,
          node_manager_, schema_registry_);
    }
  }

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

  /// Initialise persistence subsystems (storage, metadata, snapshots).
  /// Must be called once after construction when persistence is enabled.
  /// Returns `true` on success or skips silently when persistence is off.
  arrow::Result<bool> initialize() {
    if (persistence_enabled_) {
      auto storage_init = this->storage_->initialize();
      if (!storage_init.ok()) {
        return storage_init.status();
      }

      auto metadata_init = this->metadata_manager_->initialize();
      if (!metadata_init.ok()) {
        return metadata_init.status();
      }

      auto snapshot_init = this->snapshot_manager_->initialize();
      if (!snapshot_init.ok()) {
        return snapshot_init.status();
      }
    }
    return true;
  }

  /// Create a new node in the given schema with the supplied field values.
  /// The node is assigned a unique auto-incremented ID and inserted into
  /// the appropriate shard.
  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      const std::unordered_map<std::string, Value> &data) {
    if (schema_name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }
    ARROW_ASSIGN_OR_RAISE(auto node,
                          node_manager_->create_node(schema_name, data));
    ARROW_RETURN_NOT_OK(shard_manager_->insert_node(node));  // TODO optimize
    return node;
  }

  /// Update a single field on a node identified by schema name and ID.
  /// @param field  Resolved field descriptor.
  arrow::Result<bool> update_node(const std::string &schema_name,
                                  const int64_t id,
                                  const std::shared_ptr<Field> &field,
                                  const Value &value,
                                  const UpdateType update_type) {
    return shard_manager_->update_node(schema_name, id, field, value,
                                       update_type);
  }

  /// Update a single field on a node, looked up by field name.
  arrow::Result<bool> update_node(const std::string &schema_name,
                                  const int64_t id,
                                  const std::string &field_name,
                                  const Value &value,
                                  const UpdateType update_type) {
    return shard_manager_->update_node(schema_name, id, field_name, value,
                                       update_type);
  }

  /**
   * @brief Batch-update multiple fields on one node (creates 1 version).
   */
  arrow::Result<bool> update_node_fields(
      const std::string &schema_name, const int64_t id,
      const std::vector<FieldUpdate> &field_updates,
      const UpdateType update_type) {
    return shard_manager_->update_node_fields(schema_name, id, field_updates,
                                              update_type);
  }

  /// Remove a node from both the node manager and its shard.
  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id) {
    if (auto res = node_manager_->remove_node(schema_name, node_id); !res) {
      return arrow::Status::Invalid("Failed to remove node: ", schema_name, ":",
                                    node_id);
    }
    return shard_manager_->remove_node(schema_name, node_id);
  }

  /// Register a typed edge schema so that edges of @p edge_type can carry
  /// the given property fields.
  arrow::Result<bool> register_edge_schema(
      const std::string &edge_type,
      const std::vector<std::shared_ptr<Field>> &fields) {
    return edge_store_->register_edge_schema(edge_type, fields);
  }

  /// Create an edge from @p source_id to @p target_id with the given type.
  arrow::Result<bool> connect(const int64_t source_id, const std::string &type,
                              const int64_t target_id) {
    const auto edge =
        edge_store_->create_edge(source_id, type, target_id).ValueOrDie();
    ARROW_RETURN_NOT_OK(edge_store_->add(edge));
    return true;
  }

  /// Create an edge with property values attached.
  arrow::Result<bool> connect(
      const int64_t source_id, const std::string &type, const int64_t target_id,
      std::unordered_map<std::string, Value> properties) {
    ARROW_ASSIGN_OR_RAISE(const auto edge,
                          edge_store_->create_edge(source_id, type, target_id,
                                                   std::move(properties)));
    ARROW_RETURN_NOT_OK(edge_store_->add(edge));
    return true;
  }

  /// Remove an edge by its unique ID.
  arrow::Result<bool> remove_edge(const int64_t edge_id) {
    return edge_store_->remove(edge_id);
  }

  /// Compact shards for a single schema, merging small shards together.
  arrow::Result<bool> compact(const std::string &schema_name) {
    return shard_manager_->compact(schema_name);
  }

  /// Return the edge store (shared ownership).
  [[nodiscard]] std::shared_ptr<EdgeStore> get_edge_store() const {
    return edge_store_;
  }

  /// Return the shard manager (shared ownership).
  [[nodiscard]] std::shared_ptr<ShardManager> get_shard_manager() const {
    return shard_manager_;
  }

  /// Compact shards for every registered schema.
  arrow::Result<bool> compact_all() { return shard_manager_->compact_all(); }

  /// Materialise all nodes of a schema into an Arrow Table.
  /// @param temporal_context  Optional temporal filter; when non-null, only
  ///   field versions visible at the requested point-in-time are included.
  /// @param chunk_size  Maximum number of rows per Arrow record batch.
  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &schema_name,
      TemporalContext *temporal_context = nullptr,
      size_t chunk_size = 10000) const {
    ARROW_ASSIGN_OR_RAISE(const auto schema,
                          schema_registry_->get(schema_name));
    auto arrow_schema = schema->arrow();
    ARROW_ASSIGN_OR_RAISE(auto all_nodes,
                          shard_manager_->get_nodes(schema_name));

    if (all_nodes.empty()) {
      std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
      empty_columns.reserve(arrow_schema->num_fields());
      for (int i = 0; i < arrow_schema->num_fields(); i++) {
        empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(
            std::vector<std::shared_ptr<arrow::Array>>{}));
      }
      return arrow::Table::Make(arrow_schema, empty_columns);
    }

    std::ranges::sort(all_nodes, [](const std::shared_ptr<Node> &a,
                                    const std::shared_ptr<Node> &b) {
      return a->id < b->id;
    });

    return create_table(schema, all_nodes, chunk_size, temporal_context);
  }

  /// Return the number of shards backing the given schema.
  arrow::Result<size_t> get_shard_count(const std::string &schema_name) const {
    if (!schema_registry_->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager_->get_shard_count(schema_name);
  }

  /// Return the node count in each shard for the given schema.
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const {
    if (!schema_registry_->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager_->get_shard_sizes(schema_name);
  }

  /// Return the [min_id, max_id] range for each shard of the given schema.
  arrow::Result<std::vector<std::pair<int64_t, int64_t>>> get_shard_ranges(
      const std::string &schema_name) const {
    if (!schema_registry_->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager_->get_shard_ranges(schema_name);
  }

  /// Persist the current database state to disk as a new snapshot.
  /// Requires persistence to be enabled.
  arrow::Result<Snapshot> create_snapshot() {
    return snapshot_manager_->commit();
  }

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
  static arrow::Result<std::unordered_map<std::string, std::string>>
  resolve_alias_map(const Query &query);
};

}  // namespace tundradb
