#pragma once

#include <arrow/result.h>

#include <memory>

#include "edge_store.hpp"
#include "metadata.hpp"
#include "schema.hpp"

namespace tundradb {

// Forward declarations
class ShardManager;
class Storage;
class NodeManager;

class SnapshotManager {
 public:
  explicit SnapshotManager(std::shared_ptr<MetadataManager> metadata_manager,
                           std::shared_ptr<Storage> storage,
                           std::shared_ptr<ShardManager> shard_manager,
                           std::shared_ptr<EdgeStore> edge_store,
                           std::shared_ptr<NodeManager> node_manager,
                           std::shared_ptr<SchemaRegistry> schema_registry);

  arrow::Result<bool> initialize();
  arrow::Result<Snapshot> commit();
  Snapshot *current_snapshot();
  std::shared_ptr<Manifest> get_manifest();

 private:
  // --- Commit phases (called by commit()) ---

  void compact_and_preserve_flags();
  void commit_edges(
      const std::unordered_map<std::string, EdgeMetadata> &curr_edge_metadata,
      Manifest &new_manifest);
  void commit_shards(
      const std::unordered_map<std::string,
                               std::unordered_map<int64_t, ShardMetadata>>
          &curr_shard_metadata,
      Manifest &new_manifest);
  void commit_schemas(Snapshot &new_snapshot, const Manifest &new_manifest);
  void finalize_commit(const Manifest &new_manifest, int64_t timestamp_ms);

  // --- Initialization phases (called by initialize()) ---

  /**
   * Reads the current database metadata JSON from disk into `metadata_`.
   * Must run first; every other restore step depends on the loaded metadata.
   */
  arrow::Status restore_metadata();

  /**
   * Registers node schemas listed in `metadata_.schemas` into
   * `schema_registry_`.  Converts each persisted SchemaMetadata back to an
   * Arrow schema and adds it to the registry so that subsequent shard
   * loading can resolve field layouts.
   */
  arrow::Status restore_schemas();

  /**
   * Loads the manifest JSON referenced by the current snapshot and seeds
   * all ID-sequence counters (edge, node-per-schema, shard) plus per-schema
   * shard-index counters from the manifest.  Sets `manifest_`.
   */
  arrow::Status restore_manifest();

  /**
   * Reads every shard Parquet file listed in the manifest, grouped and
   * sorted by schema name / index, and adds the resulting Shard objects to
   * `shard_manager_`.
   */
  arrow::Status restore_shards();

  /**
   * Registers edge schemas from `metadata_.edge_schemas` into `edge_store_`
   * so that typed edge properties are available before edge rows are loaded.
   */
  arrow::Status restore_edge_schemas();

  /**
   * Reads edge Parquet files listed in the manifest and restores every edge
   * row directly into `edge_store_` (calls `Storage::read_edges`).
   */
  arrow::Status restore_edges();

  // --- Data members ---

  std::shared_ptr<MetadataManager> metadata_manager_;
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<ShardManager> shard_manager_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::shared_ptr<EdgeStore> edge_store_;
  std::shared_ptr<NodeManager> node_manager_;
  Metadata metadata_;
  std::shared_ptr<Manifest> manifest_;
  std::shared_ptr<EdgeMetadata> edge_metadata_;
};

}  // namespace tundradb
