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
