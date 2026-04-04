#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <arrow/result.h>

#include <filesystem>
#include <string>

#include "common/config.hpp"
#include "core/edge.hpp"
#include "storage/metadata.hpp"

namespace tundradb {

class SchemaRegistry;
class NodeManager;
class Shard;
class EdgeStore;

/// Low-level persistence layer: reads and writes Parquet files for
/// shards and edge tables under the configured data directory.
class Storage {
 private:
  std::string data_directory_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::string metadata_dir_;
  std::string data_dir_;
  DatabaseConfig config_;
  std::shared_ptr<NodeManager> node_manager_;

 public:
  explicit Storage(std::string data_dir,
                   std::shared_ptr<SchemaRegistry> schema_registry,
                   std::shared_ptr<NodeManager> node_manager_,
                   DatabaseConfig config = DatabaseConfig());

  /// Create the data directory structure on disk.
  arrow::Result<bool> initialize();

  /// Write an Arrow Table to a new Parquet file and return the file path.
  [[nodiscard]] arrow::Result<std::string> write_table(
      const std::shared_ptr<arrow::Table>& table, int64_t chunk_size,
      const std::string& prefix_path = "") const;

  /// Materialise a Shard into an Arrow Table and write it to Parquet.
  [[nodiscard]] arrow::Result<std::string> write_shard(
      const std::shared_ptr<Shard>& shard) const;

  /// Read a Shard back from Parquet using metadata for path resolution.
  arrow::Result<std::shared_ptr<Shard>> read_shard(
      const ShardMetadata& shard_metadata);

  /// Read edge rows from Parquet and restore them into the EdgeStore.
  [[nodiscard]] arrow::Result<bool> read_edges(
      const EdgeMetadata& edge_metadata,
      const std::shared_ptr<EdgeStore>& edge_store) const;
};

}  // namespace tundradb

#endif  // STORAGE_HPP