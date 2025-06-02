#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <arrow/result.h>

#include <filesystem>
#include <string>

#include "config.hpp"
#include "edge.hpp"
#include "metadata.hpp"

namespace tundradb {

class SchemaRegistry;
class Shard;

class Storage {
 private:
  std::string data_directory_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::string metadata_dir_;
  std::string data_dir_;
  DatabaseConfig config_;

 public:
  explicit Storage(std::string data_dir,
                   std::shared_ptr<SchemaRegistry> schema_registry,
                   DatabaseConfig config = DatabaseConfig());

  arrow::Result<bool> initialize();

  [[nodiscard]] arrow::Result<std::string> write_table(
      const std::shared_ptr<arrow::Table>& table, int64_t chunk_size,
      const std::string& prefix_path = "") const;

  [[nodiscard]] arrow::Result<std::string> write_shard(
      const std::shared_ptr<Shard>& shard) const;

  arrow::Result<std::shared_ptr<Shard>> read_shard(
      const ShardMetadata& shard_metadata);

  [[nodiscard]] arrow::Result<std::vector<Edge>> read_edges(
      const EdgeMetadata& edge_metadata) const;
};

}  // namespace tundradb

// Include core.hpp after our declarations to prevent circular dependencies
#include "core.hpp"

#endif  // STORAGE_HPP