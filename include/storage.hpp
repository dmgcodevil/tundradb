#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <nlohmann/json.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace tundradb {

// Forward declarations
class SchemaRegistry;
class Shard;



class Storage {
 private:
  std::string data_directory;
  std::shared_ptr<SchemaRegistry> schema_registry;
  std::string metadata_dir;
  std::string data_dir;

 public:
  // Constructor
  explicit Storage(const std::string& data_dir,
                   std::shared_ptr<SchemaRegistry> schema_registry);

  // Initialize storage system, creating directories if needed
  arrow::Result<bool> initialize();

  // Store a shard - writes both data and metadata
  // Returns the path to the metadata file
  arrow::Result<std::string> write_shard(int64_t snapshot_id,
                                         const std::shared_ptr<Shard>& shard);

  // Load a shard from its metadata path
  arrow::Result<std::shared_ptr<Shard>> read_shard(
      const std::string& metadata_path);
};

}  // namespace tundradb

// Include core.hpp after our declarations to prevent circular dependencies
#include "core.hpp"

#endif  // STORAGE_HPP