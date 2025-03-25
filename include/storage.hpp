#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace tundradb {

// Forward declarations
class SchemaRegistry;
class Shard;

// Metadata for a single shard
struct ShardMetadata {
  std::string shard_id;
  std::string schema_name;
  int64_t min_id;
  int64_t max_id;
  size_t record_count;
  std::string data_file;
  int64_t timestamp_ms;  // Time in milliseconds since epoch
  
  // Serialize to JSON
  std::string to_json() const;
  
  // Deserialize from JSON
  static arrow::Result<ShardMetadata> from_json(const std::string& json_str);
};

class Storage {
private:
  std::string data_directory;
  std::shared_ptr<SchemaRegistry> schema_registry;

public:
  // Constructor
  explicit Storage(const std::string& data_dir, std::shared_ptr<SchemaRegistry> schema_registry);
  
  // Initialize storage system, creating directories if needed
  arrow::Result<bool> initialize();
  
  // Store a shard - writes both data and metadata
  // Returns the path to the metadata file
  arrow::Result<std::string> write_shard(int64_t snapshot_id, const std::shared_ptr<Shard>& shard);
  
  // Load a shard from its metadata path
  arrow::Result<std::shared_ptr<Shard>> read_shard(const std::string& metadata_path);
};

}  // namespace tundradb

// Include core.hpp after our declarations to prevent circular dependencies
#include "core.hpp"

#endif  // STORAGE_HPP 