#pragma once

#include <arrow/api.h>
#include <arrow/result.h>

#include <filesystem>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "file_utils.hpp"
#include "logger.hpp"

using namespace std::string_literals;

namespace tundradb {

struct Snapshot {
  int64_t id = 0;
  int64_t parent_id = 0;
  std::string manifest_location;
  int64_t timestamp_ms = 0;
  // int64_t node_id_counter; // todo
  // int64_t edge_id_counter; // todo

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Snapshot, id, parent_id, manifest_location,
                                 timestamp_ms);

  std::string toString() const {
    std::stringstream ss;
    ss << "Snapshot{id='" << id << "', parent_id=" << parent_id
       << ", manifest_location='" << manifest_location
       << "', timestamp_ms=" << timestamp_ms << "}";
    return ss.str();
  }

  friend std::ostream &operator<<(std::ostream &os, const Snapshot &snapshot) {
    os << snapshot.toString();
    return os;
  }
};

struct Metadata {
  // use raw pointer instead of std::shared_ptr for serialization simplicity
  Snapshot *current_snapshot = nullptr;
  std::vector<Snapshot> snapshots;

  // Custom serialization for Metadata to handle pointer
  friend void to_json(nlohmann::json &j, const Metadata &m) {
    j = nlohmann::json{{"snapshots", m.snapshots}};

    // Store the index of the current snapshot if it exists
    if (m.current_snapshot) {
      // Find the index of the current snapshot in the snapshots vector
      for (size_t i = 0; i < m.snapshots.size(); ++i) {
        if (m.current_snapshot == &m.snapshots[i]) {
          j["current_snapshot_index"] = i;
          break;
        }
      }
    } else {
      j["current_snapshot_index"] = -1;  // No current snapshot
    }
  }

  // Custom deserialization for Metadata to handle pointer
  friend void from_json(const nlohmann::json &j, Metadata &m) {
    j.at("snapshots").get_to(m.snapshots);

    // Set the current snapshot pointer if it exists
    if (j.contains("current_snapshot_index")) {
      int index = j["current_snapshot_index"];
      if (index >= 0 && index < static_cast<int>(m.snapshots.size())) {
        m.current_snapshot = &m.snapshots[index];
      } else {
        m.current_snapshot = nullptr;
      }
    } else {
      m.current_snapshot = nullptr;
    }
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "Metadata{current_snapshot="
       << (current_snapshot ? current_snapshot->toString() : "null")
       << ", snapshots_count=" << snapshots.size() << "}";
    return ss.str();
  }

  friend std::ostream &operator<<(std::ostream &os, const Metadata &metadata) {
    os << metadata.toString();
    return os;
  }
};

// DatabaseInfo to store location of the latest metadata file
struct DatabaseInfo {
  std::string metadata_location;
  int64_t timestamp_ms = 0;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(DatabaseInfo, metadata_location, timestamp_ms);

  std::string toString() const {
    std::stringstream ss;
    ss << "DatabaseInfo{metadata_location='" << metadata_location
       << "', timestamp_ms=" << timestamp_ms << "}";
    return ss.str();
  }

  friend std::ostream &operator<<(std::ostream &os, const DatabaseInfo &info) {
    os << info.toString();
    return os;
  }
};

struct ShardMetadata {
  int64_t id = 0;
  std::string schema_name;
  int64_t min_id = 0;
  int64_t max_id = 0;
  size_t record_count = 0;
  size_t chunk_size = 0;
  std::string data_file;
  int64_t timestamp_ms = 0;  // Time in milliseconds since epoch

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(ShardMetadata, id, schema_name, min_id, max_id,
                                 record_count, chunk_size, data_file,
                                 timestamp_ms);

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    ss << "ShardMetadata{id='" << id << "', schema_name='" << schema_name
       << "', min_id=" << min_id << ", max_id=" << max_id
       << ", record_count=" << record_count << ", chunk_size=" << chunk_size
       << ", data_file='" << data_file << "', timestamp_ms=" << timestamp_ms
       << "}";
    return ss.str();
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const ShardMetadata &shard) {
    os << shard.toString();
    return os;
  }

  std::string compound_id() const {
    return this->schema_name + "-" + std::to_string(this->id);
  }
};

struct EdgeMetadata {
  std::string id;
  std::string edge_type;
  std::string data_file;
  int64_t record_count = 0;
  int64_t chunk_size = 0;
  int64_t id_seq = 0;
  int64_t timestamp_ms = 0;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(EdgeMetadata, edge_type, data_file,
                                 record_count, timestamp_ms);
};

struct Manifest {
  std::string id;
  std::vector<ShardMetadata> shards;
  std::string edges_file;
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Manifest, id, shards);

  std::string toString() const {
    std::stringstream ss;
    ss << "Manifest{id='" << id << "', shards=[";
    for (size_t i = 0; i < shards.size(); ++i) {
      ss << shards[i];
      if (i < shards.size() - 1) ss << ", ";
    }
    ss << "]}";
    return ss.str();
  }

  friend std::ostream &operator<<(std::ostream &os, const Manifest &manifest) {
    os << manifest.toString();
    return os;
  }
};

class MetadataManager {
 public:
  explicit MetadataManager(const std::string &metadata_dir_path);

  arrow::Result<bool> initialize();

  arrow::Result<std::string> write_manifest(const Manifest &manifest);
  arrow::Result<Manifest> read_manifest(const std::string &id);

  arrow::Result<std::string> write_metadata(const Metadata &metadata);
  arrow::Result<Metadata> read_metadata(const std::string &path);

  arrow::Result<std::string> write_db_info(const DatabaseInfo &db_info);
  arrow::Result<DatabaseInfo> read_db_info();

  arrow::Result<Metadata> load_current_metadata();

  arrow::Result<std::string> write_edge_metadata(
      const EdgeMetadata &edge_metadata);
  arrow::Result<EdgeMetadata> read_edge_metadata(const std::string &id);

  const std::string &get_metadata_dir() const;

 private:
  std::string metadata_dir;
};
}  // namespace tundradb
