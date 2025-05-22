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
#include "types.hpp"

using namespace std::string_literals;

namespace tundradb {

/**
 * @brief A simplified representation of a field schema
 */
struct FieldMetadata {
  std::string name;
  ValueType type;
  bool nullable = true;

  // Allow JSON serialization/deserialization
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(FieldMetadata, name, type, nullable)
};

/**
 * @brief A simplified representation of a schema
 */
struct SchemaMetadata {
  std::string name;
  int version;
  std::vector<FieldMetadata> fields;

  // Allow JSON serialization/deserialization
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(SchemaMetadata, name, version, fields)
};

/**
 * @brief Convert an Arrow field to FieldMetadata
 *
 * @param field The Arrow field to convert
 * @return arrow::Result<FieldMetadata> The resulting field metadata
 */
inline arrow::Result<FieldMetadata> ArrowFieldToMetadata(
    const std::shared_ptr<arrow::Field> &field) {
  FieldMetadata result;
  result.name = field->name();
  result.nullable = field->nullable();

  switch (field->type()->id()) {
    case arrow::Type::BOOL:
      result.type = ValueType::Bool;
      break;
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      result.type = ValueType::Int64;
      break;
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
      result.type = ValueType::Double;
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      result.type = ValueType::String;
      break;
    default:
      return arrow::Status::NotImplemented("Unsupported Arrow type: ",
                                           field->type()->ToString());
  }

  return result;
}

/**
 * @brief Convert FieldMetadata to an Arrow field
 *
 * @param metadata The field metadata to convert
 * @return arrow::Result<std::shared_ptr<arrow::Field>> The resulting Arrow
 * field
 */
inline arrow::Result<std::shared_ptr<arrow::Field>> MetadataToArrowField(
    const FieldMetadata &metadata) {
  std::shared_ptr<arrow::DataType> type;

  switch (metadata.type) {
    case ValueType::Bool:
      type = arrow::boolean();
      break;
    case ValueType::Int64:
      type = arrow::int64();
      break;
    case ValueType::Double:
      type = arrow::float64();
      break;
    case ValueType::String:
      type = arrow::utf8();
      break;
    default:
      return arrow::Status::NotImplemented("Unsupported ValueType: ",
                                           static_cast<int>(metadata.type));
  }

  return arrow::field(metadata.name, type, metadata.nullable);
}

/**
 * @brief Convert an Arrow schema to SchemaMetadata
 *
 * @param schema The Arrow schema to convert
 * @return arrow::Result<SchemaMetadata> The resulting schema metadata
 */
inline arrow::Result<SchemaMetadata> ArrowSchemaToMetadata(
    const std::string &schema_name,
    const std::shared_ptr<arrow::Schema> &schema) {
  SchemaMetadata result;
  result.name = schema_name;
  result.version = 0;

  for (const auto &field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto field_metadata, ArrowFieldToMetadata(field));
    result.fields.push_back(field_metadata);
  }

  return result;
}

/**
 * @brief Convert SchemaMetadata to an Arrow schema
 *
 * @param metadata The schema metadata to convert
 * @return arrow::Result<std::shared_ptr<arrow::Schema>> The resulting Arrow
 * schema
 */
inline arrow::Result<std::shared_ptr<arrow::Schema>> MetadataToArrowSchema(
    const SchemaMetadata &metadata) {
  std::vector<std::shared_ptr<arrow::Field>> fields;

  for (const auto &field_metadata : metadata.fields) {
    ARROW_ASSIGN_OR_RAISE(auto field, MetadataToArrowField(field_metadata));
    fields.push_back(field);
  }

  return arrow::schema(fields);
}

struct Snapshot {
  int64_t id = 0;
  int64_t parent_id = 0;
  std::string manifest_location;
  int64_t timestamp_ms = 0;

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
  int current_snapshot_index = -1;
  std::vector<Snapshot> snapshots;
  std::vector<SchemaMetadata> schemas;

  // Get current snapshot safely
  Snapshot *get_current_snapshot() {
    if (current_snapshot_index >= 0 &&
        current_snapshot_index < static_cast<int>(snapshots.size())) {
      return &snapshots[current_snapshot_index];
    }
    return nullptr;
  }

  const Snapshot *get_current_snapshot() const {
    if (current_snapshot_index >= 0 &&
        current_snapshot_index < static_cast<int>(snapshots.size())) {
      return &snapshots[current_snapshot_index];
    }
    return nullptr;
  }

  // Custom serialization for Metadata
  friend void to_json(nlohmann::json &j, const Metadata &m) {
    j = nlohmann::json{{"schemas", m.schemas},
                       {"snapshots", m.snapshots},
                       {"current_snapshot_index", m.current_snapshot_index}};
  }

  // Custom deserialization for Metadata
  friend void from_json(const nlohmann::json &j, Metadata &m) {
    j.at("snapshots").get_to(m.snapshots);
    j.at("schemas").get_to(m.schemas);
    m.current_snapshot_index = j.value("current_snapshot_index", -1);
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "Metadata{current_snapshot="
       << (get_current_snapshot() ? get_current_snapshot()->toString() : "null")
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
  int64_t index = 0;         // Position of the shard within its schema

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(ShardMetadata, id, schema_name, min_id, max_id,
                                 record_count, chunk_size, data_file,
                                 timestamp_ms, index);

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    ss << "ShardMetadata{id='" << id << "', schema_name='" << schema_name
       << "', min_id=" << min_id << ", max_id=" << max_id
       << ", record_count=" << record_count << ", chunk_size=" << chunk_size
       << ", data_file='" << data_file << "', timestamp_ms=" << timestamp_ms
       << ", index=" << index << "}";
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
  std::string edge_type;
  std::string data_file;
  int64_t record_count = 0;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(EdgeMetadata, edge_type, data_file,
                                 record_count);
};

struct Manifest {
  std::string id;
  std::vector<ShardMetadata> shards;
  std::vector<EdgeMetadata> edges;
  int64_t node_id_seq = 0;
  int64_t edge_id_seq = 0;
  int64_t shard_id_seq = 0;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Manifest, id, shards, edges, node_id_seq,
                                 edge_id_seq, shard_id_seq);

  std::string toString() const {
    std::stringstream ss;
    ss << "Manifest{id='" << id << "', shards=[";
    for (size_t i = 0; i < shards.size(); ++i) {
      ss << shards[i];
      if (i < shards.size() - 1) ss << ", ";
    }
    ss << "], edges=[";
    for (size_t i = 0; i < edges.size(); ++i) {
      ss << "EdgeMetadata{edge_type='" << edges[i].edge_type << "', data_file='"
         << edges[i].data_file << "', record_count=" << edges[i].record_count
         << "}";
      if (i < edges.size() - 1) ss << ", ";
    }
    ss << "], node_id_seq=" << node_id_seq << ", edge_id_seq=" << edge_id_seq
       << ", shard_id_seq=" << shard_id_seq << "}";
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
