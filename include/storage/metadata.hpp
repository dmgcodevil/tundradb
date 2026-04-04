#pragma once

#include <arrow/api.h>
#include <arrow/result.h>

#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/map_union_types.hpp"
#include "common/logger.hpp"
#include "common/types.hpp"
#include "json.hpp"
#include "schema/schema.hpp"
#include "schema/type_descriptor.hpp"
#include "storage/file_utils.hpp"

using namespace std::string_literals;

namespace tundradb {

/**
 * @brief A simplified representation of a field schema.
 *
 * For non-array types, only `name`, `type`, `nullable` are meaningful.
 * For ARRAY types, `element_type` and `fixed_size` carry the array parameters.
 * Old JSON files without these fields will deserialize cleanly (defaults).
 */
struct FieldMetadata {
  std::string name;
  ValueType type = ValueType::NA;
  bool nullable = true;
  ValueType element_type = ValueType::NA;  // for ARRAY
  uint32_t fixed_size = 0;                 // for ARRAY: 0=dynamic
  uint32_t max_string_size = 0;            // for STRING: 0=unlimited

  /// Build a TypeDescriptor from this metadata.
  [[nodiscard]] TypeDescriptor to_type_descriptor() const {
    if (type == ValueType::ARRAY) {
      return TypeDescriptor::array(element_type, fixed_size);
    }
    auto td = TypeDescriptor::from_value_type(type);
    td.max_string_size = max_string_size;
    return td;
  }

  /// Build FieldMetadata from a TypeDescriptor.
  static FieldMetadata from_type_descriptor(const std::string &field_name,
                                            const TypeDescriptor &td,
                                            bool is_nullable = true) {
    FieldMetadata fm;
    fm.name = field_name;
    fm.type = td.base_type;
    fm.nullable = is_nullable;
    fm.element_type = td.element_type;
    fm.fixed_size = td.fixed_size;
    fm.max_string_size = td.max_string_size;
    return fm;
  }

  // Custom JSON: write all fields; on read, missing array params default to
  // 0/NA
  friend void to_json(nlohmann::json &j, const FieldMetadata &fm) {
    j = nlohmann::json{
        {"name", fm.name}, {"type", fm.type}, {"nullable", fm.nullable}};
    if (fm.type == ValueType::ARRAY) {
      j["element_type"] = fm.element_type;
      j["fixed_size"] = fm.fixed_size;
    }
    if (fm.max_string_size > 0) {
      j["max_string_size"] = fm.max_string_size;
    }
  }

  friend void from_json(const nlohmann::json &j, FieldMetadata &fm) {
    j.at("name").get_to(fm.name);
    j.at("type").get_to(fm.type);
    j.at("nullable").get_to(fm.nullable);
    if (j.contains("element_type")) {
      fm.element_type = j.at("element_type").get<ValueType>();
    }
    if (j.contains("fixed_size")) {
      fm.fixed_size = j.at("fixed_size").get<uint32_t>();
    }
    if (j.contains("max_string_size")) {
      fm.max_string_size = j.at("max_string_size").get<uint32_t>();
    }
  }
};

/**
 * @brief A simplified representation of a schema
 */
struct SchemaMetadata {
  std::string name;
  uint32_t version;
  std::vector<FieldMetadata> fields;

  // Allow JSON serialization/deserialization
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(SchemaMetadata, name, version, fields)
};

std::shared_ptr<Field> from_metadata(const FieldMetadata &metadata);
std::shared_ptr<Schema> from_metadata(const SchemaMetadata &metadata);

arrow::Result<FieldMetadata> ArrowFieldToMetadata(
    const std::shared_ptr<arrow::Field> &field);

/// Map a scalar ValueType to an Arrow DataType.
std::shared_ptr<arrow::DataType> scalar_vt_to_arrow(ValueType vt);

arrow::Result<std::shared_ptr<arrow::Field>> metadata_to_arrow_field(
    const FieldMetadata &metadata);

arrow::Result<SchemaMetadata> arrow_schema_to_metadata(
    const std::string &schema_name,
    const std::shared_ptr<arrow::Schema> &schema);

arrow::Result<std::shared_ptr<arrow::Schema>> metadata_to_arrow_schema(
    const SchemaMetadata &metadata);

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
  std::vector<SchemaMetadata> edge_schemas;

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

  friend void to_json(nlohmann::json &j, const Metadata &m) {
    j = nlohmann::json{{"schemas", m.schemas},
                       {"edge_schemas", m.edge_schemas},
                       {"snapshots", m.snapshots},
                       {"current_snapshot_index", m.current_snapshot_index}};
  }

  friend void from_json(const nlohmann::json &j, Metadata &m) {
    j.at("snapshots").get_to(m.snapshots);
    j.at("schemas").get_to(m.schemas);
    if (j.contains("edge_schemas")) {
      j.at("edge_schemas").get_to(m.edge_schemas);
    }
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
  int32_t schema_version = -1;  // -1 = schema-less

  friend void to_json(nlohmann::json &j, const EdgeMetadata &em) {
    j = nlohmann::json{{"edge_type", em.edge_type},
                       {"data_file", em.data_file},
                       {"record_count", em.record_count}};
    if (em.schema_version >= 0) {
      j["schema_version"] = em.schema_version;
    }
  }

  friend void from_json(const nlohmann::json &j, EdgeMetadata &em) {
    j.at("edge_type").get_to(em.edge_type);
    j.at("data_file").get_to(em.data_file);
    j.at("record_count").get_to(em.record_count);
    em.schema_version = j.value("schema_version", -1);
  }
};

struct Manifest {
  std::string id;
  std::vector<ShardMetadata> shards;
  std::vector<EdgeMetadata> edges;
  std::unordered_map<std::string, int64_t>
      node_id_seq_per_schema;  // Per-schema ID counters
  int64_t edge_id_seq = 0;
  int64_t shard_id_seq = 0;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Manifest, id, shards, edges,
                                 node_id_seq_per_schema, edge_id_seq,
                                 shard_id_seq);

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
    ss << "], node_id_seq_per_schema={";
    size_t idx = 0;
    for (const auto &[schema_name, counter] : node_id_seq_per_schema) {
      ss << "'" << schema_name << "':" << counter;
      if (idx++ < node_id_seq_per_schema.size() - 1) ss << ", ";
    }
    ss << "}, edge_id_seq=" << edge_id_seq << ", shard_id_seq=" << shard_id_seq
       << "}";
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

  arrow::Result<bool> initialize() const;

  [[nodiscard]] arrow::Result<std::string> write_manifest(
      const Manifest &manifest) const;
  [[nodiscard]] arrow::Result<Manifest> read_manifest(
      const std::string &id) const;

  [[nodiscard]] arrow::Result<std::string> write_metadata(
      const Metadata &metadata) const;
  [[nodiscard]] arrow::Result<Metadata> read_metadata(
      const std::string &path) const;

  [[nodiscard]] arrow::Result<std::string> write_db_info(
      const DatabaseInfo &db_info) const;
  [[nodiscard]] arrow::Result<DatabaseInfo> read_db_info() const;

  [[nodiscard]] arrow::Result<Metadata> load_current_metadata() const;

  const std::string &get_metadata_dir() const;

 private:
  std::string metadata_dir;
};
}  // namespace tundradb
