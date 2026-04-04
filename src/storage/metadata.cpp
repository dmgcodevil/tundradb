#include "storage/metadata.hpp"

#include <arrow/api.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "arrow/map_union_types.hpp"
#include "common/logger.hpp"
#include "common/utils.hpp"
#include "json.hpp"
#include "llvm/ADT/SmallVector.h"

namespace tundradb {

// ---------------------------------------------------------------------------
// Conversion functions moved from metadata.hpp
// ---------------------------------------------------------------------------

std::shared_ptr<Field> from_metadata(const FieldMetadata &metadata) {
  return std::make_shared<Field>(metadata.name, metadata.to_type_descriptor(),
                                 metadata.nullable);
}

std::shared_ptr<Schema> from_metadata(const SchemaMetadata &metadata) {
  llvm::SmallVector<std::shared_ptr<Field>, 4> fields;
  fields.reserve(metadata.fields.size());
  for (const auto &field_meta : metadata.fields) {
    fields.push_back(from_metadata(field_meta));
  }
  return std::make_shared<Schema>(metadata.name, metadata.version, fields);
}

arrow::Result<FieldMetadata> ArrowFieldToMetadata(
    const std::shared_ptr<arrow::Field> &field) {
  FieldMetadata result;
  result.name = field->name();
  result.nullable = field->nullable();

  const auto &dt = field->type();
  switch (dt->id()) {
    case arrow::Type::BOOL:
      result.type = ValueType::BOOL;
      break;
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      result.type = ValueType::INT64;
      break;
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
      result.type = ValueType::DOUBLE;
      break;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      result.type = ValueType::STRING;
      break;
    case arrow::Type::LIST: {
      auto list_type = std::static_pointer_cast<arrow::ListType>(dt);
      auto elem_result = ArrowFieldToMetadata(list_type->value_field());
      if (!elem_result.ok()) return elem_result.status();
      result.type = ValueType::ARRAY;
      result.element_type = elem_result.ValueOrDie().type;
      result.fixed_size = 0;
      break;
    }
    case arrow::Type::FIXED_SIZE_LIST: {
      auto fsl_type = std::static_pointer_cast<arrow::FixedSizeListType>(dt);
      auto elem_result = ArrowFieldToMetadata(fsl_type->value_field());
      if (!elem_result.ok()) return elem_result.status();
      result.type = ValueType::ARRAY;
      result.element_type = elem_result.ValueOrDie().type;
      result.fixed_size = static_cast<uint32_t>(fsl_type->list_size());
      break;
    }
    case arrow::Type::MAP:
      result.type = ValueType::MAP;
      break;
    default:
      return arrow::Status::NotImplemented("Unsupported Arrow type: ",
                                           dt->ToString());
  }
  return result;
}

std::shared_ptr<arrow::DataType> scalar_vt_to_arrow(ValueType vt) {
  switch (vt) {
    case ValueType::BOOL:
      return arrow::boolean();
    case ValueType::INT32:
      return arrow::int32();
    case ValueType::INT64:
      return arrow::int64();
    case ValueType::FLOAT:
      return arrow::float32();
    case ValueType::DOUBLE:
      return arrow::float64();
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      return arrow::utf8();
    default:
      return nullptr;
  }
}

arrow::Result<std::shared_ptr<arrow::Field>> metadata_to_arrow_field(
    const FieldMetadata &metadata) {
  std::shared_ptr<arrow::DataType> type;

  if (metadata.type == ValueType::ARRAY) {
    const auto elem_dt = scalar_vt_to_arrow(metadata.element_type);
    if (!elem_dt) {
      return arrow::Status::NotImplemented(
          "Unsupported array element type: ",
          static_cast<int>(metadata.element_type));
    }
    if (metadata.fixed_size > 0) {
      type = arrow::fixed_size_list(arrow::field("item", elem_dt),
                                    static_cast<int32_t>(metadata.fixed_size));
    } else {
      type = arrow::list(arrow::field("item", elem_dt));
    }
  } else if (metadata.type == ValueType::MAP) {
    type = arrow::map(arrow::utf8(), map_union_value_type());
  } else {
    type = scalar_vt_to_arrow(metadata.type);
    if (!type) {
      return arrow::Status::NotImplemented("Unsupported ValueType: ",
                                           static_cast<int>(metadata.type));
    }
  }
  return arrow::field(metadata.name, type, metadata.nullable);
}

arrow::Result<SchemaMetadata> arrow_schema_to_metadata(
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

arrow::Result<std::shared_ptr<arrow::Schema>> metadata_to_arrow_schema(
    const SchemaMetadata &metadata) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto &field_metadata : metadata.fields) {
    ARROW_ASSIGN_OR_RAISE(auto field, metadata_to_arrow_field(field_metadata));
    fields.push_back(field);
  }
  return arrow::schema(fields);
}

MetadataManager::MetadataManager(const std::string &metadata_dir_path)
    : metadata_dir(metadata_dir_path) {}

arrow::Result<bool> MetadataManager::initialize() const {
  log_info("Initializing MetadataManager with directory: " + metadata_dir);

  try {
    if (!std::filesystem::exists(metadata_dir)) {
      log_info("Creating metadata directory: " + metadata_dir);
      if (!std::filesystem::create_directory(metadata_dir)) {
        return arrow::Status::IOError("Failed to create metadata directory: " +
                                      metadata_dir);
      }
    }

    std::string manifest_dir = metadata_dir + "/manifests";
    if (!std::filesystem::exists(manifest_dir)) {
      log_info("Creating manifests directory: " + manifest_dir);
      if (!std::filesystem::create_directory(manifest_dir)) {
        return arrow::Status::IOError("Failed to create manifests directory: " +
                                      manifest_dir);
      }
    }

    if (const std::string metadata_subdir = metadata_dir + "/metadata";
        !std::filesystem::exists(metadata_subdir)) {
      log_info("Creating metadata subdirectory: " + metadata_subdir);
      if (!std::filesystem::create_directory(metadata_subdir)) {
        return arrow::Status::IOError(
            "Failed to create metadata subdirectory: " + metadata_subdir);
      }
    }

    return true;
  } catch (const std::filesystem::filesystem_error &e) {
    log_error("Failed to initialize metadata directories: " +
              std::string(e.what()));
    return arrow::Status::IOError("Failed to initialize metadata directories: ",
                                  e.what());
  }
}

arrow::Result<std::string> MetadataManager::write_manifest(
    const Manifest &manifest) const {
  log_info("Writing manifest: " + manifest.id);
  try {
    std::string manifest_path =
        metadata_dir + "/manifests/" + manifest.id + ".manifest.json";
    if (const auto result = write_json_file(manifest, manifest_path);
        !result.ok()) {
      return result.status();
    }
    return manifest_path;
  } catch (const std::exception &e) {
    log_error("Failed to write manifest: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to write manifest: ", e.what());
  }
}

arrow::Result<Manifest> MetadataManager::read_manifest(
    const std::string &id) const {
  log_info("Reading manifest: " + id);
  try {
    const std::string manifest_path =
        metadata_dir + "/manifests/" + id + ".manifest.json";
    if (const std::ifstream file(manifest_path); !file.is_open()) {
      return arrow::Status::IOError("Failed to open manifest file: " +
                                    manifest_path);
    }

    return read_json_file<Manifest>(manifest_path);
  } catch (const std::exception &e) {
    log_error("Failed to read manifest: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to read manifest: ", e.what());
  }
}

arrow::Result<std::string> MetadataManager::write_metadata(
    const Metadata &metadata) const {
  log_info("Writing metadata");
  try {
    const std::string metadata_id = std::to_string(now_millis());
    std::string metadata_path =
        metadata_dir + "/metadata/" + metadata_id + ".metadata.json";

    if (const auto result = write_json_file(metadata, metadata_path);
        !result.ok()) {
      return result.status();
    }

    return metadata_path;
  } catch (const std::exception &e) {
    log_error("Failed to write metadata: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to write metadata: ", e.what());
  }
}

arrow::Result<Metadata> MetadataManager::read_metadata(
    const std::string &path) const {
  log_info("Reading metadata from: " + path);
  try {
    if (const std::ifstream file(path); !file.is_open()) {
      return arrow::Status::IOError("Failed to open metadata file: " + path);
    }
    return read_json_file<Metadata>(path);
  } catch (const std::exception &e) {
    log_error("Failed to read metadata: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to read metadata: ", e.what());
  }
}

arrow::Result<std::string> MetadataManager::write_db_info(
    const DatabaseInfo &db_info) const {
  log_info("Writing database info");
  try {
    std::string db_info_path = metadata_dir + "/db_info.json";
    if (const auto result = write_json_file(db_info, db_info_path);
        !result.ok()) {
      return result.status();
    }
    return db_info_path;
  } catch (const std::exception &e) {
    log_error("Failed to write database info: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to write database info: ", e.what());
  }
}

arrow::Result<DatabaseInfo> MetadataManager::read_db_info() const {
  log_info("Reading database info");
  try {
    const std::string db_info_path = metadata_dir + "/db_info.json";
    if (!std::filesystem::exists(db_info_path)) {
      log_info("Database info file does not exist at " + db_info_path +
               " - likely a new database");
      return DatabaseInfo{};
    }

    if (const std::ifstream file(db_info_path); !file.is_open()) {
      return arrow::Status::IOError(
          "Failed to open existing database info file: " + db_info_path);
    }

    return read_json_file<DatabaseInfo>(db_info_path);
  } catch (const std::exception &e) {
    log_error("Failed to read database info: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to read database info: ", e.what());
  }
}

arrow::Result<Metadata> MetadataManager::load_current_metadata() const {
  log_info("Loading current metadata");
  try {
    auto db_info_result = read_db_info();
    if (!db_info_result.ok()) {
      log_error("Failed to read database info: " +
                db_info_result.status().ToString());
      return db_info_result.status();
    }

    const auto &[metadata_location, timestamp_ms] = db_info_result.ValueOrDie();
    if (metadata_location.empty()) {
      log_info(
          "No metadata location in database info - starting with fresh "
          "metadata");
      return Metadata{};
    }

    auto metadata_result = read_metadata(metadata_location);
    if (!metadata_result.ok()) {
      log_error("Failed to read metadata: " +
                metadata_result.status().ToString());
      return metadata_result.status();
    }

    Metadata metadata = metadata_result.ValueOrDie();
    if (!metadata.snapshots.empty() &&
        metadata.get_current_snapshot() == nullptr) {
      metadata.current_snapshot_index = metadata.snapshots.size() - 1;
      log_info("Set current snapshot to: " +
               metadata.get_current_snapshot()->toString());
    }

    return metadata;
  } catch (const std::exception &e) {
    log_error("Failed to load current metadata: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to load current metadata: ",
                                  e.what());
  }
}

const std::string &MetadataManager::get_metadata_dir() const {
  return metadata_dir;
}
}  // namespace tundradb
