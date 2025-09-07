#include "metadata.hpp"

#include <arrow/api.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "../libs/json/json.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace tundradb {

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
