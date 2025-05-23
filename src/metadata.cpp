#include "metadata.hpp"

#include <arrow/api.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include "../libs/json/json.hpp"
#include <string>

#include "logger.hpp"
#include "utils.hpp"

namespace tundradb {

MetadataManager::MetadataManager(const std::string &metadata_dir_path)
    : metadata_dir(metadata_dir_path) {}

arrow::Result<bool> MetadataManager::initialize() {
  log_info("Initializing MetadataManager with directory: " + metadata_dir);

  try {
    if (!std::filesystem::exists(metadata_dir)) {
      log_info("Creating metadata directory: " + metadata_dir);
      if (!std::filesystem::create_directory(metadata_dir)) {
        return arrow::Status::IOError("Failed to create metadata directory: " +
                                      metadata_dir);
      }
    }

    // Create necessary subdirectories
    std::string manifest_dir = metadata_dir + "/manifests";
    if (!std::filesystem::exists(manifest_dir)) {
      log_info("Creating manifests directory: " + manifest_dir);
      if (!std::filesystem::create_directory(manifest_dir)) {
        return arrow::Status::IOError("Failed to create manifests directory: " +
                                      manifest_dir);
      }
    }

    std::string metadata_subdir = metadata_dir + "/metadata";
    if (!std::filesystem::exists(metadata_subdir)) {
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
    const Manifest &manifest) {
  log_info("Writing manifest: " + manifest.id);
  try {
    std::string manifest_path =
        metadata_dir + "/manifests/" + manifest.id + ".manifest.json";
    auto result = write_json_file(manifest, manifest_path);
    if (!result.ok()) {
      return result.status();
    }
    return manifest_path;
  } catch (const std::exception &e) {
    log_error("Failed to write manifest: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to write manifest: ", e.what());
  }
}

arrow::Result<Manifest> MetadataManager::read_manifest(const std::string &id) {
  log_info("Reading manifest: " + id);
  try {
    std::string manifest_path =
        metadata_dir + "/manifests/" + id + ".manifest.json";
    std::ifstream file(manifest_path);
    if (!file.is_open()) {
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
    const Metadata &metadata) {
  log_info("Writing metadata");
  try {
    // Generate a unique filename for the metadata using timestamp
    std::string metadata_id = std::to_string(now_millis());
    std::string metadata_path =
        metadata_dir + "/metadata/" + metadata_id + ".metadata.json";

    auto result = write_json_file(metadata, metadata_path);
    if (!result.ok()) {
      return result.status();
    }

    return metadata_path;
  } catch (const std::exception &e) {
    log_error("Failed to write metadata: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to write metadata: ", e.what());
  }
}

arrow::Result<Metadata> MetadataManager::read_metadata(
    const std::string &path) {
  log_info("Reading metadata from: " + path);
  try {
    std::ifstream file(path);
    if (!file.is_open()) {
      return arrow::Status::IOError("Failed to open metadata file: " + path);
    }

    return read_json_file<Metadata>(path);
  } catch (const std::exception &e) {
    log_error("Failed to read metadata: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to read metadata: ", e.what());
  }
}

arrow::Result<std::string> MetadataManager::write_db_info(
    const DatabaseInfo &db_info) {
  log_info("Writing database info");
  try {
    std::string db_info_path = metadata_dir + "/db_info.json";
    auto result = write_json_file(db_info, db_info_path);
    if (!result.ok()) {
      return result.status();
    }
    return db_info_path;
  } catch (const std::exception &e) {
    log_error("Failed to write database info: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to write database info: ", e.what());
  }
}

arrow::Result<DatabaseInfo> MetadataManager::read_db_info() {
  log_info("Reading database info");
  try {
    std::string db_info_path = metadata_dir + "/db_info.json";

    // For a new database, it's ok if the file doesn't exist
    if (!std::filesystem::exists(db_info_path)) {
      log_info("Database info file does not exist at " + db_info_path +
               " - likely a new database");
      return DatabaseInfo{};  // Return empty database info for a new database
    }

    std::ifstream file(db_info_path);
    if (!file.is_open()) {
      // If the file exists but can't be opened, that's a critical error
      return arrow::Status::IOError(
          "Failed to open existing database info file: " + db_info_path);
    }

    return read_json_file<DatabaseInfo>(db_info_path);
  } catch (const std::exception &e) {
    log_error("Failed to read database info: " + std::string(e.what()));
    return arrow::Status::IOError("Failed to read database info: ", e.what());
  }
}

arrow::Result<Metadata> MetadataManager::load_current_metadata() {
  log_info("Loading current metadata");
  try {
    // Read database info to get the metadata location
    auto db_info_result = read_db_info();
    if (!db_info_result.ok()) {
      // This is a critical error - we need the database info
      log_error("Failed to read database info: " +
                db_info_result.status().ToString());
      return db_info_result.status();
    }

    const DatabaseInfo &db_info = db_info_result.ValueOrDie();
    if (db_info.metadata_location.empty()) {
      log_info(
          "No metadata location in database info - starting with fresh "
          "metadata");
      return Metadata{};  // This is acceptable for a new database
    }

    // Read metadata from the location specified in database info
    auto metadata_result = read_metadata(db_info.metadata_location);
    if (!metadata_result.ok()) {
      // This is a critical error - if we have a metadata location, we should be
      // able to read it
      log_error("Failed to read metadata: " +
                metadata_result.status().ToString());
      return metadata_result.status();
    }

    Metadata metadata = metadata_result.ValueOrDie();

    // Set current_snapshot pointer if snapshots exist
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
