#include "metadata.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <arrow/api.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <string>
#include <unordered_map>

namespace tundradb {

    MetadataManager::MetadataManager(const std::string &metadata_dir_path) : metadata_dir(metadata_dir_path) {
    }

    arrow::Result<bool> MetadataManager::initialize() {
        log_info("Initializing MetadataManager with directory: " + metadata_dir);
        
        try {
            if (!std::filesystem::exists(metadata_dir)) {
                log_info("Creating metadata directory: " + metadata_dir);
                if (!std::filesystem::create_directory(metadata_dir)) {
                    return arrow::Status::IOError("Failed to create metadata directory: " + metadata_dir);
                }
            }
            
            // Create necessary subdirectories
            std::string manifest_dir = metadata_dir + "/manifests";
            if (!std::filesystem::exists(manifest_dir)) {
                log_info("Creating manifests directory: " + manifest_dir);
                if (!std::filesystem::create_directory(manifest_dir)) {
                    return arrow::Status::IOError("Failed to create manifests directory: " + manifest_dir);
                }
            }
            
            std::string metadata_subdir = metadata_dir + "/metadata";
            if (!std::filesystem::exists(metadata_subdir)) {
                log_info("Creating metadata subdirectory: " + metadata_subdir);
                if (!std::filesystem::create_directory(metadata_subdir)) {
                    return arrow::Status::IOError("Failed to create metadata subdirectory: " + metadata_subdir);
                }
            }
            
            return true;
        } catch (const std::filesystem::filesystem_error &e) {
            log_error("Failed to initialize metadata directories: " + std::string(e.what()));
            return arrow::Status::IOError("Failed to initialize metadata directories: ", e.what());
        }
    }

    arrow::Result<std::string> MetadataManager::write_manifest(const Manifest &manifest) {
        log_info("Writing manifest: " + manifest.id);
        try {
            std::string manifest_path = metadata_dir + "/manifests/" + manifest.id + ".manifest.json";
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
            std::string manifest_path = metadata_dir + "/manifests/" + id + ".manifest.json";
            std::ifstream file(manifest_path);
            if (!file.is_open()) {
                return arrow::Status::IOError("Failed to open manifest file: " + manifest_path);
            }
            
            return read_json_file<Manifest>(manifest_path);
        } catch (const std::exception &e) {
            log_error("Failed to read manifest: " + std::string(e.what()));
            return arrow::Status::IOError("Failed to read manifest: ", e.what());
        }
    }

    arrow::Result<std::string> MetadataManager::write_metadata(const Metadata &metadata) {
        log_info("Writing metadata");
        try {
            // Generate a unique filename for the metadata using timestamp
            std::string metadata_id = std::to_string(now_millis());
            std::string metadata_path = metadata_dir + "/metadata/" + metadata_id + ".metadata.json";
            
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

    arrow::Result<Metadata> MetadataManager::read_metadata(const std::string &path) {
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

    arrow::Result<std::string> MetadataManager::write_db_info(const DatabaseInfo &db_info) {
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
            std::ifstream file(db_info_path);
            if (!file.is_open()) {
                log_warn("Database info file does not exist: " + db_info_path);
                return DatabaseInfo{}; // Return empty database info if file doesn't exist
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
                log_warn("Failed to read database info: " + db_info_result.status().ToString());
                return Metadata{}; // Return empty metadata if database info doesn't exist
            }
            
            DatabaseInfo db_info = db_info_result.ValueOrDie();
            if (db_info.metadata_location.empty()) {
                log_warn("No metadata location in database info");
                return Metadata{}; // Return empty metadata if metadata location doesn't exist
            }
            
            // Read metadata from the location specified in database info
            auto metadata_result = read_metadata(db_info.metadata_location);
            if (!metadata_result.ok()) {
                log_error("Failed to read metadata: " + metadata_result.status().ToString());
                return arrow::Status::IOError("Failed to read metadata: " + metadata_result.status().ToString());
            }
            
            Metadata metadata = metadata_result.ValueOrDie();
            
            // Set current_snapshot pointer if snapshots exist
            if (!metadata.snapshots.empty() && metadata.current_snapshot == nullptr) {
                metadata.current_snapshot = &metadata.snapshots.back();
                log_info("Set current snapshot to latest snapshot: " + metadata.current_snapshot->toString());
            }
            
            return metadata;
        } catch (const std::exception &e) {
            log_error("Failed to load current metadata: " + std::string(e.what()));
            return arrow::Status::IOError("Failed to load current metadata: ", e.what());
        }
    }

    std::string MetadataManager::get_metadata_dir() const {
        return metadata_dir;
    }
}
