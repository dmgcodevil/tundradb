#include "metadata.hpp"
#include "logger.hpp"
#include <arrow/api.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>

namespace tundradb {

    MetadataManager::MetadataManager(const std::string &data_dir): data_dir(data_dir) {
        this->metadata_dir = data_dir + "/metadata";
    }

    arrow::Result<bool> MetadataManager::initialize() {
        try {
            log_info("Initializing MetadataManager with directory: " + this->metadata_dir);
            std::filesystem::create_directories(this->metadata_dir);
            return true;
        } catch (const std::filesystem::filesystem_error &e) {
            log_error("Failed to create metadata directory: " + std::string(e.what()));
            return arrow::Status::IOError("Failed to create metadata directory: ", e.what());
        }
    }

    arrow::Result<bool> MetadataManager::write_metadata(const Metadata &metadata) {
        std::string metadata_file_path = this->metadata_dir + "/metadata.json";
        log_info("Writing metadata to: " + metadata_file_path);
        return write_json_file<Metadata>(metadata, metadata_file_path);
    }

    arrow::Result<std::string> MetadataManager::write_manifest(const Manifest &manifest) {
        std::string manifest_file_path =
                this->metadata_dir + "/" + manifest.id + ".manifest.json";
        log_info("Writing manifest to: " + manifest_file_path);
        write_json_file(manifest, manifest_file_path).ValueOrDie();

        return manifest_file_path;
    }


    arrow::Result<Manifest> MetadataManager::read_manifest(const std::string& file_path) {
        log_info("Reading manifest from: " + file_path);
        return read_json_file<Manifest>(file_path);
    }
}
