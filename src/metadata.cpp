#include "metadata.hpp"
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
            std::cout << "Initializing MetadataManager" << std::endl;
            std::filesystem::create_directories(this->metadata_dir);
            return true;
        } catch (const std::filesystem::filesystem_error &e) {
            return arrow::Status::IOError("Failed to create data directory: ",
                                          e.what());
        }
    }

    arrow::Result<bool> MetadataManager::write_metadata(const Metadata &metadata) {
     return  write_json_file<Metadata>(metadata, this->metadata_dir);

    }

    arrow::Result<std::string> MetadataManager::write_manifest(const Manifest &manifest) {
        std::string manifest_file_path =
                this->metadata_dir + "/" + manifest.id + ".manifest.json";
         write_json_file(manifest, manifest_file_path).ValueOrDie();

        return manifest_file_path;
    }


    arrow::Result<Manifest> MetadataManager::read_manifest(const std::string& file_path) {
           return read_json_file<Manifest>(file_path);
    }
}
