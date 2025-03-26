#include "metadata.hpp"
#include <arrow/api.h>
#include <nlohmann/json.hpp>
#include <fstream>

namespace tundradb {

   arrow::Result<bool> MetadataManager::initialize() {
        try {
            std::filesystem::create_directories(this->metadata_dir);
            return true;
        } catch (const std::filesystem::filesystem_error& e) {
            return arrow::Status::IOError("Failed to create data directory: ",
                                          e.what());
        }
    }

    MetadataManager::MetadataManager(const std::string &data_dir): data_dir(data_dir) {
        this->metadata_dir = data_dir + "/manifest";
    }

    arrow::Result<std::string> MetadataManager::write_metadata(const Metadata &metadata) {
       std::string file_path = this->metadata_dir + "/" + metadata.id + ".metadata.json";
       std::ofstream file(file_path);
       if (!file.is_open()) {
           return arrow::Status::IOError("Failed to open metadata file for writing: ",
                                         file_path);
       }
       nlohmann::json j = metadata;
       std::string json_str = j.dump();

       file << json_str;
       file.close();
       return file_path;
    }

    arrow::Result<Metadata> MetadataManager::read_metadata(const std::string &id) {
        std::string metadata_file_path = metadata_dir + "/" + id + ".metadata.json";
        std::ifstream file(metadata_file_path);
        if (!file.is_open()) {
            return arrow::Status::IOError("Failed to open metadata file for reading: ", metadata_file_path);
        }

        Metadata metadata;
        try {
            nlohmann::json j = nlohmann::json::parse(file);
            metadata = j.get<Metadata>();
        } catch (const std::exception &e) {
            return arrow::Status::Invalid("Failed to parse JSON metadata: ", e.what());
        }
        return metadata;
    }

    arrow::Result<std::string> MetadataManager::write_manifest(const Manifest &manifest) {
        std::string manifest_file_path =
                this->metadata_dir + "/" + manifest.id + ".manifest.json";

        std::ofstream file(manifest_file_path);
        if (!file.is_open()) {
            return arrow::Status::IOError("Failed to open metadata file for writing: ",
                                          manifest_file_path);
        }
        nlohmann::json j = manifest;
        std::string json_str = j.dump();

        file << json_str;
        file.close();
        return manifest_file_path;
    }


    arrow::Result<Manifest> MetadataManager::read_manifest(const std::string &id) {
        std::string manifest_file_path =
                this->metadata_dir + "/" + id + ".manifest.json";
        std::ifstream file(manifest_file_path);
        if (!file.is_open()) {
            return arrow::Status::IOError("Failed to open metadata file for reading: ", manifest_file_path);
        }

        Manifest manifest;
        try {
            nlohmann::json j = nlohmann::json::parse(file);
            manifest = j.get<Manifest>();
        } catch (const std::exception &e) {
            return arrow::Status::Invalid("Failed to parse JSON manifest: ", e.what());
        }
        return manifest;
    }
}
