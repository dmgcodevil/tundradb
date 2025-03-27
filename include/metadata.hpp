#ifndef METADATA_HPP
#define METADATA_HPP

#include <string>
#include <vector>
#include <sstream>
#include <nlohmann/json.hpp>
#include <arrow/result.h>
#include <arrow/api.h>
#include <filesystem>
#include "file_utils.hpp"
#include "logger.hpp"

using namespace std::string_literals;

namespace tundradb {
    struct Metadata {
        std::string snapshot_location;
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(Metadata, snapshot_location);
    };


    struct ShardMetadata {
        int64_t id;
        std::string schema_name;
        int64_t min_id;
        int64_t max_id;
        size_t record_count;
        size_t chunk_size;
        std::string data_file;
        int64_t timestamp_ms; // Time in milliseconds since epoch

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(ShardMetadata,
                                       id,
                                       schema_name,
                                       min_id,
                                       max_id,
                                       record_count,
                                       chunk_size,
                                       data_file,
                                       timestamp_ms);

        [[nodiscard]] std::string toString() const {
            std::stringstream ss;
            ss << "ShardMetadata{id='" << id
                    << "', schema_name='" << schema_name
                    << "', min_id=" << min_id
                    << ", max_id=" << max_id
                    << ", record_count=" << record_count
                    << ", chunk_size=" << chunk_size
                    << ", data_file='" << data_file
                    << "', timestamp_ms=" << timestamp_ms << "}";
            return ss.str();
        }

        friend std::ostream &operator<<(std::ostream &os, const ShardMetadata &shard) {
            os << shard.toString();
            return os;
        }

        // std::string compound_id() const {
        //     return this->schema_name + "-" + std::to_string(this->id);
        // }
    };

    struct Manifest {
        std::string id;
        std::vector<ShardMetadata> shards;
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
        std::string data_dir;

        arrow::Result<std::string> write_manifest(const Manifest &manifest);

        arrow::Result<bool> write_metadata(const Metadata &manifest);

        arrow::Result<Manifest> read_manifest(const std::string &file_path);

        explicit MetadataManager(const std::string &data_dir);

        arrow::Result<bool> initialize();

        arrow::Result<Metadata> load_metadata() {
            log_info("Loading metadata");
            auto file_path = this->metadata_dir + "/metadata.json";
            log_info("Loading metadata from " + file_path);
            if (file_exists(file_path)) {
                return read_json_file<Metadata>(file_path);
            }
            log_info("File '" + file_path + "' doesn't exist. Creating empty metadata");
            Metadata metadata;
            metadata.snapshot_location = ""s;
            ARROW_RETURN_NOT_OK(write_json_file(metadata, file_path));
            return metadata;
        }

        std::string get_metadata_dir() {
            return this->metadata_dir;
        }

    private:
        std::string metadata_dir;
    };
}

#endif //METADATA_HPP
