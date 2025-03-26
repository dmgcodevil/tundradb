#include <nlohmann/json.hpp>
#include <arrow/api.h>
#include <sstream>
#include <string>
#include <vector>

#ifndef METADATA_HPP
#define METADATA_HPP

namespace tundradb {
    struct ShardMetadata {
        std::string shard_id;
        std::string schema_name;
        int64_t min_id;
        int64_t max_id;
        size_t record_count;
        size_t chunk_size;
        std::string data_file;
        int64_t timestamp_ms; // Time in milliseconds since epoch

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(ShardMetadata,
                                       shard_id,
                                       schema_name,
                                       min_id,
                                       max_id,
                                       record_count,
                                       chunk_size,
                                       data_file,
                                       timestamp_ms);

        std::string toString() const {
            std::stringstream ss;
            ss << "ShardMetadata{shard_id='" << shard_id 
               << "', schema_name='" << schema_name 
               << "', min_id=" << min_id 
               << ", max_id=" << max_id 
               << ", record_count=" << record_count 
               << ", chunk_size=" << chunk_size 
               << ", data_file='" << data_file 
               << "', timestamp_ms=" << timestamp_ms << "}";
            return ss.str();
        }

        friend std::ostream& operator<<(std::ostream& os, const ShardMetadata& shard) {
            os << shard.toString();
            return os;
        }
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

        friend std::ostream& operator<<(std::ostream& os, const Manifest& manifest) {
            os << manifest.toString();
            return os;
        }
    };

    struct Metadata {
        std::string id;
        int64_t snapshot_id;
        int64_t parent_snapshot_id;
        std::string manifest_location;
        std::vector<Metadata> snapshots;
        int64_t timestamp_ms;
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(Metadata,
            id,
            snapshot_id,
            parent_snapshot_id,
            manifest_location,
            snapshots, timestamp_ms);

        std::string toString() const {
            std::stringstream ss;
            ss << "Metadata{id='" << id 
               << "', snapshot_id=" << snapshot_id 
               << ", snapshot_parent_id=" << parent_snapshot_id 
               << ", manifest_location='" << manifest_location 
               << "', timestamp_ms=" << timestamp_ms 
               << ", snapshots=[";
            for (size_t i = 0; i < snapshots.size(); ++i) {
                ss << snapshots[i];
                if (i < snapshots.size() - 1) ss << ", ";
            }
            ss << "]}";
            return ss.str();
        }

        friend std::ostream& operator<<(std::ostream& os, const Metadata& metadata) {
            os << metadata.toString();
            return os;
        }
    };

    class MetadataManager {
        public:
        std::string data_dir;
        arrow::Result<std::string> write_manifest(const Manifest &manifest);
        arrow::Result<std::string> write_metadata(const Metadata &metadata);
        arrow::Result<Manifest> read_manifest(const std::string &id);
        arrow::Result<Metadata> read_metadata(const std::string &id);
        explicit MetadataManager(const std::string &data_dir);
        arrow::Result<bool> initialize();
        private:
        std::string metadata_dir;

    };
}

#endif //METADATA_HPP
