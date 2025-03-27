//
// Created by Roman Pliashkou on 3/26/25.
//

#ifndef SNAPSHOT_HPP
#define SNAPSHOT_HPP
#include "metadata.hpp"
#include "storage.hpp"


namespace tundradb {
    class ShardManager;
    class MetadataManager;
    class Storage;
    struct Snapshot {
        int64_t id;
        int64_t parent_id;
        std::string manifest_location;
        std::vector<Snapshot> snapshots;
        int64_t timestamp_ms;
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(Snapshot,
                                       id,
                                       parent_id,
                                       manifest_location,
                                       snapshots, timestamp_ms);

        std::string toString() const {
            std::stringstream ss;
            ss << "Snapshot{id='" << id
                    << "', parent_id=" << parent_id
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

        friend std::ostream &operator<<(std::ostream &os, const Snapshot &snapshot) {
            os << snapshot.toString();
            return os;
        }
    };

    class SnapshotManager {
    public:
        explicit SnapshotManager(std::shared_ptr<MetadataManager> metadata_manager,
                                 std::shared_ptr<Storage> storage,
                                 std::shared_ptr<ShardManager> shard_manager);

        arrow::Result<bool> initialize();

        arrow::Result<std::shared_ptr<Snapshot>> commit();

        std::shared_ptr<Snapshot> current_snapshot();

    private:
        std::shared_ptr<Snapshot> snapshot;
        std::shared_ptr<MetadataManager> metadata_manager;
        std::shared_ptr<Storage> storage;
        std::shared_ptr<ShardManager> shard_manager;
    };
};


#endif //SNAPSHOT_HPP
