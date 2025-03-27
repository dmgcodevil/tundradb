#include "snapshot.hpp"
#include "metadata.hpp"
#include "core.hpp"
#include "utils.hpp"

namespace tundradb {
    std::shared_ptr<MetadataManager> metadata_manager;
    std::shared_ptr<Storage> storage;
    std::shared_ptr<ShardManager> shard_manager;

    SnapshotManager::SnapshotManager(std::shared_ptr<MetadataManager> metadata_manager,
                                     std::shared_ptr<Storage> storage,
                                     std::shared_ptr<ShardManager> shard_manager
    ): metadata_manager(move(metadata_manager)), storage(move(storage)), shard_manager(move(shard_manager)) {
    }

    arrow::Result<bool> SnapshotManager::initialize() {
        try {
            auto metadata = tundradb::metadata_manager->load_metadata().ValueOrDie();
            if (!metadata.snapshot_location.empty()) {
                this->snapshot = std::make_shared<Snapshot>(
                    read_json_file<Snapshot>(metadata.snapshot_location).ValueOrDie());
                Manifest manifest = read_json_file<Manifest>(this->snapshot->manifest_location).ValueOrDie();
                // load shards data
                std::unordered_map<std::string, std::vector<ShardMetadata> > grouped_shards;

                for (auto shard: manifest.shards) {
                    grouped_shards[shard.schema_name].push_back(shard);
                }

                for (auto &[_, v]: grouped_shards) {
                    std::sort(v.begin(), v.end(), [](const ShardMetadata &a, const ShardMetadata &b) {
                        return a.id < b.id;
                    });
                }
                for (auto &[schema_name, shards]: grouped_shards) {
                    for (auto shard_metadata: shards) {
                        auto shard = this->storage->read_shard(shard_metadata).ValueOrDie();
                        this->shard_manager->add_shard(shard).ValueOrDie();
                    }
                }
            } else {
                std::cout << "no snapshots exist";
            }
            return true;
        } catch (const std::filesystem::filesystem_error &e) {
            return arrow::Status::IOError("Failed to create data directory: ",
                                          e.what());
        }
    }

    arrow::Result<std::shared_ptr<Snapshot> > SnapshotManager::commit() {
        std::cout << "creating snapshot" << std::endl;
        auto timestamp_ms = now_millis();
        Snapshot snapshot;
        snapshot.id = timestamp_ms;
        snapshot.timestamp_ms = timestamp_ms;
        Manifest current_manifest;
        if (this->snapshot != nullptr) {
            snapshot.parent_id = this->snapshot->id;
            snapshot.snapshots.push_back(*this->snapshot);
            snapshot.snapshots.insert(snapshot.snapshots.end(),
                                      this->snapshot->snapshots.begin(), this->snapshot->snapshots.end());
            current_manifest = read_json_file<Manifest>(this->snapshot->manifest_location).ValueOrDie();
        }
        std::unordered_map<std::string, std::unordered_map<int64_t, ShardMetadata> > curr_shard_metadata;
        for (const auto &shard: current_manifest.shards) {
            curr_shard_metadata[shard.compound_id()][shard.id]= shard;
        }
        Manifest new_manifest;
        new_manifest.id = generate_uuid();

        for (const auto &schema_name: tundradb::shard_manager->get_schema_names()) {
            for (const auto &shard: tundradb::shard_manager->get_shards(schema_name).ValueOrDie()) {
                if (shard->is_updated()) {
                    ShardMetadata shard_metadata;
                    shard_metadata.id = shard->id;
                    shard_metadata.schema_name = schema_name;
                    shard_metadata.timestamp_ms = shard->get_updated_ts();
                    shard_metadata.min_id = shard->min_id;
                    shard_metadata.max_id = shard->max_id;
                    shard_metadata.record_count = shard->size();
                    shard_metadata.chunk_size = shard->chunk_size;
                    shard_metadata.data_file = this->storage->write_shard(shard).ValueOrDie();
                    new_manifest.shards.push_back(shard_metadata);
                } else if (curr_shard_metadata.contains(shard->schema_name) &&
                           curr_shard_metadata[shard->schema_name].contains(shard->id)) {
                    new_manifest.shards.push_back(curr_shard_metadata[shard->schema_name][shard->id]);
                }
            }
        }
        snapshot.manifest_location = this->metadata_manager->write_manifest(new_manifest).ValueOrDie();
        this->snapshot = std::make_shared<Snapshot>(std::move(snapshot));
        auto snap_id = generate_uuid();
        auto snapshot_location = this->metadata_manager->get_metadata_dir() + "/" + snap_id + ".metadata.json";
        write_json_file(snapshot,
                        snapshot_location).ValueOrDie();
        Metadata new_metadata;
        new_metadata.snapshot_location = snapshot_location;
        this->metadata_manager->write_metadata(new_metadata).ValueOrDie();
        return this->snapshot;
    }

    std::shared_ptr<Snapshot> SnapshotManager::current_snapshot() {
        return this->snapshot;
    }
}
