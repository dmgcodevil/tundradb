#include "core.hpp"
#include "metadata.hpp"
#include "utils.hpp"
#include "logger.hpp"

namespace tundradb {
    // Remove these global variables as they conflict with instance members
    // std::shared_ptr<MetadataManager> metadata_manager;
    // std::shared_ptr<Storage> storage;
    // std::shared_ptr<ShardManager> shard_manager;

    SnapshotManager::SnapshotManager(std::shared_ptr<MetadataManager> metadata_manager,
                                     std::shared_ptr<Storage> storage,
                                     std::shared_ptr<ShardManager> shard_manager
    ): metadata_manager(std::move(metadata_manager)), storage(std::move(storage)), shard_manager(std::move(shard_manager)) {
    }

    arrow::Result<bool> SnapshotManager::initialize() {
        log_info("Initializing snapshot manager...");
        try {
            // Fix: Use the instance member instead of the global variable
            auto metadata = this->metadata_manager->load_metadata().ValueOrDie();
            log_info("Metadata loaded.");
            if (!metadata.snapshot_location.empty()) {
                this->snapshot = std::make_shared<Snapshot>(
                    read_json_file<Snapshot>(metadata.snapshot_location).ValueOrDie());
                log_info("Snapshot loaded.");
                Manifest manifest = read_json_file<Manifest>(this->snapshot->manifest_location).ValueOrDie();
                log_info("Manifest loaded. shards count=" + std::to_string(manifest.shards.size()) + 
                         ", id=" + manifest.id);
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
                log_info("Grouped shards: " + std::to_string(grouped_shards.size()));
                for (auto &[schema_name, shards]: grouped_shards) {
                    for (auto shard_metadata: shards) {
                        auto shard = this->storage->read_shard(shard_metadata).ValueOrDie();
                        log_debug("Adding shard from snapshot");
                        shard->set_updated(false);
                        this->shard_manager->add_shard(shard).ValueOrDie();
                    }
                }
            } else {
                log_info("No snapshots exist");
            }
            return true;
        } catch (const std::filesystem::filesystem_error &e) {
            log_error("Failed to create data directory: " + std::string(e.what()));
            return arrow::Status::IOError("Failed to create data directory: ",
                                          e.what());
        }
    }

    arrow::Result<std::shared_ptr<Snapshot> > SnapshotManager::commit() {
        log_info("Creating snapshot");
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
            curr_shard_metadata[shard.schema_name][shard.id]= shard;
        }
        Manifest new_manifest;
        new_manifest.id = generate_uuid();

        for (const auto &schema_name: this->shard_manager->get_schema_names()) {
            log_info("Writing shards for schema: " + schema_name);
            for (const auto &shard: this->shard_manager->get_shards(schema_name).ValueOrDie()) {
                log_debug("Snapshotting shard: " + std::to_string(shard->id));
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
        log_info("Writing snapshot: " + snapshot.toString());
        auto snap_id = generate_uuid();
        auto snapshot_location = this->metadata_manager->get_metadata_dir() + "/" + snap_id + ".metadata.json";
        write_json_file(snapshot,
                        snapshot_location).ValueOrDie();
        this->snapshot = std::make_shared<Snapshot>(std::move(snapshot));
        Metadata new_metadata;
        new_metadata.snapshot_location = snapshot_location;
        this->metadata_manager->write_metadata(new_metadata).ValueOrDie();
        shard_manager->reset_all_updated();
        return this->snapshot;
    }

    std::shared_ptr<Snapshot> SnapshotManager::current_snapshot() {
        return this->snapshot;
    }
}
