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
            // Load current metadata using the new loadMetadata method
            auto metadata_result = this->metadata_manager->load_current_metadata();
            if (!metadata_result.ok()) {
                log_error("Failed to load metadata: " + metadata_result.status().ToString());
                return metadata_result.status();  // Return the error instead of continuing
            }

            this->metadata = metadata_result.ValueOrDie();
            log_info("Metadata loaded: " + this->metadata.toString());

            // Check if we have a current snapshot
            if (this->metadata.current_snapshot != nullptr) {
                this->snapshot = this->metadata.current_snapshot;
                log_info("Current snapshot loaded: " + this->snapshot->toString());

                // Load the manifest to initialize shards
                auto manifest_result = read_json_file<Manifest>(this->snapshot->manifest_location);
                if (!manifest_result.ok()) {
                    log_error("Failed to load manifest: " + manifest_result.status().ToString());
                    return manifest_result.status();  // Return the error instead of continuing
                }
                
                Manifest manifest = manifest_result.ValueOrDie();
                log_info("Manifest loaded. shards count=" + std::to_string(manifest.shards.size()) + 
                         ", id=" + manifest.id);

                // Group shards by schema name
                std::unordered_map<std::string, std::vector<ShardMetadata>> grouped_shards;
                for (auto& shard : manifest.shards) {
                    grouped_shards[shard.schema_name].push_back(shard);
                }

                // Sort shards by ID for each schema
                for (auto &[_, v]: grouped_shards) {
                    std::sort(v.begin(), v.end(), [](const ShardMetadata &a, const ShardMetadata &b) {
                        return a.id < b.id;
                    });
                }

                log_info("Grouped shards: " + std::to_string(grouped_shards.size()));
                
                // Load and add each shard
                for (auto &[schema_name, shards]: grouped_shards) {
                    for (auto& shard_metadata: shards) {
                        auto shard_result = this->storage->read_shard(shard_metadata);
                        if (!shard_result.ok()) {
                            log_error("Failed to load shard: " + shard_result.status().ToString());
                            return shard_result.status();  // Return the error instead of continuing
                        }
                        
                        auto shard = shard_result.ValueOrDie();
                        log_debug("Adding shard from snapshot: " + shard_metadata.toString());
                        shard->set_updated(false);
                        
                        auto add_result = this->shard_manager->add_shard(shard);
                        if (!add_result.ok()) {
                            log_error("Failed to add shard: " + add_result.status().ToString());
                            return add_result.status();  // Return the error instead of continuing
                        }
                    }
                }
            } else {
                log_info("No current snapshot exists");
            }
            return true;
        } catch (const std::exception &e) {
            log_error("Failed to initialize snapshot manager: " + std::string(e.what()));
            return arrow::Status::IOError("Failed to initialize snapshot manager: ", e.what());
        }
    }

    arrow::Result<Snapshot*> SnapshotManager::commit() {
        log_info("Creating new snapshot");
        auto timestamp_ms = now_millis();
        
        // Create new snapshot
        Snapshot new_snapshot;
        new_snapshot.id = timestamp_ms;
        new_snapshot.timestamp_ms = timestamp_ms;
        
        // Set parent ID if previous snapshot exists
        if (this->snapshot != nullptr) {
            new_snapshot.parent_id = this->snapshot->id;
        }
        
        // Create new manifest or load existing one
        Manifest current_manifest;
        if (this->snapshot != nullptr) {
            current_manifest = read_json_file<Manifest>(this->snapshot->manifest_location).ValueOrDie();
        }
        
        // Track shard metadata from current manifest
        std::unordered_map<std::string, std::unordered_map<int64_t, ShardMetadata>> curr_shard_metadata;
        for (const auto &shard: current_manifest.shards) {
            curr_shard_metadata[shard.schema_name][shard.id] = shard;
        }
        
        // Create new manifest
        Manifest new_manifest;
        new_manifest.id = generate_uuid();

        // Go through all schemas and shards
        for (const auto &schema_name: this->shard_manager->get_schema_names()) {
            log_info("Writing shards for schema: " + schema_name);
            for (const auto &shard: this->shard_manager->get_shards(schema_name).ValueOrDie()) {
                log_debug("Snapshotting shard: " + std::to_string(shard->id));
                
                // Only write updated shards, reuse unchanged ones
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
        
        // Save the manifest
        new_snapshot.manifest_location = this->metadata_manager->write_manifest(new_manifest).ValueOrDie();
        log_info("Created manifest: " + new_manifest.id + " at " + new_snapshot.manifest_location);
        
        // Add the new snapshot to metadata
        this->metadata.snapshots.push_back(new_snapshot);
        
        // Set the current snapshot to the newly created one
        this->snapshot = &this->metadata.snapshots.back();
        this->metadata.current_snapshot = this->snapshot;
        
        // Save the updated metadata
        std::string metadata_location = this->metadata_manager->write_metadata(this->metadata).ValueOrDie();
        log_info("Saved metadata to: " + metadata_location);
        
        // Update database info to point to the new metadata location
        DatabaseInfo db_info;
        db_info.metadata_location = metadata_location;
        db_info.timestamp_ms = timestamp_ms;
        this->metadata_manager->write_db_info(db_info).ValueOrDie();
        log_info("Updated database info to point to new metadata");
        
        // Reset all updated flags
        auto reset_result = shard_manager->reset_all_updated();
        if (!reset_result.ok()) {
            log_warn("Failed to reset shard updated flags: " + reset_result.status().ToString());
        }
        
        return this->snapshot;
    }

    Snapshot* SnapshotManager::current_snapshot() {
        return this->snapshot;
    }
}
