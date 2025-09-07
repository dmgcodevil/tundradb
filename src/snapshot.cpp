#include "core.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "utils.hpp"

namespace tundradb {

arrow::Result<bool> SnapshotManager::initialize() {
  log_info("Initializing snapshot manager...");
  try {
    ARROW_ASSIGN_OR_RAISE(this->metadata_,
                          metadata_manager_->load_current_metadata());
    log_info("Metadata has been loaded.");
    log_info("Metadata current snapshot index=" +
             std::to_string(this->metadata_.current_snapshot_index));
    log_info("Metadata current snapshots size=" +
             std::to_string(this->metadata_.snapshots.size()));

    // Set current snapshot if it exists
    if (this->metadata_.get_current_snapshot() != nullptr) {
      log_info("Current snapshot id = " +
               std::to_string(this->metadata_.get_current_snapshot()->id));
      log_info("Initializing schemas");
      for (const auto &schema_metadata : this->metadata_.schemas) {
        log_info("Loading schema " + schema_metadata.name);
        auto arrow_schema_result = metadata_to_arrow_schema(schema_metadata);
        if (!arrow_schema_result.ok()) {
          log_error("Failed to load schema: " +
                    arrow_schema_result.status().ToString());
        }
        const auto &arrow_shema = arrow_schema_result.ValueOrDie();
        auto add_schema_result =
            schema_registry_->add_arrow(schema_metadata.name, arrow_shema);
        if (!add_schema_result.ok()) {
          log_error("Failed to add schema to registry: " +
                    add_schema_result.status().ToString());
          return arrow_schema_result.status();
        }
      }
      log_info("Load the manifest and initialize shards");
      auto manifest_result = read_json_file<Manifest>(
          this->metadata_.get_current_snapshot()->manifest_location);
      if (!manifest_result.ok()) {
        log_error("Failed to load manifest: " +
                  manifest_result.status().ToString());
        return manifest_result.status();
      }
      this->manifest_ =
          std::make_shared<Manifest>(manifest_result.ValueOrDie());
      log_info("Manifest has been loaded");
      log_info(this->manifest_->toString());

      edge_store_->set_id_seq(manifest_->edge_id_seq);
      node_manager_->set_id_counter(manifest_->node_id_seq);
      shard_manager_->set_id_counter(manifest_->shard_id_seq);

      // Restore index counters for each schema
      std::unordered_map<std::string, int64_t> max_index_per_schema;
      for (const auto &shard : this->manifest_->shards) {
        auto &max_index = max_index_per_schema[shard.schema_name];
        max_index =
            std::max(max_index,
                     shard.index + 1);  // Add 1 so next index is after highest
      }

      // Set the index counters based on max values
      for (const auto &[schema_name, max_index] : max_index_per_schema) {
        shard_manager_->set_index_counter(schema_name, max_index);
        log_debug("Set shard index counter for schema '" + schema_name +
                  "' to " + std::to_string(max_index));
      }

      std::unordered_map<std::string, std::vector<ShardMetadata>>
          grouped_shards;
      for (auto &shard : manifest_result.ValueOrDie().shards) {
        grouped_shards[shard.schema_name].push_back(shard);
      }

      // Sort shards by index for each schema to ensure consistent ordering
      // during restoration
      for (auto &v : grouped_shards | std::views::values) {
        std::ranges::sort(v,
                          [](const ShardMetadata &a, const ShardMetadata &b) {
                            return a.index < b.index;
                          });
      }

      log_info("Grouped shards: " + std::to_string(grouped_shards.size()));
      for (auto &[schema_name, shards] : grouped_shards) {
        log_info("Loading shard '{}', size={}", schema_name, shards.size());
        for (auto &shard_metadata : shards) {
          auto shard_result = this->storage_->read_shard(shard_metadata);
          if (!shard_result.ok()) {
            log_error("Failed to load shard: " +
                      shard_result.status().ToString());
            return shard_result
                .status();  // Return the error instead of continuing
          }

          const auto &shard = shard_result.ValueOrDie();
          log_debug("Adding shard from snapshot, shard_metadata:  " +
                    shard_metadata.toString());
          log_debug("shard id = " + std::to_string(shard->id) +
                    ", min_id: " + std::to_string(shard->min_id) +
                    ", max_id: " + std::to_string(shard->max_id) +
                    ", size: " + std::to_string(shard->size()));

          shard->set_updated(false);

          if (auto add_result = this->shard_manager_->add_shard(shard);
              !add_result.ok()) {
            log_error("Failed to add shard: " + add_result.status().ToString());
            return add_result.status();
          }
        }
      }
      log_info("Load edges");
      for (const auto &edge_metadata : this->manifest_->edges) {
        for (auto edges = storage_->read_edges(edge_metadata).ValueOrDie();
             const auto &edge : edges) {
          edge_store_->add(std::make_shared<Edge>(edge)).ValueOrDie();
        }
      }
      log_info("Edges have been loaded");
      for (const auto &edge_type : edge_store_->get_edge_types()) {
        log_debug("edges type '" + edge_type + "' size = " +
                  std::to_string(edge_store_->get_count_by_type(edge_type)));
      }
    } else {
      log_info("No current snapshot exists");
    }
    return true;
  } catch (const std::exception &e) {
    log_error("Failed to initialize snapshot manager: " +
              std::string(e.what()));
    return arrow::Status::IOError("Failed to initialize snapshot manager: ",
                                  e.what());
  }
}

arrow::Result<Snapshot> SnapshotManager::commit() {
  log_info("Creating new snapshot");
  auto timestamp_ms = now_millis();

  std::unordered_map<std::string, std::unordered_map<int64_t, bool>>
      original_update_states;
  for (const auto &schema_name : this->shard_manager_->get_schema_names()) {
    for (const auto &shard :
         this->shard_manager_->get_shards(schema_name).ValueOrDie()) {
      original_update_states[schema_name][shard->id] = shard->is_updated();
    }
  }

  log_info("Compacting all shards before snapshot creation");
  if (auto compact_result = this->shard_manager_->compact_all();
      !compact_result.ok()) {
    log_warn("Failed to compact all shards: " +
             compact_result.status().ToString());
  }

  // Restore the original updated status for shards whose content didn't
  // actually change
  for (const auto &schema_name : this->shard_manager_->get_schema_names()) {
    for (const auto &shard :
         this->shard_manager_->get_shards(schema_name).ValueOrDie()) {
      // If the shard existed before compaction and wasn't marked as updated,
      // restore that status
      if (original_update_states.contains(schema_name) &&
          original_update_states[schema_name].contains(shard->id) &&
          !original_update_states[schema_name][shard->id]) {
        shard->set_updated(false);
      }
    }
  }

  Snapshot new_snapshot;
  new_snapshot.id = generate_unique_snapshot_id();  // timestamp_ms;
  new_snapshot.timestamp_ms = timestamp_ms;

  if (this->metadata_.get_current_snapshot() != nullptr) {
    new_snapshot.parent_id = this->metadata_.get_current_snapshot()->id;
  }

  std::unordered_map<std::string, std::unordered_map<int64_t, ShardMetadata>>
      curr_shard_metadata;
  std::unordered_map<std::string, EdgeMetadata> curr_edge_metadata;

  if (this->manifest_ != nullptr) {
    for (const auto &shard : this->manifest_->shards) {
      curr_shard_metadata[shard.schema_name][shard.id] = shard;
    }
    for (const auto &edge : this->manifest_->edges) {
      curr_edge_metadata[edge.edge_type] = edge;
    }
  }

  Manifest new_manifest;
  new_manifest.id = generate_uuid();
  new_manifest.edge_id_seq = edge_store_->get_edge_id_counter();
  new_manifest.node_id_seq = node_manager_->get_id_counter();
  new_manifest.shard_id_seq = shard_manager_->get_id_counter();

  log_info("Saving counters: edge_id_seq=" +
           std::to_string(new_manifest.edge_id_seq) +
           ", node_id_seq=" + std::to_string(new_manifest.node_id_seq) +
           ", shard_id_seq=" + std::to_string(new_manifest.shard_id_seq));

  for (const auto &edge_type : edge_store_->get_edge_types()) {
    if (curr_edge_metadata.contains(edge_type) &&
        curr_edge_metadata[edge_type].record_count ==
            edge_store_->get_count_by_type(edge_type)) {
      log_debug("edges type '" + edge_type + "' has not changed");
      new_manifest.edges.push_back(curr_edge_metadata[edge_type]);
    } else {
      log_debug("store edges type '" + edge_type + "'");
      EdgeMetadata new_edge_metadata;
      auto table = edge_store_->get_table(edge_type).ValueOrDie();
      new_edge_metadata.edge_type = edge_type;
      new_edge_metadata.data_file =
          storage_->write_table(table, edge_store_->get_chunk_size(), "edges")
              .ValueOrDie();
      new_edge_metadata.record_count =
          edge_store_->get_count_by_type(edge_type);
      new_manifest.edges.push_back(new_edge_metadata);
    }
  }

  for (const auto &schema_name : this->shard_manager_->get_schema_names()) {
    log_info("Writing shards for schema: " + schema_name);
    for (const auto &shard :
         this->shard_manager_->get_shards(schema_name).ValueOrDie()) {
      log_debug("Snapshotting shard id: " + std::to_string(shard->id));
      log_debug("Snapshotting shard size: " + std::to_string(shard->size()));

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
        shard_metadata.index = shard->index;
        shard_metadata.data_file =
            this->storage_->write_shard(shard).ValueOrDie();
        new_manifest.shards.push_back(shard_metadata);
      } else if (curr_shard_metadata.contains(shard->schema_name) &&
                 curr_shard_metadata[shard->schema_name].contains(shard->id)) {
        new_manifest.shards.push_back(
            curr_shard_metadata[shard->schema_name][shard->id]);
      }
    }
  }

  new_snapshot.manifest_location =
      this->metadata_manager_->write_manifest(new_manifest).ValueOrDie();
  log_info("Created manifest: " + new_manifest.id + " at " +
           new_snapshot.manifest_location);
  this->metadata_.snapshots.push_back(new_snapshot);
  this->metadata_.current_snapshot_index = this->metadata_.snapshots.size() - 1;
  log_info("Updating schemas");
  std::vector<SchemaMetadata> schemas;
  for (const auto &name : this->schema_registry_->get_schema_names()) {
    auto arrow_shema = this->schema_registry_->get_arrow(name).ValueOrDie();
    schemas.push_back(arrow_schema_to_metadata(name, arrow_shema).ValueOrDie());
  }
  log_info("schemas count {}", schemas.size());
  this->metadata_.schemas = schemas;
  std::string metadata_location =
      this->metadata_manager_->write_metadata(this->metadata_).ValueOrDie();
  log_info("Saved metadata to: " + metadata_location);
  DatabaseInfo db_info;
  db_info.metadata_location = metadata_location;
  db_info.timestamp_ms = timestamp_ms;
  this->metadata_manager_->write_db_info(db_info).ValueOrDie();
  log_info("Updated database info to point to new metadata");
  if (auto reset_result = shard_manager_->reset_all_updated();
      !reset_result.ok()) {
    log_warn("Failed to reset shard updated flags: " +
             reset_result.status().ToString());
  }
  this->manifest_.reset();
  this->manifest_ = std::make_shared<Manifest>(new_manifest);
  return new_snapshot;
}

Snapshot *SnapshotManager::current_snapshot() {
  return this->metadata_.get_current_snapshot();
}

std::shared_ptr<Manifest> SnapshotManager::get_manifest() {
  return this->manifest_;
}

}  // namespace tundradb
