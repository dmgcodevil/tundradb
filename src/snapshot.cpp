#include "core.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "utils.hpp"

namespace tundradb {

arrow::Result<bool> SnapshotManager::initialize() {
  log_info("Initializing snapshot manager...");
  try {
    ARROW_ASSIGN_OR_RAISE(this->metadata,
                          metadata_manager->load_current_metadata());
    log_info("Metadata has been loaded.");
    log_info("Metadata current snapshot index=" +
             std::to_string(this->metadata.current_snapshot_index));
    log_info("Metadata current snapshots size=" +
             std::to_string(this->metadata.snapshots.size()));

    // Set current snapshot if it exists
    if (this->metadata.get_current_snapshot() != nullptr) {
      log_info("Current snapshot id = " +
               std::to_string(this->metadata.get_current_snapshot()->id));
      log_info("Initializing schemas");
      for (const auto &schema_metadata : this->metadata.schemas) {
        log_info("Loading schema " + schema_metadata.name);
        auto arrow_schema_result = MetadataToArrowSchema(schema_metadata);
        if (!arrow_schema_result.ok()) {
          log_error("Failed to load schema: " +
                    arrow_schema_result.status().ToString());
        }
        auto arrow_shema = arrow_schema_result.ValueOrDie();
        auto add_schema_result =
            schema_registry_->add(schema_metadata.name, arrow_shema);
        if (!add_schema_result.ok()) {
          log_error("Failed to add schema to registry: " +
                    add_schema_result.status().ToString());
          return arrow_schema_result.status();
        }
      }
      log_info("Load the manifest and initialize shards");
      auto manifest_result = read_json_file<Manifest>(
          this->metadata.get_current_snapshot()->manifest_location);
      if (!manifest_result.ok()) {
        log_error("Failed to load manifest: " +
                  manifest_result.status().ToString());
        return manifest_result.status();
      }
      this->manifest = std::make_shared<Manifest>(manifest_result.ValueOrDie());
      log_info("Manifest has been loaded");
      log_info(this->manifest->toString());

      // Set ID counters for all managers
      edge_store->set_id_seq(manifest->edge_id_seq);
      node_manager->set_id_counter(manifest->node_id_seq);
      shard_manager->set_id_counter(manifest->shard_id_seq);

      // Restore index counters for each schema
      std::unordered_map<std::string, int64_t> max_index_per_schema;
      for (const auto &shard : this->manifest->shards) {
        auto &max_index = max_index_per_schema[shard.schema_name];
        max_index =
            std::max(max_index,
                     shard.index + 1);  // Add 1 so next index is after highest
      }

      // Set the index counters based on max values
      for (const auto &[schema_name, max_index] : max_index_per_schema) {
        shard_manager->set_index_counter(schema_name, max_index);
        log_debug("Set shard index counter for schema '" + schema_name +
                  "' to " + std::to_string(max_index));
      }

      // Group shards by schema name
      std::unordered_map<std::string, std::vector<ShardMetadata>>
          grouped_shards;
      for (auto &shard : manifest_result.ValueOrDie().shards) {
        grouped_shards[shard.schema_name].push_back(shard);
      }

      // Sort shards by index for each schema to ensure consistent ordering
      // during restoration
      for (auto &[_, v] : grouped_shards) {
        std::sort(v.begin(), v.end(),
                  [](const ShardMetadata &a, const ShardMetadata &b) {
                    return a.index < b.index;
                  });
      }

      log_info("Grouped shards: " + std::to_string(grouped_shards.size()));

      // Load and add each shard
      for (auto &[schema_name, shards] : grouped_shards) {
        log_info("Loading shard '{}', size={}", schema_name, shards.size());
        for (auto &shard_metadata : shards) {
          auto shard_result = this->storage->read_shard(shard_metadata);
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

          auto add_result = this->shard_manager->add_shard(shard);
          if (!add_result.ok()) {
            log_error("Failed to add shard: " + add_result.status().ToString());
            return add_result
                .status();  // Return the error instead of continuing
          }
          for (const auto &node : shard->get_nodes()) {
            log_debug("Adding node {}", node->id);
            node_manager->add_node(node);
          }
        }
      }
      log_info("Load edges");
      for (const auto &edge_metadata : this->manifest->edges) {
        auto edges = storage->read_edges(edge_metadata).ValueOrDie();
        for (const auto &edge : edges) {
          edge_store->add(std::make_shared<Edge>(edge)).ValueOrDie();
        }
      }
      log_info("Edges have been loaded");
      for (const auto &edge_type : edge_store->get_edge_types()) {
        log_debug("edges type '" + edge_type + "' size = " +
                  std::to_string(edge_store->get_count_by_type(edge_type)));
      }

      // Ensure all shards are properly sorted by index
      // log_info("Sorting all shards by index");
      // this->shard_manager->sort_shards();
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

  // Save the original updated states of shards before compaction
  std::unordered_map<std::string, std::unordered_map<int64_t, bool>>
      original_update_states;

  // Store the original update status for each shard
  for (const auto &schema_name : this->shard_manager->get_schema_names()) {
    for (const auto &shard :
         this->shard_manager->get_shards(schema_name).ValueOrDie()) {
      original_update_states[schema_name][shard->id] = shard->is_updated();
    }
  }

  // Compact all shards before creating a new snapshot
  log_info("Compacting all shards before snapshot creation");
  auto compact_result = this->shard_manager->compact_all();
  if (!compact_result.ok()) {
    log_warn("Failed to compact all shards: " +
             compact_result.status().ToString());
  }

  // Restore the original updated status for shards whose content didn't
  // actually change
  for (const auto &schema_name : this->shard_manager->get_schema_names()) {
    for (const auto &shard :
         this->shard_manager->get_shards(schema_name).ValueOrDie()) {
      // If the shard existed before compaction and wasn't marked as updated,
      // restore that status
      if (original_update_states.count(schema_name) > 0 &&
          original_update_states[schema_name].count(shard->id) > 0 &&
          !original_update_states[schema_name][shard->id]) {
        shard->set_updated(false);
      }
    }
  }

  // Create new snapshot
  Snapshot new_snapshot;
  new_snapshot.id = timestamp_ms;
  new_snapshot.timestamp_ms = timestamp_ms;

  // Set parent ID if previous snapshot exists
  if (this->metadata.get_current_snapshot() != nullptr) {
    new_snapshot.parent_id = this->metadata.get_current_snapshot()->id;
  }

  // Track shard metadata from current manifest
  std::unordered_map<std::string, std::unordered_map<int64_t, ShardMetadata>>
      curr_shard_metadata;
  std::unordered_map<std::string, EdgeMetadata> curr_edge_metadata;

  if (this->manifest != nullptr) {
    for (const auto &shard : this->manifest->shards) {
      curr_shard_metadata[shard.schema_name][shard.id] = shard;
    }
    for (const auto &edge : this->manifest->edges) {
      curr_edge_metadata[edge.edge_type] = edge;
    }
  }

  // Create new manifest
  Manifest new_manifest;
  new_manifest.id = generate_uuid();

  // Save all ID counters to the manifest
  new_manifest.edge_id_seq = edge_store->get_edge_id_counter();
  new_manifest.node_id_seq = node_manager->get_id_counter();
  new_manifest.shard_id_seq = shard_manager->get_id_counter();

  log_info("Saving counters: edge_id_seq=" +
           std::to_string(new_manifest.edge_id_seq) +
           ", node_id_seq=" + std::to_string(new_manifest.node_id_seq) +
           ", shard_id_seq=" + std::to_string(new_manifest.shard_id_seq));

  for (const auto &edge_type : edge_store->get_edge_types()) {
    if (curr_edge_metadata.contains(edge_type) &&
        curr_edge_metadata[edge_type].record_count ==
            edge_store->get_count_by_type(edge_type)) {
      log_debug("edges type '" + edge_type + "' has not changed");
      new_manifest.edges.push_back(curr_edge_metadata[edge_type]);
    } else {
      log_debug("store edges type '" + edge_type + "'");
      EdgeMetadata new_edge_metadata;
      auto table = edge_store->get_table(edge_type).ValueOrDie();
      new_edge_metadata.edge_type = edge_type;
      new_edge_metadata.data_file =
          storage->write_table(table, edge_store->get_chunk_size(), "edges")
              .ValueOrDie();
      new_edge_metadata.record_count = edge_store->get_count_by_type(edge_type);
      new_manifest.edges.push_back(new_edge_metadata);
    }
  }

  // Go through all schemas and shards
  for (const auto &schema_name : this->shard_manager->get_schema_names()) {
    log_info("Writing shards for schema: " + schema_name);
    for (const auto &shard :
         this->shard_manager->get_shards(schema_name).ValueOrDie()) {
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
        shard_metadata.index =
            shard->index;  // shard_manager->get_index_counter(schema_name);
        shard_metadata.data_file =
            this->storage->write_shard(shard).ValueOrDie();
        new_manifest.shards.push_back(shard_metadata);
      } else if (curr_shard_metadata.contains(shard->schema_name) &&
                 curr_shard_metadata[shard->schema_name].contains(shard->id)) {
        new_manifest.shards.push_back(
            curr_shard_metadata[shard->schema_name][shard->id]);
      }
    }
  }

  // Save the manifest
  new_snapshot.manifest_location =
      this->metadata_manager->write_manifest(new_manifest).ValueOrDie();
  log_info("Created manifest: " + new_manifest.id + " at " +
           new_snapshot.manifest_location);

  // Add the new snapshot to metadata
  this->metadata.snapshots.push_back(new_snapshot);

  this->metadata.current_snapshot_index = this->metadata.snapshots.size() - 1;

  // Save the updated metadata
  std::string metadata_location =
      this->metadata_manager->write_metadata(this->metadata).ValueOrDie();
  log_info("Saved metadata to: " + metadata_location);

  log_info("Updating schemas");
  std::vector<SchemaMetadata> schemas;
  for (const auto &name : this->schema_registry_->get_schema_names()) {
    auto arrow_shema = this->schema_registry_->get(name).ValueOrDie();
    schemas.push_back(ArrowSchemaToMetadata(name, arrow_shema).ValueOrDie());
  }
  log_info("schemas count {}", schemas.size());
  this->metadata.schemas = schemas;

  // Update database info to point to the new metadata location
  DatabaseInfo db_info;
  db_info.metadata_location = metadata_location;
  db_info.timestamp_ms = timestamp_ms;
  this->metadata_manager->write_db_info(db_info).ValueOrDie();
  log_info("Updated database info to point to new metadata");

  // Reset all updated flags
  auto reset_result = shard_manager->reset_all_updated();
  if (!reset_result.ok()) {
    log_warn("Failed to reset shard updated flags: " +
             reset_result.status().ToString());
  }
  this->manifest.reset();
  this->manifest = std::make_shared<Manifest>(new_manifest);

  // Return a copy of the snapshot instead of a pointer
  return new_snapshot;
}

Snapshot *SnapshotManager::current_snapshot() {
  return this->metadata.get_current_snapshot();
}

std::shared_ptr<Manifest> SnapshotManager::get_manifest() {
  return this->manifest;
}

}  // namespace tundradb
