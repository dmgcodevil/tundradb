#include "storage/snapshot_manager.hpp"

#include "common/logger.hpp"
#include "common/utils.hpp"
#include "core/edge.hpp"
#include "core/node.hpp"
#include "storage/shard.hpp"
#include "storage/storage.hpp"

namespace tundradb {

SnapshotManager::SnapshotManager(
    std::shared_ptr<MetadataManager> metadata_manager,
    std::shared_ptr<Storage> storage,
    std::shared_ptr<ShardManager> shard_manager,
    std::shared_ptr<EdgeStore> edge_store,
    std::shared_ptr<NodeManager> node_manager,
    std::shared_ptr<SchemaRegistry> schema_registry)
    : metadata_manager_(std::move(metadata_manager)),
      storage_(std::move(storage)),
      shard_manager_(std::move(shard_manager)),
      edge_store_(std::move(edge_store)),
      node_manager_(std::move(node_manager)),
      schema_registry_(std::move(schema_registry)) {}

arrow::Result<bool> SnapshotManager::initialize() {
  log_info("Initializing snapshot manager...");
  try {
    ARROW_RETURN_NOT_OK(restore_metadata());
    if (metadata_.get_current_snapshot() == nullptr) {
      log_info("No current snapshot exists");
      return true;
    }
    ARROW_RETURN_NOT_OK(restore_schemas());
    ARROW_RETURN_NOT_OK(restore_manifest());
    ARROW_RETURN_NOT_OK(restore_shards());
    ARROW_RETURN_NOT_OK(restore_edge_schemas());
    ARROW_RETURN_NOT_OK(restore_edges());
    return true;
  } catch (const std::exception &e) {
    log_error("Failed to initialize snapshot manager: " +
              std::string(e.what()));
    return arrow::Status::IOError("Failed to initialize snapshot manager: ",
                                  e.what());
  }
}

// ---------------------------------------------------------------------------
// Initialization phases
// ---------------------------------------------------------------------------

arrow::Status SnapshotManager::restore_metadata() {
  ARROW_ASSIGN_OR_RAISE(metadata_, metadata_manager_->load_current_metadata());
  log_info("Metadata has been loaded.");
  log_info("Metadata current snapshot index=" +
           std::to_string(metadata_.current_snapshot_index));
  log_info("Metadata current snapshots size=" +
           std::to_string(metadata_.snapshots.size()));
  if (metadata_.get_current_snapshot() != nullptr) {
    log_info("Current snapshot id = " +
             std::to_string(metadata_.get_current_snapshot()->id));
  }
  return arrow::Status::OK();
}

arrow::Status SnapshotManager::restore_schemas() {
  log_info("Restoring schemas");
  for (const auto &schema_metadata : metadata_.schemas) {
    log_info("Loading schema " + schema_metadata.name);
    ARROW_ASSIGN_OR_RAISE(auto arrow_schema,
                          metadata_to_arrow_schema(schema_metadata));
    ARROW_RETURN_NOT_OK(
        schema_registry_->add_arrow(schema_metadata.name, arrow_schema));
  }
  return arrow::Status::OK();
}

arrow::Status SnapshotManager::restore_manifest() {
  log_info("Restoring manifest and counters");
  ARROW_ASSIGN_OR_RAISE(
      auto manifest, read_json_file<Manifest>(
                         metadata_.get_current_snapshot()->manifest_location));
  manifest_ = std::make_shared<Manifest>(std::move(manifest));
  log_info("Manifest has been loaded");
  log_info(manifest_->toString());

  edge_store_->set_id_seq(manifest_->edge_id_seq);
  node_manager_->set_all_id_counters(manifest_->node_id_seq_per_schema);
  shard_manager_->set_id_counter(manifest_->shard_id_seq);

  std::unordered_map<std::string, int64_t> max_index_per_schema;
  for (const auto &shard : manifest_->shards) {
    auto &max_index = max_index_per_schema[shard.schema_name];
    max_index = std::max(max_index, shard.index + 1);
  }
  for (const auto &[schema_name, max_index] : max_index_per_schema) {
    shard_manager_->set_index_counter(schema_name, max_index);
    log_debug("Set shard index counter for schema '" + schema_name + "' to " +
              std::to_string(max_index));
  }
  return arrow::Status::OK();
}

arrow::Status SnapshotManager::restore_shards() {
  log_info("Restoring shards");

  std::unordered_map<std::string, std::vector<ShardMetadata>> grouped_shards;
  for (const auto &shard : manifest_->shards) {
    grouped_shards[shard.schema_name].push_back(shard);
  }
  for (auto &v : grouped_shards | std::views::values) {
    std::ranges::sort(v, [](const ShardMetadata &a, const ShardMetadata &b) {
      return a.index < b.index;
    });
  }

  log_info("Grouped shards: " + std::to_string(grouped_shards.size()));
  for (auto &[schema_name, shards] : grouped_shards) {
    log_info("Restoring shard '{}', size={}", schema_name, shards.size());
    for (auto &shard_metadata : shards) {
      ARROW_ASSIGN_OR_RAISE(auto shard, storage_->read_shard(shard_metadata));
      log_debug("Adding shard from snapshot, shard_metadata:  " +
                shard_metadata.toString());
      log_debug("shard id = " + std::to_string(shard->id) +
                ", min_id: " + std::to_string(shard->min_id) +
                ", max_id: " + std::to_string(shard->max_id) +
                ", size: " + std::to_string(shard->size()));
      shard->set_updated(false);
      ARROW_RETURN_NOT_OK(shard_manager_->add_shard(shard));
    }
  }
  return arrow::Status::OK();
}

arrow::Status SnapshotManager::restore_edge_schemas() {
  log_info("Restoring edge schemas");
  for (const auto &es_meta : metadata_.edge_schemas) {
    if (edge_store_->has_edge_schema(es_meta.name)) {
      continue;
    }
    std::vector<std::shared_ptr<Field>> fields;
    fields.reserve(es_meta.fields.size());
    for (const auto &fm : es_meta.fields) {
      fields.push_back(from_metadata(fm));
    }
    auto reg_res = edge_store_->register_edge_schema(es_meta.name, fields);
    if (!reg_res.ok()) {
      log_warn("Failed to register edge schema for '" + es_meta.name +
               "': " + reg_res.status().ToString());
    }
  }
  return arrow::Status::OK();
}

arrow::Status SnapshotManager::restore_edges() {
  log_info("Restoring edges");
  for (const auto &edge_metadata : manifest_->edges) {
    ARROW_ASSIGN_OR_RAISE(auto ok,
                          storage_->read_edges(edge_metadata, edge_store_));
    (void)ok;
  }
  log_info("Edges have been loaded");
  for (const auto &edge_type : edge_store_->get_edge_types()) {
    log_debug("edges type '" + edge_type + "' size = " +
              std::to_string(edge_store_->get_count_by_type(edge_type)));
  }
  return arrow::Status::OK();
}

void SnapshotManager::compact_and_preserve_flags() {
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

  for (const auto &schema_name : this->shard_manager_->get_schema_names()) {
    for (const auto &shard :
         this->shard_manager_->get_shards(schema_name).ValueOrDie()) {
      if (original_update_states.contains(schema_name) &&
          original_update_states[schema_name].contains(shard->id) &&
          !original_update_states[schema_name][shard->id]) {
        shard->set_updated(false);
      }
    }
  }
}

void SnapshotManager::commit_edges(
    const std::unordered_map<std::string, EdgeMetadata> &curr_edge_metadata,
    Manifest &new_manifest) {
  for (const auto &edge_type : edge_store_->get_edge_types()) {
    if (curr_edge_metadata.contains(edge_type) &&
        curr_edge_metadata.at(edge_type).record_count ==
            edge_store_->get_count_by_type(edge_type)) {
      log_debug("edges type '" + edge_type + "' has not changed");
      new_manifest.edges.push_back(curr_edge_metadata.at(edge_type));
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

      if (auto es = edge_store_->get_edge_schema(edge_type)) {
        new_edge_metadata.schema_version = static_cast<int32_t>(es->version());
      }

      new_manifest.edges.push_back(new_edge_metadata);
    }
  }
}

void SnapshotManager::commit_shards(
    const std::unordered_map<std::string,
                             std::unordered_map<int64_t, ShardMetadata>>
        &curr_shard_metadata,
    Manifest &new_manifest) {
  for (const auto &schema_name : this->shard_manager_->get_schema_names()) {
    log_info("Writing shards for schema: " + schema_name);
    for (const auto &shard :
         this->shard_manager_->get_shards(schema_name).ValueOrDie()) {
      log_debug("Snapshotting shard id: " + std::to_string(shard->id));
      log_debug("Snapshotting shard size: " + std::to_string(shard->size()));

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
                 curr_shard_metadata.at(shard->schema_name)
                     .contains(shard->id)) {
        new_manifest.shards.push_back(
            curr_shard_metadata.at(shard->schema_name).at(shard->id));
      }
    }
  }
}

void SnapshotManager::commit_schemas(Snapshot &new_snapshot,
                                     const Manifest &new_manifest) {
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

  std::vector<SchemaMetadata> edge_schemas;
  for (const auto &edge_type : edge_store_->get_edge_types()) {
    if (auto es = edge_store_->get_edge_schema(edge_type)) {
      SchemaMetadata sm;
      sm.name = es->name();
      sm.version = es->version();
      for (const auto &field : es->fields()) {
        sm.fields.push_back(FieldMetadata::from_type_descriptor(
            field->name(), field->type_descriptor(), field->nullable()));
      }
      edge_schemas.push_back(sm);
    }
  }
  this->metadata_.edge_schemas = edge_schemas;
}

void SnapshotManager::finalize_commit(const Manifest &new_manifest,
                                      int64_t timestamp_ms) {
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
}

arrow::Result<Snapshot> SnapshotManager::commit() {
  log_info("Creating new snapshot");
  auto timestamp_ms = now_millis();

  compact_and_preserve_flags();

  Snapshot new_snapshot;
  new_snapshot.id = generate_unique_snapshot_id();
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
  new_manifest.node_id_seq_per_schema = node_manager_->get_all_id_counters();
  new_manifest.shard_id_seq = shard_manager_->get_id_counter();

  std::stringstream node_counters_str;
  node_counters_str << "{";
  size_t idx = 0;
  for (const auto &[schema_name, counter] :
       new_manifest.node_id_seq_per_schema) {
    node_counters_str << schema_name << ":" << counter;
    if (idx++ < new_manifest.node_id_seq_per_schema.size() - 1) {
      node_counters_str << ", ";
    }
  }
  node_counters_str << "}";

  log_info("Saving counters: edge_id_seq=" +
           std::to_string(new_manifest.edge_id_seq) +
           ", node_id_seq_per_schema=" + node_counters_str.str() +
           ", shard_id_seq=" + std::to_string(new_manifest.shard_id_seq));

  commit_edges(curr_edge_metadata, new_manifest);
  commit_shards(curr_shard_metadata, new_manifest);
  commit_schemas(new_snapshot, new_manifest);
  finalize_commit(new_manifest, timestamp_ms);

  return new_snapshot;
}

Snapshot *SnapshotManager::current_snapshot() {
  return this->metadata_.get_current_snapshot();
}

std::shared_ptr<Manifest> SnapshotManager::get_manifest() {
  return this->manifest_;
}

}  // namespace tundradb
