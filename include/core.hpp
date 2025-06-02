#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <parquet/arrow/reader.h>
#include <spdlog/spdlog.h>
#include <tbb/concurrent_map.h>
#include <tbb/concurrent_vector.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../libs/json/json.hpp"
#include "config.hpp"
#include "edge_store.hpp"
#include "file_utils.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "node.hpp"
#include "query.hpp"
#include "schema.hpp"
#include "storage.hpp"
#include "utils.hpp"

namespace tundradb {

class Database;
class Node;
class Shard;
class ShardManager;
class MetadataManager;
class Storage;
class NodeManager;

class SnapshotManager {
 public:
  explicit SnapshotManager(std::shared_ptr<MetadataManager> metadata_manager,
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

  arrow::Result<bool> initialize();
  arrow::Result<Snapshot> commit();
  Snapshot *current_snapshot();
  std::shared_ptr<Manifest> get_manifest();

 private:
  std::shared_ptr<MetadataManager> metadata_manager_;
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<ShardManager> shard_manager_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::shared_ptr<EdgeStore> edge_store_;
  std::shared_ptr<NodeManager> node_manager_;
  Metadata metadata_;
  std::shared_ptr<Manifest> manifest_;
  std::shared_ptr<EdgeMetadata> edge_metadata_;
};

class Shard {
 private:
  std::pmr::monotonic_buffer_resource memory_pool_;
  std::pmr::unordered_map<int64_t, std::shared_ptr<Node>> nodes_;
  std::set<int64_t> nodes_ids_;
  std::atomic<bool> dirty_{false};
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  int64_t updated_ts_ = now_millis();
  bool updated_ = true;  // todo should be false when we read from snaptshot and
                         // after commit

 public:
  const int64_t id;         // Unique shard identifier
  const int64_t index;      // index of the shard in the shard manager
  int64_t min_id;           // Minimum node ID in this shard
  int64_t max_id;           // Maximum node ID in this shard
  const size_t capacity;    // Maximum number of nodes
  const size_t chunk_size;  // Size of chunks for table creation
  std::string schema_name;  // Name of the schema this shard holds

  Shard(int64_t id, int64_t index, size_t capacity, int64_t min_id,
        int64_t max_id, size_t chunk_size, const std::string &schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry,
        size_t buffer_size = 10 * 1024 * 1024)
      : id(id),
        index(index),
        capacity(capacity),
        min_id(min_id),
        max_id(max_id),
        chunk_size(chunk_size),
        memory_pool_(buffer_size),
        nodes_(&memory_pool_),
        schema_registry_(std::move(schema_registry)),
        schema_name(schema_name) {}

  // Constructor that uses DatabaseConfig
  Shard(int64_t id, int64_t index, const DatabaseConfig &config, int64_t min_id,
        int64_t max_id, const std::string &schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry)
      : id(id),
        index(index),
        capacity(config.get_shard_capacity()),
        min_id(min_id),
        max_id(max_id),
        chunk_size(config.get_chunk_size()),
        memory_pool_(config.get_shard_memory_pool_size()),
        nodes_(&memory_pool_),
        schema_registry_(std::move(schema_registry)),
        schema_name(schema_name) {}

  ~Shard() {
    // Clear the nodes map first to release node resources
    nodes_.clear();

    // Clear the nodes_ids set
    nodes_ids_.clear();

    // Clear the table
    table_.reset();

    // The memory_pool will be automatically destroyed
    // The schema_registry is a shared_ptr, so it will be handled by reference
    // counting
  }

  bool is_updated() const { return updated_; }

  bool set_updated(bool v) {
    updated_ = v;
    return updated_;
  }

  int64_t get_updated_ts() const { return updated_ts_; }

  std::string compound_id() const {
    return this->schema_name + "-" + std::to_string(this->id);
  }

  arrow::Result<bool> add(const std::shared_ptr<Node> &node) {
    if (node->id < min_id || node->id > max_id) {
      return arrow::Status::Invalid("Node id is out of range");
    }
    if (nodes_.contains(node->id)) {
      return arrow::Status::KeyError("Node already exists: ", node->id);
    }
    if (nodes_.size() >= capacity) {
      return arrow::Status::KeyError("Shard is full");
    }
    nodes_.insert(std::make_pair(node->id, node));
    nodes_ids_.insert(node->id);
    dirty_ = true;
    updated_ = true;
    return true;
  }

  arrow::Result<bool> extend(const std::shared_ptr<Node> &node) {
    if (nodes_.contains(node->id)) {
      return arrow::Status::KeyError("Node already exists: ", node->id);
    }
    if (nodes_.size() >= capacity) {
      return arrow::Status::KeyError("Shard is full");
    }

    if (empty()) {
      min_id = node->id;
      max_id = node->id;
    } else {
      if (node->id < min_id) {
        return arrow::Status::Invalid("Node id is below the minimum range");
      }
      max_id = std::max(max_id, node->id);
    }

    nodes_.insert(std::make_pair(node->id, node));
    nodes_ids_.insert(node->id);

    dirty_ = true;
    updated_ = true;
    updated_ts_ = now_millis();
    return true;
  }

  arrow::Result<std::shared_ptr<Node>> remove(int64_t id) {
    auto it = nodes_.find(id);
    if (it == nodes_.end()) {
      return arrow::Status::Invalid("Node not found: ", id);
    }
    auto node = it->second;
    nodes_.erase(id);
    nodes_ids_.erase(id);
    dirty_ = true;
    updated_ = true;
    return node;
  }

  arrow::Result<std::shared_ptr<Node>> poll_first() {
    if (nodes_ids_.empty()) {
      return arrow::Status::Invalid("Shard is empty");
    }
    auto first = nodes_ids_.begin();
    auto node_id = *first;
    nodes_ids_.erase(first);
    auto node = nodes_[node_id];
    nodes_.erase(node_id);

    // Update the min_id to the next minimum if available
    if (!nodes_ids_.empty()) {
      min_id = *nodes_ids_.begin();
    }

    dirty_ = true;
    updated_ = true;
    updated_ts_ = now_millis();
    return node;
  }

  arrow::Result<bool> update(const std::shared_ptr<BaseOperation> &update) {
    updated_ = true;
    if (!nodes_.contains(update->node_id)) {
      return arrow::Status::KeyError("Node not found: ", update->node_id);
    }
    dirty_ = true;
    updated_ = true;
    updated_ts_ = now_millis();
    return nodes_[update->node_id]->update(update);
  }

  arrow::Result<std::shared_ptr<arrow::Table>> get_table() {
    if (dirty_ || !table_) {
      ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry_->get(schema_name));
      std::vector<std::shared_ptr<Node>> result;
      std::ranges::transform(nodes_, std::back_inserter(result),
                             [](const auto &pair) { return pair.second; });

      // Sort nodes by ID in ascending order
      std::sort(result.begin(), result.end(),
                [](const std::shared_ptr<Node> &a,
                   const std::shared_ptr<Node> &b) { return a->id < b->id; });

      ARROW_ASSIGN_OR_RAISE(table_, create_table(schema, result, chunk_size));
      dirty_ = false;
    }
    return table_;
  }

  size_t size() const { return nodes_.size(); }

  bool has_space() const { return nodes_.size() < capacity; }

  bool empty() const { return nodes_.empty(); }

  std::vector<std::shared_ptr<Node>> get_nodes() const {
    std::vector<std::shared_ptr<Node>> result;
    result.reserve(nodes_.size());
    for (const auto &[_, node] : nodes_) {
      result.push_back(node);
    }
    return result;
  }
};

class ShardManager {
 private:
  std::pmr::monotonic_buffer_resource memory_pool_;
  std::pmr::unordered_map<std::string, std::vector<std::shared_ptr<Shard>>>
      shards_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  const size_t shard_capacity_;
  const size_t chunk_size_;
  const DatabaseConfig config_;
  std::atomic<int64_t> id_counter_{
      0};  // Global unique ID counter for all shards
  std::unordered_map<std::string, std::atomic<int64_t>>
      index_counters_;                      // Per-schema index/position counter
  mutable std::mutex index_counter_mutex_;  // todo use tbb map instead

  void create_new_shard(const std::shared_ptr<Node> &node) {
    auto new_min_id = node->id;
    auto new_max_id = node->id + shard_capacity_ - 1;

    // Get the next index for this schema
    int64_t shard_index;
    {
      std::lock_guard<std::mutex> lock(index_counter_mutex_);
      shard_index = index_counters_[node->schema_name]++;
    }

    // Create shard with global unique ID and schema-specific index
    auto shard = std::make_shared<Shard>(id_counter_.fetch_add(1), shard_index,
                                         config_, new_min_id, new_max_id,
                                         node->schema_name, schema_registry_);

    auto result = shard->add(node);
    if (!result.ok()) {
      // Log error - this shouldn't happen with newly created shard
      std::cerr << "Error adding node to new shard: "
                << result.status().ToString() << std::endl;
    }

    shards_[node->schema_name].push_back(shard);
  }

 public:
  explicit ShardManager(std::shared_ptr<SchemaRegistry> schema_registry,
                        const DatabaseConfig &config)
      : memory_pool_(config.get_manager_memory_pool_size()),
        shards_(&memory_pool_),
        schema_registry_(std::move(schema_registry)),
        shard_capacity_(config.get_shard_capacity()),
        chunk_size_(config.get_chunk_size()),
        config_(config) {}

  void set_id_counter(int64_t value) { id_counter_.store(value); }
  int64_t get_id_counter() const { return id_counter_.load(); }

  void set_index_counter(const std::string &schema_name, int64_t value) {
    std::lock_guard<std::mutex> lock(index_counter_mutex_);
    index_counters_[schema_name].store(value);
  }

  arrow::Result<std::shared_ptr<Shard>> get_shard(
      const std::string &schema_name, int64_t id) {
    return shards_[schema_name][id];
  }

  int64_t get_index_counter(const std::string &schema_name) const {
    std::lock_guard<std::mutex> lock(index_counter_mutex_);
    auto it = index_counters_.find(schema_name);
    return it != index_counters_.end() ? it->second.load() : 0;
  }

  // Get all schema names that have shards
  std::vector<std::string> get_schema_names() const {
    std::vector<std::string> schema_names;
    schema_names.reserve(shards_.size());
    for (const auto &[schema_name, _] : shards_) {
      schema_names.push_back(schema_name);
    }
    return schema_names;
  }

  // Get shards for a given schema (returns a copy)
  arrow::Result<std::vector<std::shared_ptr<Shard>>> get_shards(
      const std::string &schema_name) const {
    auto it = shards_.find(schema_name);
    if (it == shards_.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }
    return it->second;
  }

  arrow::Result<bool> is_shard_clean(std::string s, int64_t id) {
    return !shards_[s][id]->is_updated();
  }

  arrow::Result<bool> compact(const std::string &schema_name) {
    const auto it = shards_.find(schema_name);
    if (it == shards_.end()) {
      return arrow::Status::Invalid("Shard not found for the given schema: ",
                                    schema_name);
    }

    auto &shard_list = it->second;  // Use reference to modify actual collection
    if (shard_list.size() <= 1) {
      // nothing to compact
      return true;
    }

    for (size_t i = 1; i < shard_list.size(); i++) {
      const auto &prev = shard_list[i - 1];
      const auto &curr = shard_list[i];

      while (prev->has_space() && !curr->empty()) {
        auto node = curr->poll_first().ValueOrDie();
        prev->extend(node).ValueOrDie();
        log_debug("node id: " + std::to_string(node->id) +
                  " moved from shard: " + std::to_string(i) +
                  " to shard: " + std::to_string(i - 1));
        log_debug("prev shard id: " + std::to_string(i - 1) +
                  " min_id=" + std::to_string(prev->min_id) +
                  " max_id=" + std::to_string(prev->max_id));

        log_debug("curr shard id: " + std::to_string(i) +
                  " min_id=" + std::to_string(curr->min_id) +
                  " max_id=" + std::to_string(curr->max_id));
      }
    }

    // Second pass: remove empty shards
    auto it_shard = shard_list.begin();
    while (it_shard != shard_list.end()) {
      if ((*it_shard)->empty()) {
        it_shard = shard_list.erase(it_shard);
      } else {
        ++it_shard;
      }
    }

    return true;
  }

  // Compact all schemas in the database
  arrow::Result<bool> compact_all() {
    std::vector<std::string> schema_names =
        schema_registry_->get_schema_names();
    bool success = true;

    for (const auto &schema_name : schema_names) {
      auto result = compact(schema_name);
      if (!result.ok()) {
        std::cerr << "Error compacting schema '" << schema_name
                  << "': " << result.status().ToString() << std::endl;
        success = false;
      }
    }

    return success;
  }

  arrow::Result<bool> insert_node(const std::shared_ptr<Node> &node) {
    log_debug("inserting node id " + std::to_string(node->id));
    const auto it = shards_.find(node->schema_name);
    if (it == shards_.end()) {
      // std::cout << " Create new shard entry for: " << node->schema_name
      //           << std::endl;
      shards_[node->schema_name] = std::vector<std::shared_ptr<Shard>>();
      create_new_shard(node);
      return true;
    }

    const auto &shard_list = it->second;
    if (shard_list.empty()) {
      // std::cout << "shard is empty schema: " << node->schema_name <<
      // std::endl;
      create_new_shard(node);
      return true;
    }

    // First try to find shards that can directly add the node (ID is in range)
    for (auto &shard : shard_list) {
      if (node->id >= shard->min_id && node->id <= shard->max_id &&
          shard->has_space()) {
        if (auto result = shard->add(node); result.ok()) {
          log_debug("node id: '" + std::to_string(node->id) +
                    "' inserted to shard id: " + std::to_string(shard->id));
          return true;
        }
        // If there was an error, we'll try the next shard
      }
    }

    // std::cout << "no shard with space to insert node: " << node->id
    //           << std::endl;

    // If no shard can directly add the node, try to find a shard that has space
    // and can be extended with this node ID
    for (auto &shard : shard_list) {
      if (shard->has_space()) {
        // If node ID is higher than max_id, we can extend the shard
        if (node->id > shard->max_id) {
          if (auto result = shard->extend(node); result.ok()) {
            return true;
          }
        }
        // We don't handle node ID < min_id because that's rare in our
        // design where IDs are normally assigned in increasing order
      }
    }

    // If we get here, we need a new shard
    create_new_shard(node);
    return true;
  }

  arrow::Result<std::shared_ptr<Node>> get_node(const std::string &schema_name,
                                                int64_t node_id) {
    const auto schema_it = shards_.find(schema_name);
    if (schema_it == shards_.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }

    // Search through all shards for this schema
    for (auto &shard : schema_it->second) {
      if (node_id >= shard->min_id && node_id <= shard->max_id) {
        // This is the right shard range, check if node exists
        try {
          if (auto node_result = shard->remove(node_id); node_result.ok()) {
            return node_result.ValueOrDie();
          }
        } catch (...) {
          // Node wasn't in this shard, continue to next shard
        }
      }
    }

    return arrow::Status::KeyError("Node with id ", node_id,
                                   " not found in schema '", schema_name, "'");
  }

  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id) {
    auto schema_it = shards_.find(schema_name);
    if (schema_it == shards_.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }

    // Search through all shards for this schema
    for (const auto &shard : schema_it->second) {
      if (node_id >= shard->min_id && node_id <= shard->max_id) {
        // This is the right shard range, try to remove
        if (auto remove_result = shard->remove(node_id); remove_result.ok()) {
          return true;
        }
      }
    }

    return arrow::Status::KeyError("Node with id ", node_id,
                                   " not found in schema '", schema_name, "'");
  }

  arrow::Result<bool> update_node(
      const std::shared_ptr<BaseOperation> &update) {
    // Since we don't have the schema name in the operation anymore, we need to
    // search in all schemas
    for (auto &schema_shards : shards_ | std::views::values) {
      // Find the right shard
      for (const auto &shard : schema_shards) {
        if (update->node_id >= shard->min_id &&
            update->node_id <= shard->max_id) {
          // Try to update in this shard
          auto update_result = shard->update(update);
          if (update_result.ok()) {
            return true;
          }
        }
      }
    }

    return arrow::Status::KeyError("Node with id ", update->node_id,
                                   " not found in any schema");
  }

  arrow::Result<std::vector<std::shared_ptr<Node>>> get_nodes(
      const std::string &schema_name) {
    const auto schema_it = shards_.find(schema_name);
    if (schema_it == shards_.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }

    std::vector<std::shared_ptr<Node>> result;
    // Reserve space for efficiency
    size_t total_estimated_nodes = 0;
    for (const auto &shard : schema_it->second) {
      total_estimated_nodes += shard->size();
    }
    result.reserve(total_estimated_nodes);

    // Collect nodes from all shards
    for (auto &shard : schema_it->second) {
      auto nodes = shard->get_nodes();
      result.insert(result.end(), nodes.begin(), nodes.end());
    }

    return result;
  }

  arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> get_tables(
      const std::string &schema_name) {
    const auto schema_it = shards_.find(schema_name);
    if (schema_it == shards_.end()) {
      return std::vector<std::shared_ptr<arrow::Table>>{};
    }

    // Copy shards to a vector we can sort
    std::vector<std::shared_ptr<Shard>> sorted_shards = schema_it->second;

    // Sort shards by min_id to ensure consistent ordering
    std::sort(
        sorted_shards.begin(), sorted_shards.end(),
        [](const std::shared_ptr<Shard> &a, const std::shared_ptr<Shard> &b) {
          return a->min_id < b->min_id;
        });

    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (const auto &shard : sorted_shards) {
      ARROW_ASSIGN_OR_RAISE(auto table, shard->get_table());
      if (table->num_rows() > 0) {
        tables.push_back(table);
      }
    }

    return tables;
  }

  bool has_shards(const std::string &schema_name) const {
    const auto it = shards_.find(schema_name);
    return it != shards_.end() && !it->second.empty();
  }

  // Get information about shards for a schema
  arrow::Result<size_t> get_shard_count(const std::string &schema_name) const {
    if (!has_shards(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shards_.find(schema_name)->second.size();
  }

  // Get sizes of all shards for a schema
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const {
    if (!has_shards(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    std::vector<size_t> sizes;
    for (const auto &shard : shards_.find(schema_name)->second) {
      sizes.push_back(shard->size());
    }
    return sizes;
  }

  // Get the min/max IDs of all shards for a schema
  arrow::Result<std::vector<std::pair<int64_t, int64_t>>> get_shard_ranges(
      const std::string &schema_name) const {
    if (!has_shards(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    std::vector<std::pair<int64_t, int64_t>> ranges;
    for (const auto &shard : shards_.find(schema_name)->second) {
      ranges.emplace_back(shard->min_id, shard->max_id);
    }
    return ranges;
  }

  // Add a pre-existing shard directly to the shard manager
  arrow::Result<bool> add_shard(const std::shared_ptr<Shard> &shard) {
    if (!shard) {
      return arrow::Status::Invalid("Cannot add null shard");
    }

    // Add shard to the appropriate schema's shard list
    shards_[shard->schema_name].push_back(shard);
    return true;
  }

  // Reset the updated flag for all shards
  arrow::Result<bool> reset_all_updated() {
    log_info("Resetting 'updated' flag for all shards");
    for (auto &schema_shards : shards_ | std::views::values) {
      for (auto &shard : schema_shards) {
        shard->set_updated(false);
      }
    }
    return true;
  }
};

class Database {
 private:
  // Schema registry
  std::shared_ptr<SchemaRegistry> schema_registry_;

  // Shard manager for node storage
  std::shared_ptr<ShardManager> shard_manager_;

  // Node manager for ID management
  std::shared_ptr<NodeManager> node_manager_;

  // Database configuration
  DatabaseConfig config_;

  // Whether persistence is enabled
  bool persistence_enabled_;

  // Storage for persistence
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<MetadataManager> metadata_manager_;
  std::shared_ptr<SnapshotManager> snapshot_manager_;
  std::shared_ptr<EdgeStore> edge_store_;

 public:
  // Constructor that takes a DatabaseConfig
  explicit Database(const DatabaseConfig &config = DatabaseConfig())
      : schema_registry_(std::make_shared<SchemaRegistry>()),
        shard_manager_(
            std::make_shared<ShardManager>(schema_registry_, config)),
        node_manager_(std::make_shared<NodeManager>()),
        config_(config),
        persistence_enabled_(config.is_persistence_enabled()),
        edge_store_(std::make_shared<EdgeStore>(0, config.get_chunk_size())) {
    if (persistence_enabled_) {
      const std::string &db_path = config.get_db_path();
      if (db_path.empty()) {
        log_error("Database path is empty but persistence is enabled");
        persistence_enabled_ = false;
        return;
      }

      std::string data_path = db_path + "/data";
      storage_ = std::make_shared<Storage>(std::move(data_path),
                                           schema_registry_, config);
      metadata_manager_ = std::make_shared<MetadataManager>(db_path);
      snapshot_manager_ = std::make_shared<SnapshotManager>(
          metadata_manager_, storage_, shard_manager_, edge_store_,
          node_manager_, schema_registry_);
    }
  }

  // Get a copy of the current configuration
  DatabaseConfig get_config() const { return config_; }

  std::shared_ptr<SchemaRegistry> get_schema_registry() {
    return schema_registry_;
  }

  std::shared_ptr<MetadataManager> get_metadata_manager() {
    return metadata_manager_;
  }

  arrow::Result<bool> initialize() {
    if (persistence_enabled_) {
      auto storage_init = this->storage_->initialize();
      if (!storage_init.ok()) {
        return storage_init.status();
      }

      auto metadata_init = this->metadata_manager_->initialize();
      if (!metadata_init.ok()) {
        return metadata_init.status();
      }

      auto snapshot_init = this->snapshot_manager_->initialize();
      if (!snapshot_init.ok()) {
        return snapshot_init.status();
      }
    }
    return true;
  }

  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> &data) {
    if (schema_name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }
    ARROW_ASSIGN_OR_RAISE(auto node, node_manager_->create_node(
                                         schema_name, data, schema_registry_));
    ARROW_RETURN_NOT_OK(shard_manager_->insert_node(node));
    return node;
  }

  arrow::Result<bool> update_node(
      const std::shared_ptr<BaseOperation> &update) {
    return shard_manager_->update_node(update);
  }

  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id) {
    if (auto res = node_manager_->remove_node(node_id); !res) {
      return arrow::Status::Invalid("Failed to remove node: {}", node_id);
    }
    return shard_manager_->remove_node(schema_name, node_id);
  }

  arrow::Result<bool> connect(int64_t source_id, const std::string &type,
                              int64_t target_id) {
    const auto edge =
        edge_store_->create_edge(source_id, type, target_id).ValueOrDie();
    ARROW_RETURN_NOT_OK(edge_store_->add(edge));
    return true;
  }

  arrow::Result<bool> remove_edge(int64_t edge_id) {
    return edge_store_->remove(edge_id);
  }

  arrow::Result<bool> compact(const std::string &schema_name) {
    return shard_manager_->compact(schema_name);
  }

  // internal api
  [[nodiscard]] std::shared_ptr<EdgeStore> get_edge_store() const {
    return edge_store_;
  }

  [[nodiscard]] std::shared_ptr<ShardManager> get_shard_manager() const {
    return shard_manager_;
  }

  // Compact all schemas in the database
  arrow::Result<bool> compact_all() { return shard_manager_->compact_all(); }

  // Get a table for all nodes of a given schema
  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &schema_name, size_t chunk_size = 10000) const {
    // Get the schema
    ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry_->get(schema_name));

    // First, get all nodes for the schema (this gets from all shards)
    ARROW_ASSIGN_OR_RAISE(auto all_nodes,
                          shard_manager_->get_nodes(schema_name));

    if (all_nodes.empty()) {
      // No data in any shards, return empty table
      std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
      for (int i = 0; i < schema->num_fields(); i++) {
        empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(
            std::vector<std::shared_ptr<arrow::Array>>{}));
      }
      return arrow::Table::Make(schema, empty_columns);
    }

    // Sort the nodes by ID to ensure consistent ordering
    std::sort(all_nodes.begin(), all_nodes.end(),
              [](const std::shared_ptr<Node> &a,
                 const std::shared_ptr<Node> &b) { return a->id < b->id; });

    // Create a table directly from the sorted nodes
    return create_table(schema, all_nodes, chunk_size);
  }

  // Get information about shards for a schema
  arrow::Result<size_t> get_shard_count(const std::string &schema_name) const {
    if (!schema_registry_->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager_->get_shard_count(schema_name);
  }

  // Get sizes of all shards for a schema
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const {
    if (!schema_registry_->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager_->get_shard_sizes(schema_name);
  }

  // Get the min/max IDs of all shards for a schema
  arrow::Result<std::vector<std::pair<int64_t, int64_t>>> get_shard_ranges(
      const std::string &schema_name) const {
    if (!schema_registry_->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager_->get_shard_ranges(schema_name);
  }

  arrow::Result<Snapshot> create_snapshot() {
    return snapshot_manager_->commit();
  }

  arrow::Result<std::shared_ptr<QueryResult>> query(const Query &query) const;
};

}  // namespace tundradb
