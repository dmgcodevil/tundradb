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
#include <nlohmann/json.hpp>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.hpp"
#include "edge_store.hpp"
#include "file_utils.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "node.hpp"
#include "query.hpp"
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

// Schema registry class for managing node schemas
class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schemas;

 public:
  SchemaRegistry() = default;

  // Add a schema with the given name
  arrow::Result<bool> add(const std::string &name,
                          std::shared_ptr<arrow::Schema> schema) {
    schemas.insert(std::make_pair(name, schema));
    return {true};
  }

  // Get a schema by name, returning an error if not found
  arrow::Result<std::shared_ptr<arrow::Schema>> get(
      const std::string &name) const {
    auto it = schemas.find(name);
    if (it == schemas.end()) {
      return arrow::Status::KeyError("Schema not found: ", name);
    }
    return it->second;
  }

  // Check if a schema exists
  [[nodiscard]] bool exists(const std::string &name) const {
    return schemas.find(name) != schemas.end();
  }

  // Get all schema names
  [[nodiscard]] std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(schemas.size());
    for (const auto &schema_pair : schemas) {
      names.push_back(schema_pair.first);
    }
    return names;
  }
};

// NodeManager class for managing node creation and ID assignment
class NodeManager {
 public:
  NodeManager() = default;

  arrow::Result<std::shared_ptr<Node>> get_node(int64_t id) {
    return nodes[id];
  }

  bool add_node(std::shared_ptr<Node> node) {
    // todo check if node exists
    nodes[node->id] = node;
    return true;
  }

  bool remove_node(int64_t id) { return nodes.erase(id) > 0; }

  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> &data,
      std::shared_ptr<SchemaRegistry> schema_registry) {
    if (schema_name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }

    ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry->get(schema_name));
    if (data.contains("id")) {
      return arrow::Status::Invalid("'id' column is auto generated");
    }

    std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
        normalized_data;
    for (auto field : schema->fields()) {
      if (field->name() != "id" && !field->nullable() &&
          (!data.contains(field->name()) ||
           data.find(field->name())->second->IsNull(0))) {
        return arrow::Status::Invalid("Field '", field->name(),
                                      "' is required");
      }
      if (!data.contains(field->name())) {
        normalized_data[field->name()] =
            create_null_array(field->type()).ValueOrDie();
      } else {
        auto array = data.find(field->name())->second;
        if (!array->type()->Equals(field->type())) {
          return arrow::Status::Invalid("Type mismatch for field '",
                                        field->name(), "'. Expected ",
                                        field->type()->ToString(), " but got ",
                                        array->type()->ToString());
        }
        normalized_data[field->name()] = array;
      }
    }

    auto id = id_counter.fetch_add(1);
    normalized_data["id"] = create_int64_array(id).ValueOrDie();
    auto node = std::make_shared<Node>(id, schema_name, normalized_data);
    nodes[id] = node;
    return node;
  }

  void set_id_counter(int64_t value) { id_counter.store(value); }
  int64_t get_id_counter() const { return id_counter.load(); }

 private:
  std::atomic<int64_t> id_counter{0};
  std::unordered_map<int64_t, std::shared_ptr<Node>> nodes;
};

class SnapshotManager {
 public:
  explicit SnapshotManager(std::shared_ptr<MetadataManager> metadata_manager,
                           std::shared_ptr<Storage> storage,
                           std::shared_ptr<ShardManager> shard_manager,
                           std::shared_ptr<EdgeStore> edge_store,
                           std::shared_ptr<NodeManager> node_manager)
      : metadata_manager(std::move(metadata_manager)),
        storage(std::move(storage)),
        shard_manager(std::move(shard_manager)),
        edge_store(std::move(edge_store)),
        node_manager(std::move(node_manager)) {}

  arrow::Result<bool> initialize();
  arrow::Result<Snapshot> commit();
  Snapshot *current_snapshot();
  std::shared_ptr<Manifest> get_manifest();

 private:
  std::shared_ptr<MetadataManager> metadata_manager;
  std::shared_ptr<Storage> storage;
  std::shared_ptr<ShardManager> shard_manager;
  std::shared_ptr<EdgeStore> edge_store;
  std::shared_ptr<NodeManager> node_manager;
  Metadata metadata;
  std::shared_ptr<Manifest> manifest;
  std::shared_ptr<EdgeMetadata> edge_metada;
};

class Shard {
 private:
  std::pmr::monotonic_buffer_resource memory_pool;
  std::pmr::unordered_map<int64_t, std::shared_ptr<Node>> nodes;
  std::set<int64_t> nodes_ids;
  std::atomic<bool> dirty{false};
  std::shared_ptr<arrow::Table> table;
  std::shared_ptr<SchemaRegistry> schema_registry;
  int64_t updated_ts = now_millis();
  bool updated = true;  // todo should be false when we read from snaptshot and
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
        memory_pool(buffer_size),
        nodes(&memory_pool),
        schema_registry(std::move(schema_registry)),
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
        memory_pool(config.get_shard_memory_pool_size()),
        nodes(&memory_pool),
        schema_registry(std::move(schema_registry)),
        schema_name(schema_name) {}

  ~Shard() {
    // Clear the nodes map first to release node resources
    nodes.clear();

    // Clear the nodes_ids set
    nodes_ids.clear();

    // Clear the table
    table.reset();

    // The memory_pool will be automatically destroyed
    // The schema_registry is a shared_ptr, so it will be handled by reference
    // counting
  }

  bool is_updated() const { return updated; }
  bool set_updated(bool v) {
    updated = v;
    return updated;
  }
  int64_t get_updated_ts() const { return updated_ts; }

  std::string compound_id() const {
    return this->schema_name + "-" + std::to_string(this->id);
  }

  arrow::Result<bool> add(const std::shared_ptr<Node> &node) {
    if (node->id < min_id || node->id > max_id) {
      return arrow::Status::Invalid("Node id is out of range");
    }
    if (nodes.contains(node->id)) {
      return arrow::Status::KeyError("Node already exists: ", node->id);
    }
    if (nodes.size() >= capacity) {
      return arrow::Status::KeyError("Shard is full");
    }
    nodes.insert(std::make_pair(node->id, node));
    nodes_ids.insert(node->id);
    dirty = true;
    updated = true;
    return true;
  }

  arrow::Result<bool> extend(const std::shared_ptr<Node> &node) {
    if (nodes.contains(node->id)) {
      return arrow::Status::KeyError("Node already exists: ", node->id);
    }
    if (nodes.size() >= capacity) {
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

    nodes.insert(std::make_pair(node->id, node));
    nodes_ids.insert(node->id);

    dirty = true;
    updated = true;
    updated_ts = now_millis();
    return true;
  }

  arrow::Result<std::shared_ptr<Node>> remove(int64_t id) {
    auto it = nodes.find(id);
    if (it == nodes.end()) {
      return arrow::Status::Invalid("Node not found: ", id);
    }
    auto node = it->second;
    nodes.erase(id);
    nodes_ids.erase(id);
    dirty = true;
    return node;
  }

  arrow::Result<std::shared_ptr<Node>> poll_first() {
    if (nodes_ids.empty()) {
      return arrow::Status::Invalid("Shard is empty");
    }
    auto first = nodes_ids.begin();
    auto node_id = *first;
    nodes_ids.erase(first);
    auto node = nodes[node_id];
    nodes.erase(node_id);

    // Update the min_id to the next minimum if available
    if (!nodes_ids.empty()) {
      min_id = *nodes_ids.begin();
    }

    dirty = true;
    updated = true;
    updated_ts = now_millis();
    return node;
  }

  arrow::Result<bool> update(const std::shared_ptr<BaseOperation> &update) {
    updated = true;
    if (!nodes.contains(update->node_id)) {
      return arrow::Status::KeyError("Node not found: ", update->node_id);
    }
    dirty = true;
    updated = true;
    updated_ts = now_millis();
    return nodes[update->node_id]->update(update);
  }

  arrow::Result<std::shared_ptr<arrow::Table>> get_table() {
    if (dirty || !table) {
      ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry->get(schema_name));
      std::vector<std::shared_ptr<Node>> result;
      std::ranges::transform(nodes, std::back_inserter(result),
                             [](const auto &pair) { return pair.second; });

      // Sort nodes by ID in ascending order
      std::sort(result.begin(), result.end(),
                [](const std::shared_ptr<Node> &a,
                   const std::shared_ptr<Node> &b) { return a->id < b->id; });

      ARROW_ASSIGN_OR_RAISE(table, create_table(schema, result, chunk_size));
      dirty = false;
    }
    return table;
  }

  size_t size() const { return nodes.size(); }

  bool has_space() const { return nodes.size() < capacity; }

  bool empty() const { return nodes.empty(); }

  std::vector<std::shared_ptr<Node>> get_nodes() const {
    std::vector<std::shared_ptr<Node>> result;
    result.reserve(nodes.size());
    for (const auto &[_, node] : nodes) {
      result.push_back(node);
    }
    return result;
  }
};

class ShardManager {
 private:
  std::pmr::monotonic_buffer_resource memory_pool;
  std::pmr::unordered_map<std::string, std::vector<std::shared_ptr<Shard>>>
      shards;
  std::shared_ptr<SchemaRegistry> schema_registry;
  const size_t shard_capacity;
  const size_t chunk_size;
  const DatabaseConfig config;
  std::atomic<int64_t> id_counter{
      0};  // Global unique ID counter for all shards
  std::unordered_map<std::string, std::atomic<int64_t>>
      index_counters;                      // Per-schema index/position counter
  mutable std::mutex index_counter_mutex;  // todo use tbb map instead

  void create_new_shard(const std::shared_ptr<Node> &node) {
    auto new_min_id = node->id;
    auto new_max_id = node->id + shard_capacity - 1;

    // Get the next index for this schema
    int64_t shard_index;
    {
      std::lock_guard<std::mutex> lock(index_counter_mutex);
      shard_index = index_counters[node->schema_name]++;
    }

    // Create shard with global unique ID and schema-specific index
    auto shard = std::make_shared<Shard>(id_counter.fetch_add(1), shard_index,
                                         config, new_min_id, new_max_id,
                                         node->schema_name, schema_registry);

    auto result = shard->add(node);
    if (!result.ok()) {
      // Log error - this shouldn't happen with newly created shard
      std::cerr << "Error adding node to new shard: "
                << result.status().ToString() << std::endl;
    }

    shards[node->schema_name].push_back(shard);
  }

 public:
  explicit ShardManager(std::shared_ptr<SchemaRegistry> schema_registry,
                        const DatabaseConfig &config)
      : memory_pool(config.get_manager_memory_pool_size()),
        shards(&memory_pool),
        schema_registry(std::move(schema_registry)),
        shard_capacity(config.get_shard_capacity()),
        chunk_size(config.get_chunk_size()),
        config(config) {}

  void set_id_counter(int64_t value) { id_counter.store(value); }
  int64_t get_id_counter() const { return id_counter.load(); }

  void set_index_counter(const std::string &schema_name, int64_t value) {
    std::lock_guard<std::mutex> lock(index_counter_mutex);
    index_counters[schema_name].store(value);
  }

  arrow::Result<std::shared_ptr<Shard>> get_shard(
      const std::string &schema_name, int64_t id) {
    return shards[schema_name][id];
  }

  int64_t get_index_counter(const std::string &schema_name) const {
    std::lock_guard<std::mutex> lock(index_counter_mutex);
    auto it = index_counters.find(schema_name);
    return it != index_counters.end() ? it->second.load() : 0;
  }

  // Get all schema names that have shards
  std::vector<std::string> get_schema_names() const {
    std::vector<std::string> schema_names;
    schema_names.reserve(shards.size());
    for (const auto &[schema_name, _] : shards) {
      schema_names.push_back(schema_name);
    }
    return schema_names;
  }

  // Get shards for a given schema (returns a copy)
  arrow::Result<std::vector<std::shared_ptr<Shard>>> get_shards(
      const std::string &schema_name) const {
    auto it = shards.find(schema_name);
    if (it == shards.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }
    return it->second;
  }

  arrow::Result<bool> is_shard_clean(std::string s, int64_t id) {
    return !shards[s][id]->is_updated();
  }

  arrow::Result<bool> compact(const std::string &schema_name) {
    auto it = shards.find(schema_name);
    if (it == shards.end()) {
      return arrow::Status::Invalid("Shard not found for the given schema: ",
                                    schema_name);
    }

    auto &shard_list = it->second;  // Use reference to modify actual collection
    if (shard_list.size() <= 1) {
      // nothing to compact
      return true;
    }

    for (size_t i = 1; i < shard_list.size(); i++) {
      auto &prev = shard_list[i - 1];
      auto &curr = shard_list[i];

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
    std::vector<std::string> schema_names = schema_registry->get_schema_names();
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
    auto it = shards.find(node->schema_name);
    if (it == shards.end()) {
      std::cout << " Create new schema entry for: " << node->schema_name
                << std::endl;
      shards[node->schema_name] = std::vector<std::shared_ptr<Shard>>();
      create_new_shard(node);
      return true;
    }

    auto &shard_list = it->second;
    if (shard_list.empty()) {
      std::cout << "shard is empty schema: " << node->schema_name << std::endl;
      create_new_shard(node);
      return true;
    }

    // First try to find shards that can directly add the node (ID is in range)
    for (auto &shard : shard_list) {
      if (node->id >= shard->min_id && node->id <= shard->max_id &&
          shard->has_space()) {
        auto result = shard->add(node);
        if (result.ok()) {
          log_debug("node id: '" + std::to_string(node->id) +
                    "' inserted to shard id: " + std::to_string(shard->id));
          return true;
        }
        // If there was an error, we'll try the next shard
      }
    }

    std::cout << "no shard with space to insert node: " << node->id
              << std::endl;

    // If no shard can directly add the node, try to find a shard that has space
    // and can be extended with this node ID
    for (auto &shard : shard_list) {
      if (shard->has_space()) {
        // If node ID is higher than max_id, we can extend the shard
        if (node->id > shard->max_id) {
          auto result = shard->extend(node);
          if (result.ok()) {
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
    auto schema_it = shards.find(schema_name);
    if (schema_it == shards.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }

    // Search through all shards for this schema
    for (auto &shard : schema_it->second) {
      if (node_id >= shard->min_id && node_id <= shard->max_id) {
        // This is the right shard range, check if node exists
        try {
          auto node_result = shard->remove(node_id);
          if (node_result.ok()) {
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
    auto schema_it = shards.find(schema_name);
    if (schema_it == shards.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }

    // Search through all shards for this schema
    for (auto &shard : schema_it->second) {
      if (node_id >= shard->min_id && node_id <= shard->max_id) {
        // This is the right shard range, try to remove
        auto remove_result = shard->remove(node_id);
        if (remove_result.ok()) {
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
    for (auto &[schema_name, schema_shards] : shards) {
      // Find the right shard
      for (auto &shard : schema_shards) {
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
    auto schema_it = shards.find(schema_name);
    if (schema_it == shards.end()) {
      return arrow::Status::KeyError("Schema '", schema_name,
                                     "' not found in shards");
    }

    std::vector<std::shared_ptr<Node>> result;
    // Reserve space for efficiency
    size_t total_estimated_nodes = 0;
    for (auto &shard : schema_it->second) {
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
    auto schema_it = shards.find(schema_name);
    if (schema_it == shards.end()) {
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
    auto it = shards.find(schema_name);
    return it != shards.end() && !it->second.empty();
  }

  // Get information about shards for a schema
  arrow::Result<size_t> get_shard_count(const std::string &schema_name) const {
    if (!has_shards(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shards.find(schema_name)->second.size();
  }

  // Get sizes of all shards for a schema
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const {
    if (!has_shards(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    std::vector<size_t> sizes;
    for (const auto &shard : shards.find(schema_name)->second) {
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
    for (const auto &shard : shards.find(schema_name)->second) {
      ranges.push_back({shard->min_id, shard->max_id});
    }
    return ranges;
  }

  // Add a pre-existing shard directly to the shard manager
  arrow::Result<bool> add_shard(const std::shared_ptr<Shard> &shard) {
    if (!shard) {
      return arrow::Status::Invalid("Cannot add null shard");
    }

    // Add shard to the appropriate schema's shard list
    shards[shard->schema_name].push_back(shard);
    return true;
  }

  // Reset the updated flag for all shards
  arrow::Result<bool> reset_all_updated() {
    log_info("Resetting 'updated' flag for all shards");
    for (auto &[schema_name, schema_shards] : shards) {
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
  std::shared_ptr<SchemaRegistry> schema_registry;

  // Shard manager for node storage
  std::shared_ptr<ShardManager> shard_manager;

  // Node manager for ID management
  std::shared_ptr<NodeManager> node_manager;

  // Database configuration
  DatabaseConfig config;

  // Whether persistence is enabled
  bool persistence_enabled;

  // Storage for persistence
  std::shared_ptr<Storage> storage;
  std::shared_ptr<MetadataManager> metadata_manager;
  std::shared_ptr<SnapshotManager> snapshot_manager;
  std::shared_ptr<EdgeStore> edge_store;

 public:
  // Constructor that takes a DatabaseConfig
  explicit Database(const DatabaseConfig &config = DatabaseConfig())
      : schema_registry(std::make_shared<SchemaRegistry>()),
        shard_manager(std::make_shared<ShardManager>(schema_registry, config)),
        node_manager(std::make_shared<NodeManager>()),
        edge_store(std::make_shared<EdgeStore>(0, config.get_chunk_size())),
        config(config),
        persistence_enabled(config.is_persistence_enabled()) {
    if (persistence_enabled) {
      const std::string &db_path = config.get_db_path();
      if (db_path.empty()) {
        log_error("Database path is empty but persistence is enabled");
        persistence_enabled = false;
        return;
      }

      std::string data_path = db_path + "/data";
      storage = std::make_shared<Storage>(std::move(data_path), schema_registry,
                                          config);
      metadata_manager = std::make_shared<MetadataManager>(db_path);
      snapshot_manager = std::make_shared<SnapshotManager>(
          metadata_manager, storage, shard_manager, edge_store, node_manager);
    }
  }

  // Get a copy of the current configuration
  DatabaseConfig get_config() const { return config; }

  std::shared_ptr<SchemaRegistry> get_schema_registry() {
    return schema_registry;
  }

  arrow::Result<bool> initialize() {
    if (persistence_enabled) {
      auto storage_init = this->storage->initialize();
      if (!storage_init.ok()) {
        return storage_init.status();
      }

      auto metadata_init = this->metadata_manager->initialize();
      if (!metadata_init.ok()) {
        return metadata_init.status();
      }

      auto snapshot_init = this->snapshot_manager->initialize();
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

    ARROW_ASSIGN_OR_RAISE(auto node, node_manager->create_node(
                                         schema_name, data, schema_registry));

    // Insert node into shards
    ARROW_RETURN_NOT_OK(shard_manager->insert_node(node));

    return node;
  }

  arrow::Result<bool> update_node(
      const std::shared_ptr<BaseOperation> &update) {
    return shard_manager->update_node(update);
  }

  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id) {
    return shard_manager->remove_node(schema_name, node_id);
  }

  arrow::Result<bool> connect(int64_t source_id, const std::string &type,
                              int64_t target_id) {
    auto edge =
        edge_store->create_edge(source_id, type, target_id).ValueOrDie();
    ARROW_RETURN_NOT_OK(edge_store->add(edge));
    return true;
  }

  arrow::Result<bool> compact(const std::string &schema_name) {
    return shard_manager->compact(schema_name);
  }

  // internal api
  [[nodiscard]] std::shared_ptr<EdgeStore> get_edge_store() const {
    return edge_store;
  }

  [[nodiscard]] std::shared_ptr<ShardManager> get_shard_manager() const {
    return shard_manager;
  }

  // Compact all schemas in the database
  arrow::Result<bool> compact_all() { return shard_manager->compact_all(); }

  // Get a table for all nodes of a given schema
  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &schema_name, size_t chunk_size = 10000) {
    // Get the schema
    ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry->get(schema_name));

    // First, get all nodes for the schema (this gets from all shards)
    ARROW_ASSIGN_OR_RAISE(auto all_nodes,
                          shard_manager->get_nodes(schema_name));

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
    if (!schema_registry->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager->get_shard_count(schema_name);
  }

  // Get sizes of all shards for a schema
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const {
    if (!schema_registry->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager->get_shard_sizes(schema_name);
  }

  // Get the min/max IDs of all shards for a schema
  arrow::Result<std::vector<std::pair<int64_t, int64_t>>> get_shard_ranges(
      const std::string &schema_name) const {
    if (!schema_registry->exists(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager->get_shard_ranges(schema_name);
  }

  // Create a new snapshot of the current state
  arrow::Result<Snapshot> create_snapshot() {
    return snapshot_manager->commit();
  }

  arrow::Result<std::shared_ptr<QueryResult>> query(const Query &query);
};

// Helper function to print a single row
static void print_row(const std::shared_ptr<arrow::Table> &table,
                      int64_t row_index) {
  for (int j = 0; j < table->num_columns(); ++j) {
    auto column = table->column(j);
    if (!column || column->num_chunks() == 0) {
      std::cout << "NULL\t";
      continue;
    }

    // Find the chunk containing this row
    int64_t accumulated_length = 0;
    std::shared_ptr<arrow::Array> chunk;
    int64_t chunk_offset = row_index;

    for (int c = 0; c < column->num_chunks(); ++c) {
      chunk = column->chunk(c);
      if (accumulated_length + chunk->length() > row_index) {
        chunk_offset = row_index - accumulated_length;
        break;
      }
      accumulated_length += chunk->length();
    }

    if (!chunk || chunk_offset >= chunk->length() ||
        chunk->IsNull(chunk_offset)) {
      std::cout << "NULL\t";
      continue;
    }

    switch (chunk->type_id()) {
      case arrow::Type::INT32:
        std::cout << std::static_pointer_cast<arrow::Int32Array>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::INT64:
        std::cout << std::static_pointer_cast<arrow::Int64Array>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::FLOAT:
        std::cout << std::static_pointer_cast<arrow::FloatArray>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::DOUBLE:
        std::cout << std::static_pointer_cast<arrow::DoubleArray>(chunk)->Value(
            chunk_offset);
        break;
      case arrow::Type::BOOL:
        std::cout << (std::static_pointer_cast<arrow::BooleanArray>(chunk)
                              ->Value(chunk_offset)
                          ? "true"
                          : "false");
        break;
      case arrow::Type::STRING:
        try {
          auto str_array = std::static_pointer_cast<arrow::StringArray>(chunk);
          if (chunk_offset < str_array->length()) {
            std::cout << str_array->GetString(chunk_offset);
          } else {
            std::cout << "NULL";
          }
        } catch (const std::exception &e) {
          std::cout << "ERROR";
        }
        break;
      case arrow::Type::TIMESTAMP:
        std::cout << std::static_pointer_cast<arrow::TimestampArray>(chunk)
                         ->Value(chunk_offset);
        break;
      default:
        std::cout << "Unsupported";
    }
    std::cout << "\t";
  }
  std::cout << std::endl;
}

static void print_table(const std::shared_ptr<arrow::Table> &table,
                        int64_t max_rows = 100) {
  if (!table) {
    std::cout << "Null table" << std::endl;
    return;
  }

  std::cout << "Table Schema:" << std::endl;
  std::cout << table->schema()->ToString() << std::endl;

  // Print chunk information
  std::cout << "\nChunk Information:" << std::endl;
  for (int j = 0; j < table->num_columns(); ++j) {
    auto column = table->column(j);
    std::cout << "Column '" << table->schema()->field(j)->name()
              << "': " << column->num_chunks();

    if (column->num_chunks() > 0) {
      std::cout << " chunk sizes = [ ";
      for (int c = 0; c < column->num_chunks(); c++) {
        std::cout << column->chunk(c)->length();
        if (c < column->num_chunks() - 1) std::cout << ", ";
      }
      std::cout << " ]";
    }
    std::cout << std::endl;
  }

  const int64_t total_rows = table->num_rows();
  std::cout << "\nTable Data (" << total_rows << " rows):" << std::endl;

  try {
    // Determine how many rows to print
    bool use_ellipsis = max_rows > 0 && total_rows > max_rows;
    int64_t rows_to_print = use_ellipsis ? max_rows / 2 : total_rows;

    std::cout << "First " << rows_to_print << " rows:" << std::endl;
    // Print first half of rows
    for (int64_t i = 0; i < rows_to_print && i < total_rows; ++i) {
      print_row(table, i);
    }

    // Print ellipsis and last half of rows if needed
    if (use_ellipsis) {
      std::cout << "....\n" << std::endl;

      std::cout << "Last " << rows_to_print << " rows:" << std::endl;
      for (int64_t i = total_rows - rows_to_print; i < total_rows; ++i) {
        print_row(table, i);
      }
    }
  } catch (const std::exception &e) {
    std::cout << "Error while printing table: " << e.what() << std::endl;
  }
}

}  // namespace tundradb
