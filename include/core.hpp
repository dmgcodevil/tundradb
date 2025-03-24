#ifndef CORE_HPP
#define CORE_HPP
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <algorithm>
#include <iostream>
#include <memory_resource>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace tundradb {
class Database;
class Node;
class Shard;
class ShardManager;

// Default configuration constants
namespace defaults {
constexpr size_t SHARD_CAPACITY = 100000;
constexpr size_t CHUNK_SIZE = 10000;
constexpr size_t SHARD_MEMORY_POOL_SIZE = 10 * 1024 * 1024;       // 10 MB
constexpr size_t MANAGER_MEMORY_POOL_SIZE = 100 * 1024 * 1024;    // 100 MB
constexpr size_t DATABASE_MEMORY_POOL_SIZE = 1024 * 1024 * 1024;  // 1 GB
}  // namespace defaults

// Configuration parameters for the database
class DatabaseConfig {
 private:
  // Maximum number of nodes per shard
  size_t shard_capacity = defaults::SHARD_CAPACITY;

  // Size of chunks when creating tables
  size_t chunk_size = defaults::CHUNK_SIZE;

  // Memory pool size for shards (in bytes)
  size_t shard_memory_pool_size = defaults::SHARD_MEMORY_POOL_SIZE;

  // Memory pool size for shard manager (in bytes)
  size_t manager_memory_pool_size = defaults::MANAGER_MEMORY_POOL_SIZE;

  // Memory pool size for database (in bytes)
  size_t database_memory_pool_size = defaults::DATABASE_MEMORY_POOL_SIZE;

  // Allow DatabaseConfigBuilder to modify private fields
  friend class DatabaseConfigBuilder;

 public:
  size_t get_shard_capacity() const { return shard_capacity; }
  size_t get_chunk_size() const { return chunk_size; }
  size_t get_shard_memory_pool_size() const { return shard_memory_pool_size; }
  size_t get_manager_memory_pool_size() const {
    return manager_memory_pool_size;
  }
  size_t get_database_memory_pool_size() const {
    return database_memory_pool_size;
  }
};

// Builder class for DatabaseConfig - separate implementation to avoid circular
// dependency
class DatabaseConfigBuilder {
 private:
  DatabaseConfig config;

 public:
  DatabaseConfigBuilder() = default;

  DatabaseConfigBuilder &with_shard_capacity(size_t capacity) {
    config.shard_capacity = capacity;
    return *this;
  }

  DatabaseConfigBuilder &with_chunk_size(size_t size) {
    config.chunk_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_shard_memory_pool_size(size_t size) {
    config.shard_memory_pool_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_manager_memory_pool_size(size_t size) {
    config.manager_memory_pool_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_database_memory_pool_size(size_t size) {
    config.database_memory_pool_size = size;
    return *this;
  }

  // Helper for setting all memory sizes with a single scale factor
  DatabaseConfigBuilder &with_memory_scale_factor(double factor) {
    config.shard_memory_pool_size =
        static_cast<size_t>(defaults::SHARD_MEMORY_POOL_SIZE * factor);
    config.manager_memory_pool_size =
        static_cast<size_t>(defaults::MANAGER_MEMORY_POOL_SIZE * factor);
    config.database_memory_pool_size =
        static_cast<size_t>(defaults::DATABASE_MEMORY_POOL_SIZE * factor);
    return *this;
  }

  [[nodiscard]] DatabaseConfig build() const { return config; }
};

// Helper function to create a config builder
inline DatabaseConfigBuilder make_config() { return {}; }


static arrow::Result<std::shared_ptr<arrow::Array>> create_int64_array(
    const int64_t value) {
  arrow::Int64Builder int64_builder;
  ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
  ARROW_RETURN_NOT_OK(int64_builder.Append(value));
  std::shared_ptr<arrow::Array> int64_array;
  ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
  return int64_array;
}

static arrow::Result<std::shared_ptr<arrow::Array>> create_str_array(
    const std::string &value) {
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(value));
  std::shared_ptr<arrow::Array> string_arr;
  ARROW_RETURN_NOT_OK(builder.Finish(&string_arr));
  return string_arr;
}

static arrow::Result<std::shared_ptr<arrow::Array>> create_null_array(
    const std::shared_ptr<arrow::DataType> &type) {
  switch (type->id()) {
    case arrow::Type::INT64: {
      arrow::Int64Builder builder;
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      return array;
    }
    case arrow::Type::STRING: {
      arrow::StringBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      return array;
    }
    default:
      return arrow::Status::NotImplemented("Unsupported type: ",
                                           type->ToString());
  }
}

enum OperationType { SET };

// Base operation class
struct BaseOperation {
  int64_t node_id;  // The node identifier to apply the operation to
  std::vector<std::string> field_name;  // Name of the field to update

  BaseOperation(int64_t id, const std::vector<std::string> &field)
      : node_id(id), field_name(field) {}

  virtual arrow::Result<bool> apply(const std::shared_ptr<arrow::Array> &array,
                                    int64_t row_index) const = 0;

  virtual OperationType op_type() const = 0;

  virtual bool should_replace_array() const { return false; }
  virtual std::shared_ptr<arrow::Array> get_replacement_array() const {
    return nullptr;
  }

  virtual ~BaseOperation() = default;
};

struct SetOperation : public BaseOperation {
  std::shared_ptr<arrow::Array> value;  // The new value to set

  SetOperation(int64_t id, const std::vector<std::string> &field,
               const std::shared_ptr<arrow::Array> &v)
      : BaseOperation(id, field), value(v) {}

  arrow::Result<bool> apply(const std::shared_ptr<arrow::Array> &array,
                            int64_t row_index) const override {
    auto array_data = array->data();
    switch (array->type_id()) {
      case arrow::Type::INT64: {
        auto raw_values = array_data->GetMutableValues<int32_t>(1);
        raw_values[row_index] =
            std::static_pointer_cast<arrow::Int32Array>(value)->Value(0);
        return {true};
      }
      default:
        return arrow::Status::Invalid("not implemented");
    }
  }
  OperationType op_type() const override { return OperationType::SET; }
  bool should_replace_array() const override {
    return value->type_id() == arrow::Type::STRING;
  }
  std::shared_ptr<arrow::Array> get_replacement_array() const override {
    return value;
  }
};

class Node {
 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data;

 public:
  int64_t id;
  std::string schema_name;

  explicit Node(const int64_t id, std::string schema_name,
                std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
                    initial_data)
      : id(id),
        schema_name(std::move(schema_name)),
        data(std::move(initial_data)) {}

  ~Node() = default;

  void add_field(const std::string &field_name,
                 const std::shared_ptr<arrow::Array> &value) {
    data.insert(std::make_pair(field_name, value));
  }

  arrow::Result<std::shared_ptr<arrow::Array>> get_field(
      const std::string &field_name) const {
    auto it = data.find(field_name);
    if (it == data.end()) {
      return arrow::Status::KeyError("Field not found: ", field_name);
    }
    return it->second;
  }

  arrow::Result<bool> update(const std::shared_ptr<BaseOperation> &update) {
    if (update->field_name.empty()) {
      return arrow::Status::Invalid("Field name vector is empty");
    }

    auto it = data.find(update->field_name[0]);
    if (it == data.end()) {
      return arrow::Status::KeyError("Field not found: ",
                                     update->field_name[0]);
    }
    if (update->should_replace_array()) {
      data[update->field_name[0]] = update->get_replacement_array();
      return true;
    }
    return update->apply(it->second, 0);
  }
};

class SchemaRegistry {
 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schemas;
  static arrow::Result<std::shared_ptr<arrow::Schema>> prepend_id_field(
      const std::shared_ptr<arrow::Schema> &schema) {
    auto id_field = arrow::field("id", arrow::int64(), false);
    auto fields = schema->fields();
    if (schema->GetFieldIndex("id") != -1) {
      return arrow::Status::Invalid("Schema already contains 'id' field");
    }
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    new_fields.reserve(fields.size() + 1);
    new_fields.push_back(id_field);
    new_fields.insert(new_fields.end(), fields.begin(), fields.end());
    return arrow::schema(new_fields);
  }

 public:
  arrow::Result<bool> add(const std::string &name,
                          const std::shared_ptr<arrow::Schema> &schema) {
    if (name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }
    if (!schema) {
      return arrow::Status::Invalid("Schema cannot be null");
    }
    if (schema->GetFieldIndex("id") != -1) {
      return arrow::Status::Invalid("'id' field name is reserved");
    }
    ARROW_ASSIGN_OR_RAISE(auto final_schema, prepend_id_field(schema));
    auto [it, inserted] = schemas.try_emplace(name, final_schema);
    if (!inserted) {
      return arrow::Status::AlreadyExists("Schema '", name, "' already exists");
    }
    return true;
  }

  // Check if a schema exists
  bool has_schema(const std::string &name) const {
    return schemas.contains(name);
  }

  // Get a schema by name
  arrow::Result<std::shared_ptr<arrow::Schema>> get(
      const std::string &name) const {
    auto it = schemas.find(name);
    if (it == schemas.end()) {
      return arrow::Status::KeyError("Schema '", name, "' not found");
    }
    return it->second;
  }

  // Update existing schema
  arrow::Result<bool> update(const std::string &name,
                             const std::shared_ptr<arrow::Schema> &schema) {
    if (name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }
    if (!schema) {
      return arrow::Status::Invalid("Schema cannot be null");
    }

    auto it = schemas.find(name);
    if (it == schemas.end()) {
      return arrow::Status::KeyError("Schema '", name, "' not found");
    }

    it->second = schema;
    return true;
  }

  // Remove a schema
  arrow::Result<bool> remove(const std::string &name) {
    if (name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }

    if (schemas.erase(name) == 0) {
      return arrow::Status::KeyError("Schema '", name, "' not found");
    }
    return true;
  }

  // Get all schema names
  [[nodiscard]] std::vector<std::string> get_schema_names() const {
    std::vector<std::string> names;
    names.reserve(schemas.size());
    for (const auto &[name, _] : schemas) {
      names.push_back(name);
    }
    return names;
  }
};

static arrow::Result<std::shared_ptr<arrow::Table>> create_table(
    const std::shared_ptr<arrow::Schema> &schema,
    const std::vector<std::shared_ptr<Node>> &nodes, size_t chunk_size = 0) {
  if (nodes.empty()) {
    return arrow::Status::Invalid("Empty nodes list");
  }
  auto field_names = schema->field_names();
  if (field_names.empty()) {
    return arrow::Status::Invalid("Empty field names list");
  }

  // Collect arrays for each field
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> field_arrays(
      field_names.size());

  // For each field
  for (size_t field_idx = 0; field_idx < field_names.size(); ++field_idx) {
    const auto &field_name = field_names[field_idx];

    // For each node
    for (const auto &node : nodes) {
      auto field_result = node->get_field(field_name);
      if (!field_result.ok()) {
        return field_result.status();
      }
      field_arrays[field_idx].push_back(field_result.ValueOrDie());
    }
  }

  // Concatenate arrays for each field
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
  for (const auto &arrays : field_arrays) {
    if (chunk_size <= 0) {
      // Single chunk - concatenate all arrays
      ARROW_ASSIGN_OR_RAISE(auto concat_array, arrow::Concatenate(arrays));
      columns.push_back(std::make_shared<arrow::ChunkedArray>(concat_array));
    } else {
      // Multiple chunks - create chunks of specified size
      std::vector<std::shared_ptr<arrow::Array>> chunks;
      size_t total_length = 0;
      std::vector<std::shared_ptr<arrow::Array>> current_chunk;

      for (const auto &array : arrays) {
        current_chunk.push_back(array);
        total_length += array->length();

        if (total_length >= chunk_size) {
          ARROW_ASSIGN_OR_RAISE(auto concat_chunk,
                                arrow::Concatenate(current_chunk));
          chunks.push_back(concat_chunk);
          current_chunk.clear();
          total_length = 0;
        }
      }

      // Handle remaining arrays
      if (!current_chunk.empty()) {
        ARROW_ASSIGN_OR_RAISE(auto concat_chunk,
                              arrow::Concatenate(current_chunk));
        chunks.push_back(concat_chunk);
      }

      columns.push_back(std::make_shared<arrow::ChunkedArray>(chunks));
    }
  }

  return arrow::Table::Make(schema, columns);
}

class Shard {
 private:
  std::pmr::monotonic_buffer_resource memory_pool;
  std::pmr::unordered_map<int64_t, std::shared_ptr<Node>> nodes;
  std::set<int64_t> nodes_ids;
  std::atomic<bool> dirty{false};
  std::shared_ptr<arrow::Table> table;
  std::shared_ptr<SchemaRegistry> schema_registry;
  std::string schema_name;

 public:
  int64_t min_id;
  int64_t max_id;
  const size_t capacity;
  const size_t chunk_size;

  Shard(size_t capacity, int64_t min_id, int64_t max_id, size_t chunk_size,
        const std::string &schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry,
        size_t buffer_size = 10 * 1024 * 1024)
      : capacity(capacity),
        min_id(min_id),
        max_id(max_id),
        chunk_size(chunk_size),
        memory_pool(buffer_size),
        nodes(&memory_pool),
        schema_registry(std::move(schema_registry)),
        schema_name(schema_name) {}

  // Constructor that uses DatabaseConfig
  Shard(const DatabaseConfig &config, int64_t min_id, int64_t max_id,
        const std::string &schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry)
      : capacity(config.get_shard_capacity()),
        min_id(min_id),
        max_id(max_id),
        chunk_size(config.get_chunk_size()),
        memory_pool(config.get_shard_memory_pool_size()),
        nodes(&memory_pool),
        schema_registry(std::move(schema_registry)),
        schema_name(schema_name) {}

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
    return true;
  }

  arrow::Result<bool> extend(const std::shared_ptr<Node> &node) {
    if (node->id < min_id) {
      return arrow::Status::Invalid("Node id is below the minimum range");
    }
    if (nodes.contains(node->id)) {
      return arrow::Status::KeyError("Node already exists: ", node->id);
    }
    if (nodes.size() >= capacity) {
      return arrow::Status::KeyError("Shard is full");
    }
    nodes.insert(std::make_pair(node->id, node));
    nodes_ids.insert(node->id);
    max_id = std::max(max_id, node->id);
    dirty = true;
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
    return node;
  }

  arrow::Result<bool> update(const std::shared_ptr<BaseOperation> &update) {
    if (!nodes.contains(update->node_id)) {
      return arrow::Status::KeyError("Node not found: ", update->node_id);
    }
    dirty = true;
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

  void create_new_shard(const std::shared_ptr<Node> &node) {
    auto new_min_id = node->id;
    auto new_max_id = node->id + shard_capacity - 1;

    auto shard = std::make_shared<Shard>(config, new_min_id, new_max_id,
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

  arrow::Result<bool> compact(const std::string &schema_name) {
    auto it = shards.find(schema_name);
    if (it == shards.end()) {
      return arrow::Status::Invalid("Shard not found for the given schema: ",
                                    schema_name);
    }

    auto &shard_list = it->second;  // Use reference to modify actual collection
    if (shard_list.empty()) {
      return true;
    }

    // First pass: move nodes from later shards to fill earlier shards
    for (size_t i = 0; i < shard_list.size(); i++) {
      auto &shard = shard_list[i];
      if (!shard->has_space()) {
        continue;  // Skip full shards
      }

      // Try to fill this shard from later shards
      for (size_t j = i + 1; j < shard_list.size(); j++) {
        if (!shard_list[j]->empty()) {
          while (!shard_list[j]->empty() && shard->has_space()) {
            auto node_result = shard_list[j]->poll_first();
            if (!node_result.ok()) {
              break;  // Error polling from source shard
            }

            auto extend_result = shard->extend(node_result.ValueOrDie());
            if (!extend_result.ok()) {
              break;  // Error extending target shard
            }
          }
        }
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

  arrow::Result<bool> insert_node(const std::shared_ptr<Node> &node) {
    auto it = shards.find(node->schema_name);
    if (it == shards.end()) {
      // Create new schema entry if it doesn't exist
      shards[node->schema_name] = std::vector<std::shared_ptr<Shard>>();
      create_new_shard(node);
      return true;
    }

    auto &shard_list = it->second;
    if (shard_list.empty()) {
      create_new_shard(node);
      return true;
    }

    // Try to find an appropriate shard based on ID
    for (auto &shard : shard_list) {
      if (node->id >= shard->min_id && node->id <= shard->max_id &&
          shard->has_space()) {
        auto result = shard->add(node);
        if (result.ok()) {
          return true;
        }
        // If there was an error, we'll try the next shard
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
};

class Database {
 private:
  // Schema registry
  std::shared_ptr<SchemaRegistry> schema_registry;

  // Shard manager for node storage
  std::unique_ptr<ShardManager> shard_manager;

  // ID counter for generating node IDs
  std::atomic<int64_t> id_counter;

  // Database configuration
  DatabaseConfig config;

 public:
  // Constructor that takes a DatabaseConfig
  explicit Database(const DatabaseConfig &config = DatabaseConfig())
      : schema_registry(std::make_shared<SchemaRegistry>()),
        shard_manager(std::make_unique<ShardManager>(schema_registry, config)),
        id_counter(0),
        config(config) {}

  // Get a copy of the current configuration
  DatabaseConfig get_config() const { return config; }

  std::shared_ptr<SchemaRegistry> get_schema_registry() {
    return schema_registry;
  }

  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> &data) {
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

  arrow::Result<bool> compact(const std::string &schema_name) {
    return shard_manager->compact(schema_name);
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
    if (!schema_registry->has_schema(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager->get_shard_count(schema_name);
  }

  // Get sizes of all shards for a schema
  arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const {
    if (!schema_registry->has_schema(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager->get_shard_sizes(schema_name);
  }

  // Get the min/max IDs of all shards for a schema
  arrow::Result<std::vector<std::pair<int64_t, int64_t>>> get_shard_ranges(
      const std::string &schema_name) const {
    if (!schema_registry->has_schema(schema_name)) {
      return arrow::Status::Invalid("Schema '", schema_name, "' not found");
    }
    return shard_manager->get_shard_ranges(schema_name);
  }
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

arrow::Result<bool> demo_single_node();

arrow::Result<bool> demo_batch_update();
}  // namespace tundradb

#endif  // CORE_HPP
