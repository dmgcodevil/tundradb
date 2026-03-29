#include "shard.hpp"

#include "logger.hpp"
#include "utils.hpp"

namespace tundradb {

Shard::Shard(int64_t id, int64_t index, size_t capacity, int64_t min_id,
             int64_t max_id, size_t chunk_size, std::string schema_name,
             std::shared_ptr<SchemaRegistry> schema_registry,
             size_t buffer_size)
    : memory_pool_(buffer_size),
      nodes_(&memory_pool_),
      schema_registry_(std::move(schema_registry)),
      updated_ts_(now_millis()),
      id(id),
      index(index),
      min_id(min_id),
      max_id(max_id),
      capacity(capacity),
      chunk_size(chunk_size),
      schema_name(std::move(schema_name)) {}

Shard::Shard(int64_t id, int64_t index, const DatabaseConfig &config,
             int64_t min_id, int64_t max_id, std::string schema_name,
             std::shared_ptr<SchemaRegistry> schema_registry)
    : memory_pool_(config.get_shard_memory_pool_size()),
      nodes_(&memory_pool_),
      schema_registry_(std::move(schema_registry)),
      updated_ts_(now_millis()),
      id(id),
      index(index),
      min_id(min_id),
      max_id(max_id),
      capacity(config.get_shard_capacity()),
      chunk_size(config.get_chunk_size()),
      schema_name(std::move(schema_name)) {}

Shard::~Shard() {
  nodes_.clear();
  nodes_ids_.clear();
  table_.reset();
}

bool Shard::is_updated() const { return updated_; }

bool Shard::set_updated(bool v) {
  updated_ = v;
  return updated_;
}

int64_t Shard::get_updated_ts() const { return updated_ts_; }

std::string Shard::compound_id() const {
  return schema_name + "-" + std::to_string(id);
}

arrow::Result<bool> Shard::add(const std::shared_ptr<Node> &node) {
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

arrow::Result<bool> Shard::extend(const std::shared_ptr<Node> &node) {
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

arrow::Result<std::shared_ptr<Node>> Shard::remove(int64_t id) {
  const auto it = nodes_.find(id);
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

arrow::Result<std::shared_ptr<Node>> Shard::poll_first() {
  if (nodes_ids_.empty()) {
    return arrow::Status::Invalid("Shard is empty");
  }
  const auto first = nodes_ids_.begin();
  const auto node_id = *first;
  nodes_ids_.erase(first);
  auto node = nodes_[node_id];
  nodes_.erase(node_id);

  if (!nodes_ids_.empty()) {
    min_id = *nodes_ids_.begin();
  }

  dirty_ = true;
  updated_ = true;
  updated_ts_ = now_millis();
  return node;
}

arrow::Result<bool> Shard::update(const int64_t node_id,
                                  std::shared_ptr<Field> field,
                                  const Value &value,
                                  const UpdateType update_type) {
  updated_ = true;
  if (!nodes_.contains(node_id)) {
    return arrow::Status::KeyError("Node not found: ", node_id);
  }
  dirty_ = true;
  updated_ = true;
  updated_ts_ = now_millis();
  return nodes_[node_id]->update(field, value, update_type);
}

arrow::Result<bool> Shard::update_fields(
    const int64_t node_id, const std::vector<FieldUpdate> &field_updates,
    const UpdateType update_type) {
  if (!nodes_.contains(node_id)) {
    return arrow::Status::KeyError("Node not found: ", node_id);
  }
  dirty_ = true;
  updated_ = true;
  updated_ts_ = now_millis();
  (void)update_type;
  return nodes_[node_id]->update_fields(field_updates);
}

arrow::Result<std::shared_ptr<arrow::Table>> Shard::get_table(
    TemporalContext *ctx) {
  if (dirty_ || !table_ || ctx) {
    ARROW_ASSIGN_OR_RAISE(const auto schema,
                          schema_registry_->get(schema_name));
    auto arrow_schema = schema->arrow();

    std::vector<std::shared_ptr<Node>> result;
    std::ranges::transform(nodes_, std::back_inserter(result),
                           [](const auto &pair) { return pair.second; });

    std::ranges::sort(
        result, [](const std::shared_ptr<Node> &a,
                   const std::shared_ptr<Node> &b) { return a->id < b->id; });

    ARROW_ASSIGN_OR_RAISE(auto table_res,
                          create_table(schema, result, chunk_size, ctx));

    if (!ctx) {
      table_ = table_res;
      dirty_ = false;
    }

    return table_res;
  }

  return table_;
}

size_t Shard::size() const { return nodes_.size(); }

bool Shard::has_space() const { return nodes_.size() < capacity; }

bool Shard::empty() const { return nodes_.empty(); }

std::vector<std::shared_ptr<Node>> Shard::get_nodes() const {
  std::vector<std::shared_ptr<Node>> result;
  result.reserve(nodes_.size());
  for (const auto &node : nodes_ | std::views::values) {
    result.push_back(node);
  }
  return result;
}

// =========================================================================
// ShardManager
// =========================================================================

ShardManager::ShardManager(std::shared_ptr<SchemaRegistry> schema_registry,
                           const DatabaseConfig &config)
    : memory_pool_(config.get_manager_memory_pool_size()),
      shards_(&memory_pool_),
      schema_registry_(std::move(schema_registry)),
      shard_capacity_(config.get_shard_capacity()),
      chunk_size_(config.get_chunk_size()),
      config_(config) {}

void ShardManager::set_id_counter(const int64_t value) {
  id_counter_.store(value);
}

int64_t ShardManager::get_id_counter() const { return id_counter_.load(); }

void ShardManager::set_index_counter(const std::string &schema_name,
                                     const int64_t value) {
  std::lock_guard lock(index_counter_mutex_);
  index_counters_[schema_name].store(value);
}

int64_t ShardManager::get_index_counter(const std::string &schema_name) const {
  std::lock_guard lock(index_counter_mutex_);
  const auto it = index_counters_.find(schema_name);
  return it != index_counters_.end() ? it->second.load() : 0;
}

arrow::Result<std::shared_ptr<Shard>> ShardManager::get_shard(
    const std::string &schema_name, const int64_t id) {
  return shards_[schema_name][id];
}

std::vector<std::string> ShardManager::get_schema_names() const {
  std::vector<std::string> schema_names;
  schema_names.reserve(shards_.size());
  for (const auto &schema_name : shards_ | std::views::keys) {
    schema_names.push_back(schema_name);
  }
  return schema_names;
}

arrow::Result<std::vector<std::shared_ptr<Shard>>> ShardManager::get_shards(
    const std::string &schema_name) const {
  const auto it = shards_.find(schema_name);
  if (it == shards_.end()) {
    return arrow::Status::KeyError("Schema '", schema_name,
                                   "' not found in shards");
  }
  return it->second;
}

arrow::Result<bool> ShardManager::is_shard_clean(std::string s, int64_t id) {
  return !shards_[s][id]->is_updated();
}

arrow::Result<bool> ShardManager::compact(const std::string &schema_name) {
  const auto it = shards_.find(schema_name);
  if (it == shards_.end()) {
    return arrow::Status::Invalid("Shard not found for the given schema: ",
                                  schema_name);
  }

  auto &shard_list = it->second;
  if (shard_list.size() <= 1) {
    return true;
  }

  for (size_t i = 1; i < shard_list.size(); i++) {
    const auto &prev = shard_list[i - 1];
    const auto &curr = shard_list[i];

    while (prev->has_space() && !curr->empty()) {
      auto node = curr->poll_first().ValueOrDie();
      prev->extend(node).ValueOrDie();
      if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
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
  }

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

arrow::Result<bool> ShardManager::compact_all() {
  const std::vector<std::string> schema_names =
      schema_registry_->get_schema_names();
  bool success = true;

  for (const auto &schema_name : schema_names) {
    if (auto result = compact(schema_name); !result.ok()) {
      log_error("Error compacting schema '{}':{}", schema_name,
                result.status().ToString());
      success = false;
    }
  }

  return success;
}

arrow::Result<bool> ShardManager::insert_node(
    const std::shared_ptr<Node> &node) {
  if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
    log_debug("inserting node id " + std::to_string(node->id));
  }
  const auto it = shards_.find(node->schema_name);
  if (it == shards_.end()) {
    shards_[node->schema_name] = std::vector<std::shared_ptr<Shard>>();
    create_new_shard(node);
    return true;
  }

  const auto &shard_list = it->second;
  if (shard_list.empty()) {
    create_new_shard(node);
    return true;
  }

  for (auto &shard : shard_list) {
    if (node->id >= shard->min_id && node->id <= shard->max_id &&
        shard->has_space()) {
      if (auto result = shard->add(node); result.ok()) {
        if (Logger::get_instance().get_level() == LogLevel::DEBUG) {
          log_debug("node id: '" + std::to_string(node->id) +
                    "' inserted to shard id: " + std::to_string(shard->id));
        }
        return true;
      }
    }
  }

  for (auto &shard : shard_list) {
    if (shard->has_space()) {
      if (node->id > shard->max_id) {
        if (auto result = shard->extend(node); result.ok()) {
          return true;
        }
      }
    }
  }
  create_new_shard(node);
  return true;
}

void ShardManager::create_new_shard(const std::shared_ptr<Node> &node) {
  auto new_min_id = node->id;
  auto new_max_id = node->id + static_cast<int64_t>(shard_capacity_) - 1;

  int64_t shard_index;
  {
    std::lock_guard lock(index_counter_mutex_);
    shard_index = index_counters_[node->schema_name]++;
  }

  auto shard = std::make_shared<Shard>(id_counter_.fetch_add(1), shard_index,
                                       config_, new_min_id, new_max_id,
                                       node->schema_name, schema_registry_);
  auto result = shard->add(node);
  if (!result.ok()) {
    log_error("Error adding node to new shard: {}", result.status().ToString());
  }

  shards_[node->schema_name].push_back(shard);
}

arrow::Result<std::shared_ptr<Node>> ShardManager::get_node(
    const std::string &schema_name, int64_t node_id) {
  const auto schema_it = shards_.find(schema_name);
  if (schema_it == shards_.end()) {
    return arrow::Status::KeyError("Schema '", schema_name,
                                   "' not found in shards");
  }

  for (const auto &shard : schema_it->second) {
    if (node_id >= shard->min_id && node_id <= shard->max_id) {
      try {
        if (auto node_result = shard->remove(node_id); node_result.ok()) {
          return node_result.ValueOrDie();
        }
      } catch (...) {
        // node wasn't in this shard, continue to next shard
      }
    }
  }

  return arrow::Status::KeyError("Node with id ", node_id,
                                 " not found in schema '", schema_name, "'");
}

arrow::Result<bool> ShardManager::remove_node(const std::string &schema_name,
                                              int64_t node_id) {
  if (!shards_.contains(schema_name)) {
    return arrow::Status::KeyError("Schema '", schema_name,
                                   "' not found in shards");
  }

  for (const auto &shard : shards_[schema_name]) {
    if (node_id >= shard->min_id && node_id <= shard->max_id) {
      if (auto remove_result = shard->remove(node_id); remove_result.ok()) {
        return true;
      }
    }
  }

  return arrow::Status::KeyError("Node with id ", node_id,
                                 " not found in schema '", schema_name, "'");
}

arrow::Result<bool> ShardManager::update_node(
    const std::string &schema_name, const int64_t id,
    const std::shared_ptr<Field> &field, const Value &value,
    const UpdateType update_type) {
  const auto schema_it = shards_.find(schema_name);
  if (schema_it == shards_.end()) {
    return arrow::Status::KeyError("Schema not found: ", schema_name);
  }

  for (const auto &shard : schema_it->second) {
    if (id >= shard->min_id && id <= shard->max_id) {
      return shard->update(id, field, value, update_type);
    }
  }

  return arrow::Status::KeyError("Node with id ", id, " not found in schema ",
                                 schema_name);
}

arrow::Result<bool> ShardManager::update_node(const std::string &schema_name,
                                              const int64_t id,
                                              const std::string &field_name,
                                              const Value &value,
                                              const UpdateType update_type) {
  const auto schema_it = shards_.find(schema_name);
  if (schema_it == shards_.end()) {
    return arrow::Status::KeyError("Schema not found: ", schema_name,
                                   " in shards");
  }

  auto field =
      schema_registry_->get(schema_name).ValueOrDie()->get_field(field_name);

  for (const auto &shard : schema_it->second) {
    if (id >= shard->min_id && id <= shard->max_id) {
      return shard->update(id, field, value, update_type);
    }
  }

  return arrow::Status::KeyError("Node with id ", id, " not found in schema ",
                                 schema_name);
}

arrow::Result<bool> ShardManager::update_node_fields(
    const std::string &schema_name, const int64_t id,
    const std::vector<FieldUpdate> &field_updates,
    const UpdateType update_type) {
  const auto schema_it = shards_.find(schema_name);
  if (schema_it == shards_.end()) {
    return arrow::Status::KeyError("Schema not found: ", schema_name);
  }

  for (const auto &shard : schema_it->second) {
    if (id >= shard->min_id && id <= shard->max_id) {
      return shard->update_fields(id, field_updates, update_type);
    }
  }

  return arrow::Status::KeyError("Node with id ", id, " not found in schema ",
                                 schema_name);
}

arrow::Result<std::vector<std::shared_ptr<Node>>> ShardManager::get_nodes(
    const std::string &schema_name) {
  const auto schema_it = shards_.find(schema_name);
  if (schema_it == shards_.end()) {
    return arrow::Status::KeyError("Schema '", schema_name,
                                   "' not found in shards");
  }

  std::vector<std::shared_ptr<Node>> result;
  size_t total_estimated_nodes = 0;
  for (const auto &shard : schema_it->second) {
    total_estimated_nodes += shard->size();
  }
  result.reserve(total_estimated_nodes);

  for (const auto &shard : schema_it->second) {
    auto nodes = shard->get_nodes();
    result.insert(result.end(), nodes.begin(), nodes.end());
  }

  return result;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>
ShardManager::get_tables(const std::string &schema_name,
                         TemporalContext *temporal_context) {
  const auto schema_it = shards_.find(schema_name);
  if (schema_it == shards_.end()) {
    return std::vector<std::shared_ptr<arrow::Table>>{};
  }

  std::vector<std::shared_ptr<Shard>> sorted_shards = schema_it->second;

  std::ranges::sort(sorted_shards, [](const std::shared_ptr<Shard> &a,
                                      const std::shared_ptr<Shard> &b) {
    return a->min_id < b->min_id;
  });

  std::vector<std::shared_ptr<arrow::Table>> tables;
  for (const auto &shard : sorted_shards) {
    ARROW_ASSIGN_OR_RAISE(auto table, shard->get_table(temporal_context));
    if (table->num_rows() > 0) {
      tables.push_back(table);
    }
  }

  return tables;
}

bool ShardManager::has_shards(const std::string &schema_name) const {
  const auto it = shards_.find(schema_name);
  return it != shards_.end() && !it->second.empty();
}

arrow::Result<size_t> ShardManager::get_shard_count(
    const std::string &schema_name) const {
  if (!has_shards(schema_name)) {
    return arrow::Status::Invalid("Schema '", schema_name, "' not found");
  }
  return shards_.find(schema_name)->second.size();
}

arrow::Result<std::vector<size_t>> ShardManager::get_shard_sizes(
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

arrow::Result<std::vector<std::pair<int64_t, int64_t>>>
ShardManager::get_shard_ranges(const std::string &schema_name) const {
  if (!has_shards(schema_name)) {
    return arrow::Status::Invalid("Schema '", schema_name, "' not found");
  }
  std::vector<std::pair<int64_t, int64_t>> ranges;
  for (const auto &shard : shards_.find(schema_name)->second) {
    ranges.emplace_back(shard->min_id, shard->max_id);
  }
  return ranges;
}

arrow::Result<bool> ShardManager::add_shard(
    const std::shared_ptr<Shard> &shard) {
  if (!shard) {
    return arrow::Status::Invalid("Cannot add null shard");
  }
  shards_[shard->schema_name].push_back(shard);
  return true;
}

arrow::Result<bool> ShardManager::reset_all_updated() {
  log_debug("Resetting 'updated' flag for all shards");
  for (auto &schema_shards : shards_ | std::views::values) {
    for (auto &shard : schema_shards) {
      shard->set_updated(false);
    }
  }
  return true;
}

}  // namespace tundradb
