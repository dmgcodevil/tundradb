#pragma once

#include <arrow/result.h>
#include <arrow/table.h>

#include <atomic>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.hpp"
#include "core/field_update.hpp"
#include "core/node.hpp"
#include "schema/schema.hpp"

namespace tundradb {

class TemporalContext;

// =========================================================================
// Shard - a contiguous range of nodes for one schema
// =========================================================================

class Shard {
 private:
  std::pmr::monotonic_buffer_resource memory_pool_;
  std::pmr::unordered_map<int64_t, std::shared_ptr<Node>> nodes_;
  std::set<int64_t> nodes_ids_;
  std::atomic<bool> dirty_{false};
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  int64_t updated_ts_;
  bool updated_ = true;

 public:
  const int64_t id;
  const int64_t index;
  int64_t min_id;
  int64_t max_id;
  const size_t capacity;
  const size_t chunk_size;
  std::string schema_name;

  Shard(int64_t id, int64_t index, size_t capacity, int64_t min_id,
        int64_t max_id, size_t chunk_size, std::string schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry,
        size_t buffer_size = 10 * 1024 * 1024);

  Shard(int64_t id, int64_t index, const DatabaseConfig &config, int64_t min_id,
        int64_t max_id, std::string schema_name,
        std::shared_ptr<SchemaRegistry> schema_registry);

  ~Shard();

  [[nodiscard]] bool is_updated() const;
  bool set_updated(bool v);
  [[nodiscard]] int64_t get_updated_ts() const;
  [[nodiscard]] std::string compound_id() const;

  arrow::Result<bool> add(const std::shared_ptr<Node> &node);
  arrow::Result<bool> extend(const std::shared_ptr<Node> &node);
  arrow::Result<std::shared_ptr<Node>> remove(int64_t id);
  arrow::Result<std::shared_ptr<Node>> poll_first();

  arrow::Result<bool> update(int64_t node_id, std::shared_ptr<Field> field,
                             const Value &value, UpdateType update_type);

  arrow::Result<bool> update_fields(
      int64_t node_id, const std::vector<FieldUpdate> &field_updates,
      UpdateType update_type);

  arrow::Result<std::shared_ptr<arrow::Table>> get_table(TemporalContext *ctx);

  [[nodiscard]] size_t size() const;
  [[nodiscard]] bool has_space() const;
  [[nodiscard]] bool empty() const;
  [[nodiscard]] std::vector<std::shared_ptr<Node>> get_nodes() const;
};

// =========================================================================
// ShardManager - manages per-schema shard collections
// =========================================================================

class ShardManager {
 private:
  std::pmr::monotonic_buffer_resource memory_pool_;
  std::pmr::unordered_map<std::string, std::vector<std::shared_ptr<Shard>>>
      shards_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  const size_t shard_capacity_;
  const size_t chunk_size_;
  const DatabaseConfig config_;
  std::atomic<int64_t> id_counter_{0};
  std::unordered_map<std::string, std::atomic<int64_t>> index_counters_;
  mutable std::mutex index_counter_mutex_;

  void create_new_shard(const std::shared_ptr<Node> &node);

 public:
  explicit ShardManager(std::shared_ptr<SchemaRegistry> schema_registry,
                        const DatabaseConfig &config);

  void set_id_counter(int64_t value);
  [[nodiscard]] int64_t get_id_counter() const;
  void set_index_counter(const std::string &schema_name, int64_t value);
  [[nodiscard]] int64_t get_index_counter(const std::string &schema_name) const;

  arrow::Result<std::shared_ptr<Shard>> get_shard(
      const std::string &schema_name, int64_t id);

  [[nodiscard]] std::vector<std::string> get_schema_names() const;

  [[nodiscard]] arrow::Result<std::vector<std::shared_ptr<Shard>>> get_shards(
      const std::string &schema_name) const;

  arrow::Result<bool> is_shard_clean(std::string s, int64_t id);

  arrow::Result<bool> compact(const std::string &schema_name);
  arrow::Result<bool> compact_all();

  arrow::Result<bool> insert_node(const std::shared_ptr<Node> &node);

  arrow::Result<std::shared_ptr<Node>> get_node(const std::string &schema_name,
                                                int64_t node_id);

  arrow::Result<bool> remove_node(const std::string &schema_name,
                                  int64_t node_id);

  arrow::Result<bool> update_node(const std::string &schema_name, int64_t id,
                                  const std::shared_ptr<Field> &field,
                                  const Value &value, UpdateType update_type);

  arrow::Result<bool> update_node(const std::string &schema_name, int64_t id,
                                  const std::string &field_name,
                                  const Value &value, UpdateType update_type);

  arrow::Result<bool> update_node_fields(
      const std::string &schema_name, int64_t id,
      const std::vector<FieldUpdate> &field_updates, UpdateType update_type);

  arrow::Result<std::vector<std::shared_ptr<Node>>> get_nodes(
      const std::string &schema_name);

  arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> get_tables(
      const std::string &schema_name, TemporalContext *temporal_context);

  [[nodiscard]] bool has_shards(const std::string &schema_name) const;

  [[nodiscard]] arrow::Result<size_t> get_shard_count(
      const std::string &schema_name) const;

  [[nodiscard]] arrow::Result<std::vector<size_t>> get_shard_sizes(
      const std::string &schema_name) const;

  [[nodiscard]] arrow::Result<std::vector<std::pair<int64_t, int64_t>>>
  get_shard_ranges(const std::string &schema_name) const;

  arrow::Result<bool> add_shard(const std::shared_ptr<Shard> &shard);
  arrow::Result<bool> reset_all_updated();
};

}  // namespace tundradb
