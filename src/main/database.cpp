#include "main/database.hpp"

#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <tbb/parallel_for.h>

#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow/utils.hpp"
#include "common/logger.hpp"
#include "common/utils.hpp"
#include "query/row.hpp"

namespace fs = std::filesystem;

namespace tundradb {

// ---------------------------------------------------------------------------
// Database - methods moved from database.hpp
// ---------------------------------------------------------------------------

Database::Database(const DatabaseConfig& config)
    : schema_registry_(std::make_shared<SchemaRegistry>()),
      shard_manager_(std::make_shared<ShardManager>(schema_registry_, config)),
      node_manager_(std::make_shared<NodeManager>(
          schema_registry_, config.is_validation_enabled(), true,
          config.is_versioning_enabled())),
      config_(config),
      persistence_enabled_(config.is_persistence_enabled()),
      edge_store_(std::make_shared<EdgeStore>(0, config.get_chunk_size())) {
  if (!initialize_arrow_compute()) {
    log_error("Failed to initialize Arrow Compute module");
  }
  if (persistence_enabled_) {
    const std::string& db_path = config.get_db_path();
    if (db_path.empty()) {
      log_error("Database path is empty but persistence is enabled");
      persistence_enabled_ = false;
      return;
    }
    std::string data_path = db_path + "/data";
    storage_ = std::make_shared<Storage>(std::move(data_path), schema_registry_,
                                         node_manager_, config);
    metadata_manager_ = std::make_shared<MetadataManager>(db_path);
    snapshot_manager_ = std::make_shared<SnapshotManager>(
        metadata_manager_, storage_, shard_manager_, edge_store_, node_manager_,
        schema_registry_);
  }
}

arrow::Result<bool> Database::initialize() {
  if (persistence_enabled_) {
    ARROW_RETURN_NOT_OK(storage_->initialize().status());
    ARROW_RETURN_NOT_OK(metadata_manager_->initialize().status());
    ARROW_RETURN_NOT_OK(snapshot_manager_->initialize().status());
  }
  return true;
}

arrow::Result<std::shared_ptr<Node>> Database::create_node(
    const std::string& schema_name,
    const std::unordered_map<std::string, Value>& data) {
  if (schema_name.empty()) {
    return arrow::Status::Invalid("Schema name cannot be empty");
  }
  ARROW_ASSIGN_OR_RAISE(auto node,
                        node_manager_->create_node(schema_name, data));
  ARROW_RETURN_NOT_OK(shard_manager_->insert_node(node));
  return node;
}

arrow::Result<bool> Database::update_node(const std::string& schema_name,
                                          int64_t id,
                                          const std::shared_ptr<Field>& field,
                                          const Value& value,
                                          UpdateType update_type) {
  return shard_manager_->update_node(schema_name, id, field, value,
                                     update_type);
}

arrow::Result<bool> Database::update_node(const std::string& schema_name,
                                          int64_t id,
                                          const std::string& field_name,
                                          const Value& value,
                                          UpdateType update_type) {
  return shard_manager_->update_node(schema_name, id, field_name, value,
                                     update_type);
}

arrow::Result<bool> Database::update_node_fields(
    const std::string& schema_name, int64_t id,
    const std::vector<FieldUpdate>& field_updates, UpdateType update_type) {
  return shard_manager_->update_node_fields(schema_name, id, field_updates,
                                            update_type);
}

arrow::Result<bool> Database::remove_node(const std::string& schema_name,
                                          int64_t node_id) {
  if (!node_manager_->remove_node(schema_name, node_id)) {
    return arrow::Status::Invalid("Failed to remove node: ", schema_name, ":",
                                  node_id);
  }
  return shard_manager_->remove_node(schema_name, node_id);
}

arrow::Result<bool> Database::register_edge_schema(
    const std::string& edge_type,
    const std::vector<std::shared_ptr<Field>>& fields) {
  return edge_store_->register_edge_schema(edge_type, fields);
}

arrow::Result<bool> Database::connect(int64_t source_id,
                                      const std::string& type,
                                      int64_t target_id) {
  ARROW_ASSIGN_OR_RAISE(const auto edge,
                        edge_store_->create_edge(source_id, type, target_id));
  ARROW_RETURN_NOT_OK(edge_store_->add(edge));
  return true;
}

arrow::Result<bool> Database::connect(
    int64_t source_id, const std::string& type, int64_t target_id,
    std::unordered_map<std::string, Value> properties) {
  ARROW_ASSIGN_OR_RAISE(const auto edge,
                        edge_store_->create_edge(source_id, type, target_id,
                                                 std::move(properties)));
  ARROW_RETURN_NOT_OK(edge_store_->add(edge));
  return true;
}

arrow::Result<bool> Database::remove_edge(int64_t edge_id) {
  return edge_store_->remove(edge_id);
}

arrow::Result<bool> Database::compact(const std::string& schema_name) {
  return shard_manager_->compact(schema_name);
}

arrow::Result<bool> Database::compact_all() {
  return shard_manager_->compact_all();
}

arrow::Result<std::shared_ptr<arrow::Table>> Database::get_table(
    const std::string& schema_name, TemporalContext* temporal_context,
    size_t chunk_size) const {
  ARROW_ASSIGN_OR_RAISE(const auto schema, schema_registry_->get(schema_name));
  auto arrow_schema = schema->arrow();
  ARROW_ASSIGN_OR_RAISE(auto all_nodes, shard_manager_->get_nodes(schema_name));
  if (all_nodes.empty()) {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
    empty_columns.reserve(arrow_schema->num_fields());
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
      empty_columns.push_back(std::make_shared<arrow::ChunkedArray>(
          std::vector<std::shared_ptr<arrow::Array>>{}));
    }
    return arrow::Table::Make(arrow_schema, empty_columns);
  }
  std::ranges::sort(
      all_nodes, [](const std::shared_ptr<Node>& a,
                    const std::shared_ptr<Node>& b) { return a->id < b->id; });
  return create_table(schema, all_nodes, chunk_size, temporal_context);
}

arrow::Result<size_t> Database::get_shard_count(
    const std::string& schema_name) const {
  if (!schema_registry_->exists(schema_name)) {
    return arrow::Status::Invalid("Schema '", schema_name, "' not found");
  }
  return shard_manager_->get_shard_count(schema_name);
}

arrow::Result<std::vector<size_t>> Database::get_shard_sizes(
    const std::string& schema_name) const {
  if (!schema_registry_->exists(schema_name)) {
    return arrow::Status::Invalid("Schema '", schema_name, "' not found");
  }
  return shard_manager_->get_shard_sizes(schema_name);
}

arrow::Result<std::vector<std::pair<int64_t, int64_t>>>
Database::get_shard_ranges(const std::string& schema_name) const {
  if (!schema_registry_->exists(schema_name)) {
    return arrow::Status::Invalid("Schema '", schema_name, "' not found");
  }
  return shard_manager_->get_shard_ranges(schema_name);
}

arrow::Result<Snapshot> Database::create_snapshot() {
  return snapshot_manager_->commit();
}

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state(this->schema_registry_);
  auto result = std::make_shared<QueryResult>();

  ARROW_RETURN_NOT_OK(init_query_state(query, query_state));
  ARROW_RETURN_NOT_OK(inline_from_where(query, query_state, *result));
  ARROW_ASSIGN_OR_RAISE(const auto post_where,
                        execute_clauses(query, query_state, *result));
  ARROW_ASSIGN_OR_RAISE(
      auto output_table,
      build_result_table(query, query_state, post_where, *result));
  result->set_table(std::move(output_table));
  return result;
}

// ---------------------------------------------------------------------------
// Database::execute_clauses
// ---------------------------------------------------------------------------
arrow::Result<std::vector<std::shared_ptr<WhereExpr>>>
Database::execute_clauses(const Query& query, QueryState& query_state,
                          QueryResult& result) const {
  std::vector<std::shared_ptr<WhereExpr>> post_where;
  for (size_t i = 0; i < query.clauses().size(); ++i) {
    auto clause = query.clauses()[i];
    switch (clause->type()) {
      case Clause::Type::WHERE:
        ARROW_RETURN_NOT_OK(
            apply_where_filter(std::dynamic_pointer_cast<WhereExpr>(clause),
                               query_state, post_where));
        break;
      case Clause::Type::TRAVERSE:
        ARROW_RETURN_NOT_OK(
            execute_traverse(std::static_pointer_cast<Traverse>(clause),
                             query_state, query, i, result));
        break;
      default:
        return arrow::Status::NotImplemented(
            "Database::query unsupported clause");
    }
  }
  return post_where;
}

}  // namespace tundradb
