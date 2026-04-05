#include "main/database.hpp"

#include <arrow/compute/api.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

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
#include "query/join.hpp"
#include "query/row.hpp"
#include "query/temporal_context.hpp"

namespace fs = std::filesystem;

namespace tundradb {

// ---------------------------------------------------------------------------
// Database — methods moved from database.hpp
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

arrow::Result<std::shared_ptr<std::vector<std::shared_ptr<Row>>>>
populate_rows_bfs(int64_t node_id, const SchemaRef& start_schema,
                  const std::shared_ptr<arrow::Schema>& output_schema,
                  const QueryState& query_state,
                  llvm::DenseSet<uint64_t>& global_visited) {
  IF_DEBUG_ENABLED {
    log_debug("populate_rows_bfs::node={}:{}", start_schema.value(), node_id);
  }
  auto result = std::make_shared<std::vector<std::shared_ptr<Row>>>();
  int64_t row_id_counter = 0;
  auto initial_row =
      std::make_shared<Row>(create_empty_row_from_schema(output_schema));

  std::queue<QueueItem> queue;
  queue.emplace(node_id, start_schema, 0, initial_row);
  // Use precomputed fully-qualified field names from QueryState
  auto apply_edge_to_row = [&](Row& row,
                               const GraphConnection& conn) -> arrow::Status {
    if (!conn.edge_alias.has_value() || conn.edge_id < 0 ||
        !query_state.edge_store) {
      return arrow::Status::OK();
    }

    const std::string& edge_alias = conn.edge_alias.value();
    const auto idx_it = query_state.schema_field_indices().find(edge_alias);
    if (idx_it == query_state.schema_field_indices().end()) {
      return arrow::Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(const auto edge_schema,
                          query_state.get_schema_for_alias(edge_alias));
    ARROW_ASSIGN_OR_RAISE(const auto edge_obj,
                          query_state.edge_store->get(conn.edge_id));
    row.set_cell_from_edge(idx_it->second, edge_obj, edge_schema->fields(),
                           query_state.temporal_context.get());
    return arrow::Status::OK();
  };

  while (!queue.empty()) {
    auto size = queue.size();
    while (size-- > 0) {
      auto item = queue.front();
      queue.pop();
      ARROW_ASSIGN_OR_RAISE(const auto item_schema,
                            query_state.resolve_schema(item.schema_ref));

      auto node = query_state.node_manager->get_node(item_schema, item.node_id)
                      .ValueOrDie();
      const auto& it_fq =
          query_state.schema_field_indices().find(item.schema_ref.value());
      if (it_fq == query_state.schema_field_indices().end()) {
        log_error("No fully-qualified field names for schema '{}'",
                  item.schema_ref.value());
        return arrow::Status::KeyError(
            "Missing precomputed fq_field_names for alias {}",
            item.schema_ref.value());
      }
      item.row->set_cell_from_node(it_fq->second, node,
                                   query_state.temporal_context.get());
      const uint64_t packed = hash_code_(item.schema_ref, item.node_id);
      global_visited.insert(packed);
      item.path_visited_nodes.insert(packed);

      // group connections by target schema (small, stack-friendly)
      llvm::SmallDenseMap<llvm::StringRef,
                          llvm::SmallVector<GraphConnection, 4>, 4>
          grouped_connections;

      bool skip = false;
      if (query_state.has_outgoing(item.schema_ref, item.node_id)) {
        for (const auto& conn : query_state.connections()
                                    .at(item.schema_ref.value())
                                    .at(item.node_id)) {
          const uint64_t tgt_packed = hash_code_(conn.target, conn.target_id);
          if (!item.path_visited_nodes.contains(tgt_packed)) {
            if (query_state.ids()
                    .at(conn.target.value())
                    .contains(conn.target_id)) {
              grouped_connections[conn.target.value()].push_back(conn);
            } else {
              skip = true;
            }
          }
        }
      }
      log_grouped_connections(item.node_id, grouped_connections);

      if (grouped_connections.empty()) {
        // we've done
        if (!skip) {
          auto r = std::make_shared<Row>(
              *item.row);  // Copy needed: each result needs unique ID and path
          r->path = item.path;
          r->id = row_id_counter++;
          IF_DEBUG_ENABLED { log_debug("add row: {}", r->ToString()); }
          result->push_back(r);
        }

      } else {
        for (const auto& pair : grouped_connections) {
          const auto& connections = pair.second;
          if (connections.size() == 1) {
            // continue the path
            const auto& conn = connections[0];
            ARROW_RETURN_NOT_OK(apply_edge_to_row(*item.row, conn));
            auto next =
                QueueItem(connections[0].target_id, connections[0].target,
                          item.level + 1, item.row);

            next.path = item.path;
            next.path.emplace_back(connections[0].target.value(),
                                   connections[0].target_id);
            IF_DEBUG_ENABLED {
              log_debug("continue the path: {}", join_schema_path(next.path));
            }
            queue.push(next);
          } else {
            for (const auto& conn : connections) {
              auto next_row = std::make_shared<Row>(*item.row);
              ARROW_RETURN_NOT_OK(apply_edge_to_row(*next_row, conn));
              auto next = QueueItem(conn.target_id, conn.target, item.level + 1,
                                    next_row);
              next.path = item.path;
              next.path.push_back(PathSegment{
                  conn.target.tag(), conn.target.value(), conn.target_id});
              IF_DEBUG_ENABLED {
                log_debug("create a new path {}, node={}",
                          join_schema_path(next.path), conn.target_id);
              }
              queue.push(next);
            }
          }
        }
      }
    }
  }
  RowNode tree;
  tree.path_segment = PathSegment{0, "root", -1};  // Use tag 0 for root
  for (const auto& r : *result) {
    IF_DEBUG_ENABLED { log_debug("bfs result: {}", r->ToString()); }
    // Copy needed: tree merge operations will modify rows, so each needs to be
    // independent
    auto r_copy = std::make_shared<Row>(*r);
    tree.insert_row(r_copy);
  }
  IF_DEBUG_ENABLED { tree.print(); }
  auto merged = tree.merge_rows(query_state.field_id_to_name());
  IF_DEBUG_ENABLED {
    for (const auto& row : merged) {
      log_debug("merge result: {}", row->ToString());
    }
  }
  return std::make_shared<std::vector<std::shared_ptr<Row>>>(merged);
}

// template <NodeIds NodeIdsT>
arrow::Result<std::shared_ptr<std::vector<std::shared_ptr<Row>>>>
populate_batch_rows(const llvm::DenseSet<int64_t>& node_ids,
                    const SchemaRef& schema_ref,
                    const std::shared_ptr<arrow::Schema>& output_schema,
                    const QueryState& query_state, const TraverseType join_type,
                    tbb::concurrent_unordered_set<uint64_t>& global_visited) {
  auto rows = std::make_shared<std::vector<std::shared_ptr<Row>>>();
  rows->reserve(node_ids.size());
  llvm::DenseSet<uint64_t> local_visited;
  // For INNER join: only process nodes that have connections
  // For LEFT join: process all nodes from the "left" side
  for (const auto node_id : node_ids) {
    if (!global_visited.insert(hash_code_(schema_ref, node_id)).second) {
      // Skip if already processed in an earlier traversal
      continue;
    }

    // For INNER JOIN: Skip nodes without connections
    if (join_type == TraverseType::Inner &&
        !query_state.has_outgoing(schema_ref, node_id)) {
      continue;
    }

    auto res = populate_rows_bfs(node_id, schema_ref, output_schema,
                                 query_state, local_visited);
    if (!res.ok()) {
      log_error("Failed to populate rows for node {} in schema '{}': {}",
                node_id, schema_ref.value(), res.status().ToString());
      return res.status();
    }

    const auto& res_value = res.ValueOrDie();
    rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                 std::make_move_iterator(res_value->end()));
  }
  global_visited.insert(local_visited.begin(), local_visited.end());
  return rows;
}

std::vector<llvm::DenseSet<int64_t>> batch_node_ids(
    const llvm::DenseSet<int64_t>& ids, const size_t batch_size) {
  std::vector<llvm::DenseSet<int64_t>> batches;
  batches.reserve(ids.size() / batch_size + 1);
  llvm::DenseSet<int64_t> current_batch;
  current_batch.reserve(batch_size);

  for (const auto& id : ids) {
    current_batch.insert(id);

    if (current_batch.size() >= batch_size) {
      batches.push_back(std::move(current_batch));
      current_batch.clear();
      current_batch.reserve(batch_size);
    }
  }

  if (!current_batch.empty()) {
    batches.push_back(std::move(current_batch));
  }

  return batches;
}

// process all schemas used in traverse
// Phase 1: Process connected nodes
// Phase 2: Handle outer joins for unmatched nodes
arrow::Result<std::shared_ptr<std::vector<std::shared_ptr<Row>>>> populate_rows(
    const ExecutionConfig& execution_config, const QueryState& query_state,
    const std::vector<Traverse>& traverses,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  auto rows = std::make_shared<std::vector<std::shared_ptr<Row>>>();
  std::mutex rows_mtx;
  tbb::concurrent_unordered_set<uint64_t> global_visited;

  // Map schemas to their join types
  std::unordered_map<std::string, TraverseType> schema_join_types;
  schema_join_types.reserve(traverses.size());
  if (traverses.empty()) {
    schema_join_types[query_state.from.value()] = TraverseType::Left;
  } else {
    // FROM is always inner by default
    schema_join_types[query_state.from.value()] = TraverseType::Inner;
  }

  // Only apply LEFT JOIN to FROM schema if the FROM schema is directly involved
  // in a LEFT JOIN traversal
  for (const auto& traverse : traverses) {
    if (traverse.source().value() == query_state.from.value() &&
        (traverse.traverse_type() == TraverseType::Left ||
         traverse.traverse_type() == TraverseType::Full)) {
      schema_join_types[query_state.from.value()] = traverse.traverse_type();
      break;
    }
  }

  // Build ordered list of schema references to process
  std::vector<SchemaRef> ordered_schemas;
  ordered_schemas.push_back(query_state.from);

  // Add schemas from traversals in order
  for (const auto& traverse : traverses) {
    // Update join type for the target schema
    schema_join_types[traverse.target().value()] = traverse.traverse_type();

    // Add target schema to the ordered list if not already present
    if (std::ranges::find_if(ordered_schemas, [&](const SchemaRef& sr) {
          return sr.value() == traverse.target().value();
        }) == ordered_schemas.end()) {
      ordered_schemas.push_back(traverse.target());
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Processing {} schemas with their respective join types",
              ordered_schemas.size());
  }

  // Process each schema in order
  for (const auto& schema_ref : ordered_schemas) {
    TraverseType join_type = schema_join_types[schema_ref.value()];
    IF_DEBUG_ENABLED {
      log_debug("Processing schema '{}' with join type {}", schema_ref.value(),
                static_cast<int>(join_type));
    }

    if (!query_state.ids().contains(schema_ref.value())) {
      log_warn("Schema '{}' not found in query state IDs", schema_ref.value());
      continue;
    }

    // Get all nodes for this schema
    const auto& schema_nodes = query_state.ids().at(schema_ref.value());
    std::vector<std::vector<int64_t>> batch_ids;
    if (execution_config.parallel_enabled) {
      size_t batch_size = 0;
      if (execution_config.parallel_batch_size > 0) {
        batch_size = execution_config.parallel_batch_size;
      } else {
        batch_size = execution_config.calculate_batch_size(schema_nodes.size());
      }
      auto batches = batch_node_ids(schema_nodes, batch_size);
      IF_DEBUG_ENABLED {
        log_debug(
            "process concurrently. thread_count={}, batch_size={}, "
            "batches_count={}",
            execution_config.parallel_thread_count, batch_size, batches.size());
      }
      tbb::task_arena arena(execution_config.parallel_thread_count);
      std::atomic error_occurred{false};
      std::string error_message;
      std::mutex error_mutex;
      arena.execute([&] {
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, batches.size()),
            [&](const tbb::blocked_range<size_t>& range) {
              for (size_t i = range.begin(); i != range.end(); ++i) {
                if (error_occurred.load()) {
                  return;  // Early exit from this thread
                }
                auto res =
                    populate_batch_rows(batches[i], schema_ref, output_schema,
                                        query_state, join_type, global_visited);
                if (!res.ok()) {
                  error_occurred.store(true);
                  std::lock_guard lock(error_mutex);
                  if (error_message.empty()) {  // First error wins
                    error_message = res.status().ToString();
                  }
                  return;
                }
                const auto& batch_rows = res.ValueOrDie();
                std::lock_guard lock(rows_mtx);
                rows->insert(rows->end(), batch_rows->begin(),
                             batch_rows->end());
              }
            });
      });
      if (error_occurred.load()) {
        return arrow::Status::ExecutionError(
            "Parallel batch processing failed: " + error_message);
      }

    } else {
      auto res = populate_batch_rows(schema_nodes, schema_ref, output_schema,
                                     query_state, join_type, global_visited);
      if (!res.ok()) {
        return res.status();
      }
      const auto& res_value = res.ValueOrDie();
      rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                   std::make_move_iterator(res_value->end()));
    }

    IF_DEBUG_ENABLED {
      log_debug("Processing schema '{}' nodes: [{}]", schema_ref.value(),
                join_container(schema_nodes));
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Generated {} total rows after processing all schemas",
              rows->size());
  }
  return rows;
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_rows(
    const std::shared_ptr<std::vector<std::shared_ptr<Row>>>& rows,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  if (output_schema == nullptr) {
    return arrow::Status::Invalid("output schema is null");
  }
  if (!rows || rows->empty()) {
    return create_empty_table(output_schema);
  }

  // Create array builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : output_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
  }

  const size_t num_fields = builders.size();
  const size_t num_rows = rows->size();
  for (auto& builder : builders) {
    ARROW_RETURN_NOT_OK(builder->Reserve(num_rows));
  }

  for (const auto& row : *rows) {
    for (size_t i = 0; i < num_fields; i++) {
      ValueRef value_ref;
      if (i < row->cells.size() && row->cells[i].data != nullptr) {
        value_ref = row->cells[i];
      }
      ARROW_RETURN_NOT_OK(
          append_value_to_builder(value_ref, builders[i].get()));
    }
  }

  // Finish building the arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());

  for (const auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder->Finish(&array));
    arrays.push_back(array);
  }

  // Create and return the table
  return arrow::Table::Make(output_schema, arrays);
}

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state(this->schema_registry_);
  auto result = std::make_shared<QueryResult>();

  // Initialize temporal context if AS OF clause is present
  if (query.temporal_snapshot().has_value()) {
    query_state.temporal_context =
        std::make_unique<TemporalContext>(query.temporal_snapshot().value());
    IF_DEBUG_ENABLED {
      log_debug("Temporal query: AS OF VALIDTIME={}, TXNTIME={}",
                query_state.temporal_context->snapshot().valid_time,
                query_state.temporal_context->snapshot().tx_time);
    }
  }

  // Pre-size hash maps to avoid expensive resizing during execution
  query_state.reserve_capacity(query);

  IF_DEBUG_ENABLED {
    log_debug("Executing query starting from schema '{}'",
              query.from().toString());
  }
  query_state.node_manager = this->node_manager_;
  query_state.edge_store = this->edge_store_;

  {
    IF_DEBUG_ENABLED {
      log_debug("processing 'from' {}", query.from().toString());
    }
    query_state.from = query.from();
    query_state.from.set_tag(compute_tag(query_state.from));
    ARROW_ASSIGN_OR_RAISE(auto source_schema,
                          query_state.register_schema(query.from()));
    if (!this->schema_registry_->exists(source_schema)) {
      log_error("schema '{}' doesn't exist", source_schema);
      return arrow::Status::KeyError("schema doesn't exit: {}", source_schema);
    }
    ARROW_ASSIGN_OR_RAISE(
        auto source_table,
        this->get_table(source_schema, query_state.temporal_context.get()));
    ARROW_RETURN_NOT_OK(query_state.update_table(source_table, query.from()));
    if (auto res = query_state.compute_fully_qualified_names(query.from());
        !res.ok()) {
      return res.status();
    }
  }

  // PHASE: Query Preparation - Populate aliases, traversals, tags, and resolve
  // field references
  {
    IF_DEBUG_ENABLED {
      log_debug(
          "Preparing query: populating aliases, traversals, and resolving "
          "field references");
    }
    auto preparation_result = prepare_query(query, query_state);
    if (!preparation_result.ok()) {
      log_error("Failed to prepare query: {}", preparation_result.ToString());
      return preparation_result;
    }
    IF_DEBUG_ENABLED { log_debug("Query preparation completed successfully"); }
  }

  {
    auto where_exps =
        get_where_to_inline(query.from().value(), 0, query.clauses());
    result->mutable_execution_stats().num_where_clauses_inlined +=
        where_exps.size();
    auto res =
        inline_where(query.from(), query_state.tables[query.from().value()],
                     query_state, where_exps);
    if (!res.ok()) {
      return res.status();
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Processing {} query clauses", query.clauses().size());
  }

  std::vector<std::shared_ptr<WhereExpr>> post_where;
  for (size_t i = 0; i < query.clauses().size(); ++i) {
    auto clause = query.clauses()[i];
    switch (clause->type()) {
      case Clause::Type::WHERE: {
        ARROW_RETURN_NOT_OK(
            apply_where_filter(std::dynamic_pointer_cast<WhereExpr>(clause),
                               query_state, post_where));
        break;
      }
      case Clause::Type::TRAVERSE: {
        ARROW_RETURN_NOT_OK(
            execute_traverse(std::static_pointer_cast<Traverse>(clause),
                             query_state, query, i, *result));
        break;
      }
      default:
        log_error("Unsupported clause type: {}",
                  static_cast<int>(clause->type()));
        return arrow::Status::NotImplemented(
            "Database::query unsupported clause");
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Query processing complete, building result");
    log_debug("Query state: {}", query_state.ToString());
  }

  ARROW_ASSIGN_OR_RAISE(
      auto output_table,
      build_result_table(query, query_state, post_where, *result));
  result->set_table(std::move(output_table));
  return result;
}

// ---------------------------------------------------------------------------
// Database::build_result_table
// ---------------------------------------------------------------------------
arrow::Result<std::shared_ptr<arrow::Table>> Database::build_result_table(
    const Query& query, QueryState& query_state,
    const std::vector<std::shared_ptr<WhereExpr>>& post_where,
    QueryResult& result) const {
  ARROW_ASSIGN_OR_RAISE(auto output_schema,
                        build_denormalized_schema(query_state));
  IF_DEBUG_ENABLED { log_debug("output_schema={}", output_schema->ToString()); }

  ARROW_ASSIGN_OR_RAISE(auto rows,
                        populate_rows(query.execution_config(), query_state,
                                      query_state.traversals, output_schema));
  ARROW_ASSIGN_OR_RAISE(auto table,
                        create_table_from_rows(rows, output_schema));

  for (const auto& expr : post_where) {
    result.mutable_execution_stats().num_where_clauses_post_processed++;
    IF_DEBUG_ENABLED { log_debug("post process where: {}", expr->toString()); }
    ARROW_ASSIGN_OR_RAISE(table, filter(table, *expr, false));
  }
  return apply_select(query.select(), table);
}

// ---------------------------------------------------------------------------
// Database::apply_where_filter
// ---------------------------------------------------------------------------
arrow::Status Database::apply_where_filter(
    const std::shared_ptr<WhereExpr>& where, QueryState& query_state,
    std::vector<std::shared_ptr<WhereExpr>>& post_where) const {
  if (where->inlined()) {
    IF_DEBUG_ENABLED {
      log_debug("where '{}' is inlined, skip", where->toString());
    }
    return arrow::Status::OK();
  }
  auto variables = where->get_all_variables();
  if (variables.empty()) {
    return arrow::Status::Invalid(
        "where clause field must have variable "
        "<var>.<field>, actual={}",
        where->toString());
  }
  if (variables.size() != 1) {
    IF_DEBUG_ENABLED {
      log_debug("Add compound WHERE expression: '{}' to post process",
                where->toString());
    }
    post_where.emplace_back(where);
    return arrow::Status::OK();
  }

  IF_DEBUG_ENABLED {
    log_debug("Processing WHERE clause: '{}'", where->toString());
  }

  std::string variable = *variables.begin();
  if (!query_state.tables.contains(variable)) {
    if (!query_state.aliases().contains(variable)) {
      return arrow::Status::Invalid("Unknown variable '{}'", variable);
    }
    post_where.emplace_back(where);
    return arrow::Status::OK();
  }
  auto table = query_state.tables.at(variable);
  arrow::Result<std::shared_ptr<arrow::Table>> filtered_table_result =
      filter(table, *where, true);
  if (!filtered_table_result.ok() && where->requires_row_eval()) {
    ARROW_ASSIGN_OR_RAISE(
        const auto resolved_schema,
        query_state.resolve_schema(SchemaRef::parse(variable)));

    llvm::DenseSet<int64_t> keep_ids;
    for (const auto id : query_state.ids()[variable]) {
      auto node_res = node_manager_->get_node(resolved_schema, id);
      if (!node_res.ok()) continue;
      ARROW_ASSIGN_OR_RAISE(const bool matches,
                            where->matches(node_res.ValueOrDie()));
      if (matches) {
        keep_ids.insert(id);
      }
    }

    auto id_column = table->GetColumnByName("id");
    if (!id_column) {
      return arrow::Status::Invalid("Could not find 'id' column for variable '",
                                    variable, "'");
    }

    arrow::BooleanBuilder mask_builder;
    for (int ci = 0; ci < id_column->num_chunks(); ++ci) {
      auto ids =
          std::static_pointer_cast<arrow::Int64Array>(id_column->chunk(ci));
      for (int64_t irow = 0; irow < ids->length(); ++irow) {
        if (ids->IsNull(irow)) {
          ARROW_RETURN_NOT_OK(mask_builder.Append(false));
        } else {
          ARROW_RETURN_NOT_OK(
              mask_builder.Append(keep_ids.contains(ids->Value(irow))));
        }
      }
    }

    std::shared_ptr<arrow::Array> mask_array;
    ARROW_RETURN_NOT_OK(mask_builder.Finish(&mask_array));
    ARROW_ASSIGN_OR_RAISE(
        auto filtered_datum,
        arrow::compute::Filter(arrow::Datum(table), arrow::Datum(mask_array)));
    filtered_table_result = filtered_datum.table();
  }
  if (!filtered_table_result.ok()) {
    log_error("Failed to process where: '{}', error: {}", where->toString(),
              filtered_table_result.status().ToString());
    return filtered_table_result.status();
  }
  ARROW_RETURN_NOT_OK(query_state.update_table(
      filtered_table_result.ValueOrDie(), SchemaRef::parse(variable)));
  return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Database::execute_traverse
// ---------------------------------------------------------------------------
arrow::Status Database::execute_traverse(
    const std::shared_ptr<Traverse>& traverse, QueryState& query_state,
    const Query& query, size_t clause_index, QueryResult& result) const {
  ARROW_ASSIGN_OR_RAISE(const auto source_schema,
                        query_state.resolve_schema(traverse->source()));
  ARROW_ASSIGN_OR_RAISE(const auto target_schema,
                        query_state.resolve_schema(traverse->target()));
  ARROW_RETURN_NOT_OK(
      query_state.compute_fully_qualified_names(traverse->source()));
  ARROW_RETURN_NOT_OK(
      query_state.compute_fully_qualified_names(traverse->target()));
  if (traverse->edge_alias().has_value()) {
    ARROW_RETURN_NOT_OK(query_state.compute_fully_qualified_names(
        SchemaRef::parse(traverse->edge_alias().value())));
  }

  std::vector<std::shared_ptr<WhereExpr>> where_clauses;
  std::vector<std::shared_ptr<WhereExpr>> edge_where_clauses;
  if (query.inline_where()) {
    where_clauses = get_where_to_inline(traverse->target().value(),
                                        clause_index + 1, query.clauses());
  }
  if (traverse->edge_alias().has_value()) {
    edge_where_clauses = get_where_to_inline(traverse->edge_alias().value(),
                                             clause_index + 1, query.clauses());
  }
  for (const auto& wc : where_clauses) wc->set_inlined(true);
  for (const auto& wc : edge_where_clauses) wc->set_inlined(true);
  result.mutable_execution_stats().num_where_clauses_inlined +=
      where_clauses.size() + edge_where_clauses.size();

  IF_DEBUG_ENABLED {
    log_debug("Processing TRAVERSE {}-({})->{}", traverse->source().toString(),
              traverse->edge_type(), traverse->target().toString());
  }
  auto source = traverse->source();
  if (!query_state.tables.contains(source.value())) {
    IF_DEBUG_ENABLED {
      log_debug("Source table '{}' not found. Loading",
                traverse->source().toString());
    }
    ARROW_ASSIGN_OR_RAISE(
        auto source_table,
        this->get_table(source_schema, query_state.temporal_context.get()));
    ARROW_RETURN_NOT_OK(
        query_state.update_table(source_table, traverse->source()));
  }

  IF_DEBUG_ENABLED {
    log_debug("Traversing from {} source nodes",
              query_state.ids()[source.value()].size());
  }
  llvm::DenseSet<int64_t> matched_source_ids;
  llvm::DenseSet<int64_t> matched_target_ids;
  llvm::DenseSet<int64_t> unmatched_source_ids;
  for (auto source_id : query_state.ids()[source.value()]) {
    auto outgoing_edges =
        edge_store_->get_outgoing_edges(source_id, traverse->edge_type())
            .ValueOrDie();
    IF_DEBUG_ENABLED {
      log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                outgoing_edges.size(), traverse->edge_type());
    }

    bool source_had_match = false;
    for (const auto& edge : outgoing_edges) {
      auto target_id = edge->get_target_id();
      if (query_state.ids().contains(traverse->target().value()) &&
          !query_state.ids()
               .at(traverse->target().value())
               .contains(target_id)) {
        continue;
      }
      auto node_result = node_manager_->get_node(target_schema, target_id);
      if (node_result.ok()) {
        if (const auto target_node = node_result.ValueOrDie();
            target_node->schema_name == target_schema) {
          bool passes_all_filters = true;
          for (const auto& where_clause : where_clauses) {
            auto node_where = apply_where_to_node(where_clause, target_node);
            if (!node_where.ok()) {
              return node_where.status();
            }
            if (!node_where.ValueOrDie()) {
              passes_all_filters = false;
              break;
            }
          }
          if (passes_all_filters) {
            for (const auto& where_clause : edge_where_clauses) {
              auto edge_where = apply_where_to_edge(where_clause, edge);
              if (!edge_where.ok()) {
                return edge_where.status();
              }
              if (!edge_where.ValueOrDie()) {
                passes_all_filters = false;
                break;
              }
            }
          }
          if (passes_all_filters) {
            IF_DEBUG_ENABLED {
              log_debug("found edge {}:{} -[{}{}]-> {}:{}", source.value(),
                        source_id,
                        traverse->edge_alias().has_value()
                            ? traverse->edge_alias().value() + ":"
                            : "",
                        traverse->edge_type(), traverse->target().value(),
                        target_node->id);
            }
            if (!source_had_match) {
              matched_source_ids.insert(source_id);
              source_had_match = true;
            }
            matched_target_ids.insert(target_node->id);
            auto& conn = query_state.connection_pool().get();
            conn.source = traverse->source();
            conn.source_id = source_id;
            conn.edge_id = edge->get_id();
            conn.edge_alias = traverse->edge_alias();
            conn.edge_type = traverse->edge_type();
            conn.label = "";
            conn.target = traverse->target();
            conn.target_id = target_node->id;

            query_state.connections()[traverse->source().value()][source_id]
                .push_back(conn);
            query_state.incoming()[target_node->id].push_back(conn);
          }
        }
      } else {
        log_warn("Failed to get node {}:{}, error: {}",
                 traverse->target().value(), target_id,
                 node_result.status().ToString());
      }
    }
    if (!source_had_match) {
      IF_DEBUG_ENABLED {
        log_debug("no edge found from {}:{}", source.value(), source_id);
      }
      unmatched_source_ids.insert(source_id);
    }
  }
  IF_DEBUG_ENABLED {
    log_debug("found {} neighbors for {}", matched_target_ids.size(),
              traverse->target().toString());
  }

  llvm::DenseSet<int64_t> all_target_ids;
  if (traverse->traverse_type() == TraverseType::Right ||
      traverse->traverse_type() == TraverseType::Full) {
    all_target_ids =
        get_ids_from_table(
            get_table(target_schema, query_state.temporal_context.get())
                .ValueOrDie())
            .ValueOrDie();
  }

  const bool is_self_join = source_schema == target_schema;
  auto strategy =
      JoinStrategyFactory::create(traverse->traverse_type(), is_self_join);

  IF_DEBUG_ENABLED {
    log_debug("Using {} join strategy (self_join={})", strategy->name(),
              is_self_join);
  }

  JoinInput join_input{
      .source_ids = query_state.ids()[source.value()],
      .all_target_ids = all_target_ids,
      .matched_source_ids = matched_source_ids,
      .matched_target_ids = matched_target_ids,
      .existing_target_ids = query_state.get_ids(traverse->target()),
      .unmatched_source_ids = unmatched_source_ids,
      .is_self_join = is_self_join,
  };

  auto join_output = strategy->compute(join_input);

  query_state.ids()[traverse->target().value()] = join_output.target_ids;

  if (join_output.rebuild_source_table) {
    for (auto id : join_output.source_ids_to_remove) {
      IF_DEBUG_ENABLED {
        log_debug("remove unmatched node={}:{}", source.value(), id);
      }
      query_state.remove_node(id, source);
    }
    auto table_result = filter_table_by_id(query_state.tables[source.value()],
                                           query_state.ids()[source.value()]);
    if (!table_result.ok()) {
      return table_result.status();
    }
    query_state.tables[source.value()] = table_result.ValueOrDie();
  }

  std::vector<std::shared_ptr<Node>> neighbors;
  for (auto id : query_state.ids()[traverse->target().value()]) {
    if (auto node_res = node_manager_->get_node(target_schema, id);
        node_res.ok()) {
      neighbors.push_back(node_res.ValueOrDie());
    }
  }
  auto target_table_schema = schema_registry_->get(target_schema).ValueOrDie();
  ARROW_ASSIGN_OR_RAISE(auto target_table, create_table_from_nodes(
                                               target_table_schema, neighbors));
  ARROW_RETURN_NOT_OK(
      query_state.update_table(target_table, traverse->target()));
  return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Database::update  - dispatch to Mode 1 or Mode 2
// ---------------------------------------------------------------------------
arrow::Result<UpdateResult> Database::update(const UpdateQuery& uq) {
  if (uq.node_id().has_value()) {
    return update_by_id(uq);
  }
  if (uq.has_match()) {
    return update_by_match(uq);
  }
  return arrow::Status::Invalid(
      "UpdateQuery must specify a node ID or a MATCH query");
}

// ---------------------------------------------------------------------------
// Mode 1: update a single node by schema + ID
// ---------------------------------------------------------------------------
arrow::Result<UpdateResult> Database::update_by_id(const UpdateQuery& uq) {
  UpdateResult result;

  auto schema_result = schema_registry_->get(uq.schema());
  if (!schema_result.ok()) {
    return arrow::Status::KeyError("Schema '", uq.schema(), "' not found");
  }
  const auto& schema = schema_result.ValueOrDie();

  // Resolve fields upfront - fail early on bad field names
  std::vector<FieldUpdate> resolved;
  resolved.reserve(uq.assignments().size());
  for (const auto& a : uq.assignments()) {
    const size_t first_dot = a.field_name.find('.');
    const std::string field_name = first_dot == std::string::npos
                                       ? a.field_name
                                       : a.field_name.substr(0, first_dot);
    if (field_name.empty()) {
      return arrow::Status::Invalid("Invalid SET field '", a.field_name,
                                    "' for ID-based update");
    }
    std::vector<std::string> nested_path{};
    if (first_dot != std::string::npos) {
      size_t start = first_dot + 1;
      while (start < a.field_name.size()) {
        const size_t end = a.field_name.find('.', start);
        std::string segment = a.field_name.substr(
            start, end == std::string::npos ? std::string::npos : end - start);
        if (segment.empty()) {
          return arrow::Status::Invalid("Invalid SET field '", a.field_name,
                                        "' for ID-based update");
        }
        nested_path.push_back(std::move(segment));
        if (end == std::string::npos) break;
        start = end + 1;
      }
    }

    auto field = schema->get_field(field_name);
    if (!field) {
      return arrow::Status::Invalid(
          "Field '", field_name, "' not found in schema '", uq.schema(), "'");
    }
    resolved.push_back(
        FieldUpdate{field, a.value, uq.update_type(), std::move(nested_path)});
  }

  const int64_t id = uq.node_id().value();
  if (const auto r =
          update_node_fields(uq.schema(), id, resolved, uq.update_type());
      !r.ok()) {
    result.failed_count++;
    result.errors.push_back(uq.schema() + "(" + std::to_string(id) +
                            "): " + r.status().ToString());
  } else {
    result.updated_count = 1;
  }
  return result;
}

// ---------------------------------------------------------------------------
// Mode 2: find nodes via MATCH query, then batch-update each
// ---------------------------------------------------------------------------
arrow::Result<UpdateResult> Database::update_by_match(const UpdateQuery& uq) {
  UpdateResult result;
  const auto& match_query = uq.match_query().value();

  // 1. Build alias -> schema from node declarations
  std::unordered_map<std::string, std::string> alias_to_schema;
  if (match_query.from().is_declaration())
    alias_to_schema[match_query.from().value()] = match_query.from().schema();
  for (const auto& clause : match_query.clauses()) {
    if (clause->type() != Clause::Type::TRAVERSE) continue;
    auto t = std::static_pointer_cast<Traverse>(clause);
    if (t->source().is_declaration())
      alias_to_schema[t->source().value()] = t->source().schema();
    if (t->target().is_declaration())
      alias_to_schema[t->target().value()] = t->target().schema();
  }

  // 2. Group SET assignments by alias: { alias -> [FieldUpdate] }
  std::unordered_map<std::string, std::vector<FieldUpdate>> updates_by_alias;
  for (const auto& a : uq.assignments()) {
    const auto parsed = FieldRef::from_string(a.field_name);
    if (parsed.variable().empty()) {
      return arrow::Status::Invalid(
          "SET field '", a.field_name,
          "' must be alias-qualified (e.g. u.age) in a MATCH-based update");
    }
    const std::string& alias = parsed.variable();
    const std::string& bare_field = parsed.field_name();

    std::shared_ptr<Field> field;
    if (auto trav = match_query.find_traverse(alias); trav != nullptr) {
      auto edge_schema = edge_store_->get_edge_schema(trav->edge_type());
      if (!edge_schema) {
        return arrow::Status::KeyError("Edge schema '", trav->edge_type(),
                                       "' not found");
      }
      field = edge_schema->get_field(bare_field);
      if (!field) {
        return arrow::Status::Invalid("Field '", bare_field,
                                      "' not found in edge schema '",
                                      trav->edge_type(), "'");
      }
    } else {
      auto it = alias_to_schema.find(alias);
      if (it == alias_to_schema.end()) {
        return arrow::Status::Invalid("Alias '", alias,
                                      "' not found in MATCH query");
      }
      ARROW_ASSIGN_OR_RAISE(auto schema, schema_registry_->get(it->second));
      field = schema->get_field(bare_field);
      if (!field) {
        return arrow::Status::Invalid(
            "Field '", bare_field, "' not found in schema '", it->second, "'");
      }
    }
    updates_by_alias[alias].push_back(
        FieldUpdate{field, a.value, uq.update_type(), parsed.nested_path()});
  }

  // 3. Build SELECT with node IDs needed for updates and edge lookups.
  std::set<std::string> id_column_set;
  for (const auto& [alias, _] : updates_by_alias) {
    if (auto trav = match_query.find_traverse(alias)) {
      id_column_set.insert(trav->source().value() + ".id");
      id_column_set.insert(trav->target().value() + ".id");
    } else {
      id_column_set.insert(alias + ".id");
    }
  }
  Query id_query(match_query.from(), match_query.clauses(),
                 std::make_shared<Select>(std::vector<std::string>(
                     id_column_set.begin(), id_column_set.end())),
                 match_query.inline_where(), match_query.execution_config(),
                 match_query.temporal_snapshot());

  // 4. Run the MATCH query once
  ARROW_ASSIGN_OR_RAISE(auto query_result, this->query(id_query));
  auto table = query_result->table();
  if (!table || table->num_rows() == 0) {
    return result;
  }

  // 5. Apply updates per alias
  for (const auto& [alias, fields] : updates_by_alias) {
    if (auto trav = match_query.find_traverse(alias); !trav) {
      auto id_column = table->GetColumnByName(alias + ".id");
      if (!id_column) {
        return arrow::Status::Invalid("Could not find '", alias,
                                      ".id' column in query results");
      }
      apply_updates(alias_to_schema.at(alias), id_column, fields,
                    uq.update_type(), result);
    } else {
      auto src_col = table->GetColumnByName(trav->source().value() + ".id");
      auto tgt_col = table->GetColumnByName(trav->target().value() + ".id");
      if (!src_col || !tgt_col) {
        return arrow::Status::Invalid(
            "Could not find source/target ID columns for edge alias '", alias,
            "'");
      }
      llvm::DenseSet<int64_t> updated_edge_ids;
      for (int ci = 0; ci < src_col->num_chunks(); ci++) {
        const auto src_chunk =
            std::static_pointer_cast<arrow::Int64Array>(src_col->chunk(ci));
        const auto tgt_chunk =
            std::static_pointer_cast<arrow::Int64Array>(tgt_col->chunk(ci));
        for (int64_t i = 0; i < src_chunk->length(); i++) {
          if (src_chunk->IsNull(i) || tgt_chunk->IsNull(i)) continue;
          auto edges_res = edge_store_->get_outgoing_edges(src_chunk->Value(i),
                                                           trav->edge_type());
          if (!edges_res.ok()) continue;
          for (const auto& edge : edges_res.ValueOrDie()) {
            if (edge->get_target_id() != tgt_chunk->Value(i)) continue;
            if (!updated_edge_ids.insert(edge->get_id()).second) continue;
            if (auto upd = edge->update_fields(fields); !upd.ok()) {
              result.failed_count++;
              result.errors.push_back("edge(" + std::to_string(edge->get_id()) +
                                      "): " + upd.status().ToString());
            } else {
              result.updated_count++;
            }
          }
        }
      }
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// apply_updates - iterate an ID column and batch-update each node
// ---------------------------------------------------------------------------
void Database::apply_updates(
    const std::string& schema_name,
    const std::shared_ptr<arrow::ChunkedArray>& id_column,
    const std::vector<FieldUpdate>& fields, UpdateType update_type,
    UpdateResult& result) {
  for (int ci = 0; ci < id_column->num_chunks(); ci++) {
    const auto chunk =
        std::static_pointer_cast<arrow::Int64Array>(id_column->chunk(ci));
    for (int64_t i = 0; i < chunk->length(); i++) {
      if (chunk->IsNull(i)) continue;
      const int64_t node_id = chunk->Value(i);

      if (auto r =
              update_node_fields(schema_name, node_id, fields, update_type);
          !r.ok()) {
        result.failed_count++;
        result.errors.push_back(schema_name + "(" + std::to_string(node_id) +
                                "): " + r.status().ToString());
      } else {
        result.updated_count++;
      }
    }
  }
}

}  // namespace tundradb
