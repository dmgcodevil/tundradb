#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "main/database.hpp"
#include "query/row.hpp"

namespace tundradb {

/// Starting from one root node, walk the prepared query graph and emit the
/// denormalized row variants reachable from that node.
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

/// Materialize rows for one batch of node IDs while honoring the schema's join
/// behavior and the global visited set shared across batches.
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

/// Partition a schema's IDs into fixed-size batches for optional parallel row
/// population.
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

/// Populate denormalized rows for the full query by processing each involved
/// schema in traversal order.
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
    schema_join_types[query_state.root.value()] = TraverseType::Left;
  } else {
    // The root alias is always inner by default.
    schema_join_types[query_state.root.value()] = TraverseType::Inner;
  }

  // Only apply LEFT JOIN to the root alias if it is directly involved in a
  // LEFT JOIN traversal.
  for (const auto& traverse : traverses) {
    if (traverse.source().value() == query_state.root.value() &&
        (traverse.traverse_type() == TraverseType::Left ||
         traverse.traverse_type() == TraverseType::Full)) {
      schema_join_types[query_state.root.value()] = traverse.traverse_type();
      break;
    }
  }

  // Build ordered list of schema references to process
  std::vector<SchemaRef> ordered_schemas;
  ordered_schemas.push_back(query_state.root);

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

/// Convert populated row objects into the final Arrow table matching the
/// denormalized output schema.
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

/// Build the final query result table by denormalizing query state, applying
/// deferred WHERE clauses, and projecting the SELECT list.
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

}  // namespace tundradb
