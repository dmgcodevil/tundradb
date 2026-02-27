#include "core.hpp"

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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow_utils.hpp"
#include "join.hpp"
#include "logger.hpp"
#include "row.hpp"
#include "temporal_context.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

namespace tundradb {

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
            auto conn = connections[0];
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
  std::vector<std::string>
      field_names;  // Cache field names to avoid repeated lookups

  for (const auto& field : output_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
    field_names.push_back(field->name());
  }

  // Pre-allocate builders for better performance
  const size_t num_rows = rows->size();
  for (auto& builder : builders) {
    ARROW_RETURN_NOT_OK(builder->Reserve(num_rows));
  }

  // Populate the builders from each row
  for (const auto& row : *rows) {
    for (size_t i = 0; i < field_names.size(); i++) {
      const auto& field_name = field_names[i];  // Use cached field name

      // Optimization: try indexed access first, fallback to string lookup
      ValueRef value_ref;
      bool has_value = false;

      if (i < row->cells.size() && row->cells[i].data != nullptr) {
        value_ref = row->cells[i];
        has_value = true;
      }

      if (has_value) {
        // We have a value for this field - append directly without creating
        // scalars
        arrow::Status append_status;

        switch (value_ref.type) {
          case ValueType::INT32:
            append_status = static_cast<arrow::Int32Builder*>(builders[i].get())
                                ->Append(value_ref.as_int32());
            break;
          case ValueType::INT64:
            append_status = static_cast<arrow::Int64Builder*>(builders[i].get())
                                ->Append(value_ref.as_int64());
            break;
          case ValueType::DOUBLE:
            append_status =
                static_cast<arrow::DoubleBuilder*>(builders[i].get())
                    ->Append(value_ref.as_double());
            break;
          case ValueType::STRING: {
            const auto& str_ref = value_ref.as_string_ref();
            append_status =
                static_cast<arrow::StringBuilder*>(builders[i].get())
                    ->Append(str_ref.data(), str_ref.length());
            break;
          }
          case ValueType::BOOL:
            append_status =
                static_cast<arrow::BooleanBuilder*>(builders[i].get())
                    ->Append(value_ref.as_bool());
            break;
          default:
            append_status = builders[i]->AppendNull();
            break;
        }

        if (append_status.ok()) {
          continue;
        }
      }

      // Fall back to NULL if we couldn't append the value
      ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
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
  query_state.from = query.from();

  {
    IF_DEBUG_ENABLED {
      log_debug("processing 'from' {}", query.from().toString());
    }
    // Precompute tag for FROM schema (alias-based hash)
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
    if (auto res = query_state.compute_fully_qualified_names(query.from(),
                                                             source_schema);
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
    auto preparation_result =
        prepare_query(const_cast<Query&>(query), query_state);
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

  // Precompute 16-bit alias-based tags for all SchemaRefs
  // Also precompute fully-qualified field names per alias used in the query
  std::vector<std::shared_ptr<WhereExpr>> post_where;
  for (auto i = 0; i < query.clauses().size(); ++i) {
    auto clause = query.clauses()[i];
    switch (clause->type()) {
      case Clause::Type::WHERE: {
        auto where = std::dynamic_pointer_cast<WhereExpr>(clause);
        if (where->inlined()) {
          IF_DEBUG_ENABLED {
            log_debug("where '{}' is inlined, skip", where->toString());
          }
          continue;
        }
        auto variables = where->get_all_variables();
        if (variables.empty()) {
          return arrow::Status::Invalid(
              "where clause field must have variable "
              "<var>.<field>, actual={}",
              where->toString());
        }
        if (variables.size() == 1) {
          IF_DEBUG_ENABLED {
            log_debug("Processing WHERE clause: '{}'", where->toString());
          }

          std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
          std::string variable = *variables.begin();
          if (!query_state.tables.contains(variable)) {
            return arrow::Status::Invalid("Unknown variable '{}'", variable);
          }
          auto table = query_state.tables.at(variable);
          auto filtered_table_result = filter(table, *where, true);
          if (!filtered_table_result.ok()) {
            log_error("Failed to process where: '{}', error: {}",
                      where->toString(),
                      filtered_table_result.status().ToString());
            return filtered_table_result.status();
          }
          ARROW_RETURN_NOT_OK(query_state.update_table(
              filtered_table_result.ValueOrDie(), SchemaRef::parse(variable)));
        } else {
          IF_DEBUG_ENABLED {
            log_debug("Add compound WHERE expression: '{}' to post process",
                      where->toString());
          }
          post_where.emplace_back(where);
        }
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        // Tags and schemas are already set during preparation phase

        // Get resolved schemas using const resolve_schema (read-only)
        ARROW_ASSIGN_OR_RAISE(const auto source_schema,
                              query_state.resolve_schema(traverse->source()));
        ARROW_ASSIGN_OR_RAISE(const auto target_schema,
                              query_state.resolve_schema(traverse->target()));
        // Fully-qualified field names should also be precomputed during
        // preparation
        ARROW_RETURN_NOT_OK(query_state.compute_fully_qualified_names(
            traverse->source(), source_schema));
        ARROW_RETURN_NOT_OK(query_state.compute_fully_qualified_names(
            traverse->target(), target_schema));

        std::vector<std::shared_ptr<WhereExpr>> where_clauses;
        if (query.inline_where()) {
          where_clauses = get_where_to_inline(traverse->target().value(), i + 1,
                                              query.clauses());
          result->mutable_execution_stats().num_where_clauses_inlined +=
              where_clauses.size();
        }
        // Traversal already added to query_state.traversals during preparation
        IF_DEBUG_ENABLED {
          log_debug("Processing TRAVERSE {}-({})->{}",
                    traverse->source().toString(), traverse->edge_type(),
                    traverse->target().toString());
        }
        auto source = traverse->source();
        if (!query_state.tables.contains(source.value())) {
          IF_DEBUG_ENABLED {
            log_debug("Source table '{}' not found. Loading",
                      traverse->source().toString());
          }
          ARROW_ASSIGN_OR_RAISE(
              auto source_table,
              this->get_table(source_schema,
                              query_state.temporal_context.get()));
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
                  .ValueOrDie();  // todo check result
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
            auto node_result =
                node_manager_->get_node(target_schema, target_id);
            if (node_result.ok()) {
              const auto target_node = node_result.ValueOrDie();
              if (target_node->schema_name == target_schema) {
                // Then apply all WHERE clauses with AND logic
                bool passes_all_filters = true;
                // Multiple conditions - could optimize by creating a
                // temporary table and using Arrow expressions For now, use
                // the existing approach but this could be optimized
                for (const auto& where_clause : where_clauses) {
                  if (!apply_where_to_node(where_clause, target_node)
                           .ValueOrDie()) {
                    passes_all_filters = false;
                    break;
                  }
                }
                if (passes_all_filters) {
                  IF_DEBUG_ENABLED {
                    log_debug("found edge {}:{} -[{}]-> {}:{}", source.value(),
                              source_id, traverse->edge_type(),
                              traverse->target().value(), target_node->id);
                  }
                  // record match immediately to avoid extra containers/copies
                  if (!source_had_match) {
                    matched_source_ids.insert(source_id);
                    source_had_match = true;
                  }
                  matched_target_ids.insert(target_node->id);
                  // Use connection pool to avoid allocation
                  auto& conn = query_state.connection_pool().get();
                  conn.source = traverse->source();
                  conn.source_id = source_id;
                  conn.edge_type = traverse->edge_type();
                  conn.label = "";
                  conn.target = traverse->target();
                  conn.target_id = target_node->id;

                  query_state
                      .connections()[traverse->source().value()][source_id]
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

        // For RIGHT/FULL joins we need all target IDs from the table
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
        auto strategy = JoinStrategyFactory::create(traverse->traverse_type(),
                                                    is_self_join);

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

        // Apply target IDs
        query_state.ids()[traverse->target().value()] = join_output.target_ids;

        // Apply source pruning (INNER join removes unmatched sources)
        if (join_output.rebuild_source_table) {
          for (auto id : join_output.source_ids_to_remove) {
            IF_DEBUG_ENABLED {
              log_debug("remove unmatched node={}:{}", source.value(), id);
            }
            query_state.remove_node(id, source);
          }
          auto table_result =
              filter_table_by_id(query_state.tables[source.value()],
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
        auto target_table_schema =
            schema_registry_->get(target_schema).ValueOrDie();
        auto table_result =
            create_table_from_nodes(target_table_schema, neighbors);
        if (!table_result.ok()) {
          log_error("Failed to create table from neighbors: {}",
                    table_result.status().ToString());
          return table_result.status();
        }
        ARROW_RETURN_NOT_OK(query_state.update_table(table_result.ValueOrDie(),
                                                     traverse->target()));
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

  auto output_schema_res = build_denormalized_schema(query_state);
  if (!output_schema_res.ok()) {
    return output_schema_res.status();
  }
  const auto output_schema = output_schema_res.ValueOrDie();
  IF_DEBUG_ENABLED { log_debug("output_schema={}", output_schema->ToString()); }

  auto row_res = populate_rows(query.execution_config(), query_state,
                               query_state.traversals, output_schema);
  if (!row_res.ok()) {
    return row_res.status();
  }
  auto rows = row_res.ValueOrDie();
  auto output_table_res = create_table_from_rows(rows, output_schema);
  if (!output_table_res.ok()) {
    log_error("Failed to create table from rows: {}",
              output_table_res.status().ToString());
    return output_table_res.status();
  }
  auto output_table = output_table_res.ValueOrDie();
  for (const auto& expr : post_where) {
    result->mutable_execution_stats().num_where_clauses_post_processed++;
    IF_DEBUG_ENABLED { log_debug("post process where: {}", expr->toString()); }
    output_table = filter(output_table, *expr, false).ValueOrDie();
  }
  result->set_table(apply_select(query.select(), output_table));
  return result;
}

}  // namespace tundradb
