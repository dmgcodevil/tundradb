#include "main/database.hpp"
#include "query/join.hpp"

namespace tundradb {

namespace {

std::vector<std::shared_ptr<WhereExpr>> extract_predicates(
    const std::vector<PlannedPredicate>& predicates) {
  std::vector<std::shared_ptr<WhereExpr>> exprs;
  exprs.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    exprs.push_back(predicate.expr);
  }
  return exprs;
}

void record_traverse_pushdowns(
    QueryResult& result, const std::vector<PlannedPredicate>& predicates) {
  auto& stats = result.mutable_execution_stats();
  stats.num_where_predicates_pushed_to_traverse += predicates.size();
  stats.num_where_clauses_inlined += predicates.size();
  for (const auto& predicate : predicates) {
    const auto text = predicate.expr->toString();
    stats.inlined_conditions.push_back(text);
    stats.traverse_pushdown_conditions.push_back(text);
  }
}

}  // namespace

/// Execute one TRAVERSE clause by expanding the hop, applying the configured
/// join semantics, and refreshing the affected alias tables in QueryState.
arrow::Status Database::execute_traverse(
    const std::shared_ptr<Traverse>& traverse, QueryState& query_state,
    const Query& query, size_t clause_index, size_t traverse_index,
    QueryResult& result) const {
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
  if (query.inline_where() && query_state.where_plan.has_value()) {
    const auto& where_plan = *query_state.where_plan;
    if (traverse_index >= where_plan.traverse_filters.size()) {
      return arrow::Status::Invalid("Missing WHERE traverse plan for index ",
                                    traverse_index);
    }

    const auto& traverse_plan = where_plan.traverse_filters[traverse_index];
    where_clauses = extract_predicates(traverse_plan.target_filters);
    edge_where_clauses = extract_predicates(traverse_plan.edge_filters);
    record_traverse_pushdowns(result, traverse_plan.target_filters);
    record_traverse_pushdowns(result, traverse_plan.edge_filters);
    for (const auto& wc : where_clauses) wc->set_inlined(true);
    for (const auto& wc : edge_where_clauses) wc->set_inlined(true);
  } else {
    if (query.inline_where()) {
      where_clauses = get_where_to_inline(traverse->target().value(),
                                          clause_index + 1, query.clauses());
    }
    if (traverse->edge_alias().has_value()) {
      edge_where_clauses = get_where_to_inline(
          traverse->edge_alias().value(), clause_index + 1, query.clauses());
    }
    for (const auto& wc : where_clauses) wc->set_inlined(true);
    for (const auto& wc : edge_where_clauses) wc->set_inlined(true);
    result.mutable_execution_stats().num_where_clauses_inlined +=
        where_clauses.size() + edge_where_clauses.size();
  }

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

  ARROW_ASSIGN_OR_RAISE(
      auto hop_result,
      expand_traverse_hop(*traverse, target_schema, query_state, where_clauses,
                          edge_where_clauses));

  llvm::DenseSet<int64_t> all_target_ids;
  if (traverse->traverse_type() == TraverseType::Right ||
      traverse->traverse_type() == TraverseType::Full) {
    ARROW_ASSIGN_OR_RAISE(
        auto all_target_table,
        get_table(target_schema, query_state.temporal_context.get()));
    for (const auto& predicate : where_clauses) {
      ARROW_ASSIGN_OR_RAISE(all_target_table,
                            filter(all_target_table, *predicate, true));
    }
    ARROW_ASSIGN_OR_RAISE(all_target_ids, get_ids_from_table(all_target_table));
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
      .matched_source_ids = hop_result.matched_source_ids,
      .matched_target_ids = hop_result.matched_target_ids,
      .existing_target_ids = query_state.get_ids(traverse->target()),
      .unmatched_source_ids = hop_result.unmatched_source_ids,
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

}  // namespace tundradb
