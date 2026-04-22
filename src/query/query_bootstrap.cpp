#include "main/database.hpp"
#include "query/temporal_context.hpp"
#include "query/where_planner.hpp"

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

}  // namespace

/// Prepare the per-query execution state from the root clause and optional
/// temporal snapshot before clause execution begins.
arrow::Status Database::init_query_state(const Query& query,
                                         QueryState& query_state) const {
  if (query.temporal_snapshot().has_value()) {
    query_state.temporal_context =
        std::make_unique<TemporalContext>(query.temporal_snapshot().value());
    IF_DEBUG_ENABLED {
      log_debug("Temporal query: AS OF VALIDTIME={}, TXNTIME={}",
                query_state.temporal_context->snapshot().valid_time,
                query_state.temporal_context->snapshot().tx_time);
    }
  }

  query_state.reserve_capacity(query);
  query_state.node_manager = this->node_manager_;
  query_state.edge_store = this->edge_store_;

  IF_DEBUG_ENABLED { log_debug("processing root {}", query.root().toString()); }
  query_state.root = query.root();
  query_state.root.set_tag(compute_tag(query_state.root));
  ARROW_ASSIGN_OR_RAISE(auto source_schema,
                        query_state.register_schema(query.root()));
  if (!this->schema_registry_->exists(source_schema)) {
    return arrow::Status::KeyError("schema doesn't exit: {}", source_schema);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto source_table,
      this->get_table(source_schema, query_state.temporal_context.get()));
  ARROW_RETURN_NOT_OK(query_state.update_table(source_table, query.root()));
  ARROW_RETURN_NOT_OK(
      query_state.compute_fully_qualified_names(query.root()).status());

  ARROW_RETURN_NOT_OK(prepare_query(query, query_state));
  if (query.inline_where()) {
    ARROW_ASSIGN_OR_RAISE(query_state.where_plan,
                          build_where_plan(query, query_state));
  }
  return arrow::Status::OK();
}

/// Inline any WHERE expressions that can be applied directly to the root alias
/// before later clauses run.
arrow::Status Database::inline_root_where(const Query& query,
                                          QueryState& query_state,
                                          QueryResult& result) const {
  if (!query.inline_where()) {
    return arrow::Status::OK();
  }

  if (!query_state.where_plan.has_value()) {
    return arrow::Status::Invalid(
        "Missing WHERE plan for inline root filtering");
  }

  const auto& root_filters = query_state.where_plan->root_filters;
  if (root_filters.empty()) {
    return arrow::Status::OK();
  }

  auto& stats = result.mutable_execution_stats();
  for (const auto& predicate : root_filters) {
    stats.record_planned_predicate(PlannedPredicateSite::Root,
                                   predicate.expr->toString(), predicate.mode);
  }
  return inline_where(query.root(), query_state.tables[query.root().value()],
                      query_state, extract_predicates(root_filters), false)
      .status();
}

}  // namespace tundradb
