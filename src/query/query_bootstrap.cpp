#include "main/database.hpp"
#include "query/temporal_context.hpp"

namespace tundradb {

/// Prepare the per-query execution state from the FROM clause and optional
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

  IF_DEBUG_ENABLED {
    log_debug("processing 'from' {}", query.from().toString());
  }
  query_state.from = query.from();
  query_state.from.set_tag(compute_tag(query_state.from));
  ARROW_ASSIGN_OR_RAISE(auto source_schema,
                        query_state.register_schema(query.from()));
  if (!this->schema_registry_->exists(source_schema)) {
    return arrow::Status::KeyError("schema doesn't exit: {}", source_schema);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto source_table,
      this->get_table(source_schema, query_state.temporal_context.get()));
  ARROW_RETURN_NOT_OK(query_state.update_table(source_table, query.from()));
  ARROW_RETURN_NOT_OK(
      query_state.compute_fully_qualified_names(query.from()).status());

  return prepare_query(query, query_state);
}

/// Inline any WHERE expressions that can be applied directly to the FROM alias
/// before later clauses run.
arrow::Status Database::inline_from_where(const Query& query,
                                          QueryState& query_state,
                                          QueryResult& result) const {
  auto where_exps =
      get_where_to_inline(query.from().value(), 0, query.clauses());
  result.mutable_execution_stats().num_where_clauses_inlined +=
      where_exps.size();
  return inline_where(query.from(), query_state.tables[query.from().value()],
                      query_state, where_exps)
      .status();
}

}  // namespace tundradb
