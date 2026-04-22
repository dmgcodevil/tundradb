#include <arrow/compute/api.h>
#include <llvm/ADT/DenseSet.h>

#include "main/database.hpp"

namespace tundradb {

namespace {

/**
 * @brief Apply an Arrow-native filter to one materialized alias table.
 *
 * This is the fast path for single-alias predicates that Arrow can evaluate
 * directly. The caller decides whether to fall back to row-by-row evaluation
 * if this returns an error for a predicate that requires row evaluation.
 */
arrow::Result<std::shared_ptr<arrow::Table>> filter_alias_table(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<WhereExpr>& where) {
  return filter(table, *where, true);
}

/**
 * @brief Build a boolean mask for row-evaluated filtering of one alias table.
 *
 * The mask is produced by reloading each live node for @p alias from storage
 * and evaluating @p where with the expression engine's row path. The returned
 * mask aligns with the current Arrow table rows and is later passed to
 * `arrow::compute::Filter(...)`.
 */
arrow::Result<std::shared_ptr<arrow::Array>> build_row_eval_mask(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<WhereExpr>& where, const std::string& alias,
    const QueryState& query_state,
    const std::shared_ptr<NodeManager>& node_manager) {
  ARROW_ASSIGN_OR_RAISE(const auto resolved_schema,
                        query_state.resolve_schema(SchemaRef::parse(alias)));

  llvm::DenseSet<int64_t> keep_ids;
  for (const auto id : query_state.ids().at(alias)) {
    auto node_res = node_manager->get_node(resolved_schema, id);
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
                                  alias, "'");
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
  return mask_array;
}

/**
 * @brief Filter one alias table using row-by-row expression evaluation.
 *
 * This fallback path is used for predicates that cannot be evaluated by Arrow
 * directly, such as nested field access. It builds a boolean mask with
 * @ref build_row_eval_mask and then applies that mask to the current table.
 */
arrow::Result<std::shared_ptr<arrow::Table>> filter_alias_table_with_row_eval(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<WhereExpr>& where, const std::string& alias,
    const QueryState& query_state,
    const std::shared_ptr<NodeManager>& node_manager) {
  ARROW_ASSIGN_OR_RAISE(
      auto mask_array,
      build_row_eval_mask(table, where, alias, query_state, node_manager));
  ARROW_ASSIGN_OR_RAISE(
      auto filtered_datum,
      arrow::compute::Filter(arrow::Datum(table), arrow::Datum(mask_array)));
  return filtered_datum.table();
}

}  // namespace

/// Decide whether a WHERE clause should be skipped, deferred, or applied to an
/// already materialized alias table in legacy execution mode.
arrow::Result<Database::WhereDisposition> Database::classify_where_filter(
    const std::shared_ptr<WhereExpr>& where,
    const QueryState& query_state) const {
  if (where->inlined()) {
    IF_DEBUG_ENABLED {
      log_debug("where '{}' is inlined, skip", where->toString());
    }
    return WhereDisposition{WhereDisposition::Kind::Skip, ""};
  }

  const auto& variables = where->get_all_variables();
  if (variables.empty()) {
    return arrow::Status::Invalid(
        "where clause field must have variable "
        "<var>.<field>, actual={}",
        where->toString());
  }

  if (variables.size() != 1) {
    IF_DEBUG_ENABLED {
      log_debug("Defer compound WHERE expression: '{}'", where->toString());
    }
    return WhereDisposition{WhereDisposition::Kind::Defer, ""};
  }

  const std::string alias = *variables.begin();
  if (!query_state.tables.contains(alias)) {
    if (!query_state.aliases().contains(alias)) {
      return arrow::Status::Invalid("Unknown variable '{}'", alias);
    }
    return WhereDisposition{WhereDisposition::Kind::Defer, alias};
  }

  return WhereDisposition{WhereDisposition::Kind::ApplyToAlias, alias};
}

/// Apply a single-alias WHERE clause to one materialized alias table.
arrow::Status Database::apply_alias_where(
    const std::shared_ptr<WhereExpr>& where, const std::string& alias,
    QueryState& query_state) const {
  IF_DEBUG_ENABLED {
    log_debug("Processing WHERE clause: '{}'", where->toString());
  }

  const auto table = query_state.tables.at(alias);
  arrow::Result<std::shared_ptr<arrow::Table>> filtered_table_result =
      where->requires_row_eval()
          ? filter_alias_table_with_row_eval(table, where, alias, query_state,
                                             node_manager_)
          : filter_alias_table(table, where);

  if (!filtered_table_result.ok()) {
    log_error("Failed to process where: '{}', error: {}", where->toString(),
              filtered_table_result.status().ToString());
    return filtered_table_result.status();
  }

  ARROW_RETURN_NOT_OK(query_state.update_table(
      filtered_table_result.ValueOrDie(), SchemaRef::parse(alias)));
  return arrow::Status::OK();
}

}  // namespace tundradb
