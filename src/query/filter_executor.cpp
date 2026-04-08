#include <arrow/compute/api.h>
#include <llvm/ADT/DenseSet.h>

#include "main/database.hpp"

namespace tundradb {

/// Apply a WHERE clause immediately when it targets one materialized alias, or
/// defer it for post-processing when it spans multiple aliases.
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

}  // namespace tundradb
