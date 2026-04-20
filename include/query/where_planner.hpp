#ifndef QUERY_WHERE_PLANNER_HPP
#define QUERY_WHERE_PLANNER_HPP

#include <memory>
#include <vector>

#include "query/query.hpp"

namespace tundradb {

struct QueryState;

/**
 * @brief One predicate fragment assigned to a concrete execution phase.
 *
 * The planner preserves the original query clause position so execution can
 * keep user-written order when multiple predicates are pulled back into the
 * same root/traverse site.
 *
 * Important: planned fragments reuse subtrees from the original WHERE AST.
 * They should be treated as read-only views rather than mutated copies.
 */
struct PlannedPredicate {
  size_t source_clause_index;  ///< Original WHERE clause position in Query.
  std::shared_ptr<WhereExpr> expr;
};

/**
 * @brief Predicates that can be applied while executing one traverse hop.
 *
 * `target_filters` apply to the hop's target node alias.
 * `edge_filters` apply to the hop's optional edge alias.
 */
struct TraverseWherePlan {
  std::vector<PlannedPredicate> target_filters;
  std::vector<PlannedPredicate> edge_filters;
};

/**
 * @brief Full predicate execution plan derived from a prepared Query.
 *
 * Execution model:
 * - `root_filters` run before the first traverse.
 * - `traverse_filters[i]` run while executing `query_state.traversals[i]`.
 * - `residual_by_clause[i]` is appended when visiting clause `i` in the
 *   normal clause loop and applied later on the denormalized result table.
 */
struct WhereExecutionPlan {
  std::vector<PlannedPredicate> root_filters;
  std::vector<TraverseWherePlan> traverse_filters;
  std::vector<std::shared_ptr<WhereExpr>> residual_by_clause;
};

/**
 * @brief Build a safe pushdown plan for the query's WHERE clauses.
 *
 * Preconditions:
 * - `query_state` has already been prepared via `prepare_query(...)`.
 * - All aliases referenced by WHERE expressions are registered in
 *   `query_state`.
 *
 * Safe split rules:
 * - A subtree that references exactly one alias is pushable as-is.
 * - `AND` is decomposed recursively.
 * - Mixed-alias `OR` and alias-to-alias comparisons remain residual.
 */
arrow::Result<WhereExecutionPlan> build_where_plan(
    const Query& query, const QueryState& query_state);

/**
 * @brief Fold planned predicates into one logical AND expression.
 *
 * This is handy if an executor wants to issue one filter call per site while
 * still planning predicates as individual fragments.
 */
std::shared_ptr<WhereExpr> combine_predicates_with_and(
    const std::vector<PlannedPredicate>& predicates);

}  // namespace tundradb

#endif  // QUERY_WHERE_PLANNER_HPP
