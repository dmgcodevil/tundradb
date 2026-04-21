#ifndef QUERY_WHERE_PLANNER_HPP
#define QUERY_WHERE_PLANNER_HPP

#include <memory>
#include <vector>

#include "query/query.hpp"

namespace tundradb {

struct QueryState;

/**
 * @brief Metadata describing how a planned predicate participates in the plan.
 *
 * This enum does not decide where or whether execution applies a predicate.
 * Execution is driven by the plan shape itself:
 * - predicates present in `root_filters` / `traverse_filters` are applied early
 * - predicates present in `residual_by_clause` are applied later
 *
 * `mode` exists to make the planner output explicit and to keep execution
 * statistics honest.
 *
 * `Consume` means the fragment appears only in the early phase of the plan.
 * `PrefilterOnly` means the fragment appears in an early phase and is also
 * retained in `residual_by_clause`.
 */
enum class PlannedPredicateMode {
  Consume,
  PrefilterOnly,
};

/**
 * @brief One predicate fragment scheduled by the WHERE planner.
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
  ///< Descriptive planner metadata; not the source of execution truth.
  PlannedPredicateMode mode = PlannedPredicateMode::Consume;
};

/**
 * @brief Predicates that can be applied while executing one traverse hop.
 *
 * Each predicate carries a @c PlannedPredicateMode describing whether the
 * planner also retained that same fragment in `residual_by_clause`.
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
 *
 * A fragment may therefore appear:
 * - only in `root_filters` / `traverse_filters`
 * - both in an early filter vector and in `residual_by_clause`
 *
 * `PlannedPredicateMode` documents which of those two layouts the planner
 * chose, but execution still follows the containers above.
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
 * Planning rules:
 * - A subtree that references exactly one alias is a pushdown candidate.
 * - `AND` is decomposed recursively.
 * - Mixed-alias `OR` and alias-to-alias comparisons remain residual.
 * - If the alias stays non-nullable up to the WHERE clause, the fragment is
 *   planned as `Consume`.
 * - If the alias may become nullable before the WHERE clause, the fragment is
 *   planned as `PrefilterOnly` and also retained as residual.
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
