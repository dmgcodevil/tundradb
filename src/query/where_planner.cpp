#include "query/where_planner.hpp"

#include <iterator>
#include <optional>
#include <unordered_map>
#include <utility>

#include "query/execution.hpp"

namespace tundradb {

namespace {

/**
 * @brief Output of recursively decomposing one WHERE subtree.
 *
 * `pushdowns` are alias-local fragments that can be scheduled earlier.
 * `residual` is the part that must survive to final table filtering.
 */
struct DecomposeResult {
  std::vector<std::pair<std::string, std::shared_ptr<WhereExpr>>> pushdowns;
  std::shared_ptr<WhereExpr> residual;
};

/**
 * @brief Earliest planner site where an alias can be filtered safely.
 *
 * `traverse_index == std::nullopt` means the root phase.
 * Otherwise the alias is available while executing
 * `traversals[traverse_index]`.
 */
struct AliasActivation {
  std::optional<size_t> traverse_index;
  AliasKind kind;
};

std::shared_ptr<WhereExpr> combine_with_and(std::shared_ptr<WhereExpr> left,
                                            std::shared_ptr<WhereExpr> right) {
  if (!left) return right;
  if (!right) return left;
  return LogicalExpr::and_expr(std::move(left), std::move(right));
}

void append_pushdowns(
    std::vector<std::pair<std::string, std::shared_ptr<WhereExpr>>>& dst,
    std::vector<std::pair<std::string, std::shared_ptr<WhereExpr>>>& src) {
  dst.insert(dst.end(), std::make_move_iterator(src.begin()),
             std::make_move_iterator(src.end()));
}

/**
 * @brief Map each alias to the earliest execution site where it exists.
 *
 * The root alias is available before traversals start.
 * A target alias and optional edge alias become available at the traverse that
 * introduces them. We intentionally do not place source aliases here because
 * they must already be available from an earlier root/target binding.
 */
std::unordered_map<std::string, AliasActivation> build_alias_activation_map(
    const Query& query, const QueryState& query_state) {
  std::unordered_map<std::string, AliasActivation> activation;
  activation.emplace(query.root().value(),
                     AliasActivation{std::nullopt, AliasKind::Node});

  for (size_t traverse_index = 0;
       traverse_index < query_state.traversals.size(); ++traverse_index) {
    const auto& traverse = query_state.traversals[traverse_index];
    activation.try_emplace(traverse.target().value(),
                           AliasActivation{traverse_index, AliasKind::Node});
    if (traverse.edge_alias().has_value()) {
      activation.try_emplace(traverse.edge_alias().value(),
                             AliasActivation{traverse_index, AliasKind::Edge});
    }
  }

  return activation;
}

/**
 * @brief Recursively split a WHERE subtree into pushable and residual parts.
 *
 * Safe rules:
 * - single-alias subtree: push whole subtree
 * - AND: recurse into both children
 * - everything else: keep residual
 */
DecomposeResult decompose_where(const std::shared_ptr<WhereExpr>& expr) {
  if (!expr) return {};

  const auto& vars = expr->get_all_variables();
  if (vars.size() == 1) {
    return {{{*vars.begin(), expr}}, nullptr};
  }

  auto logical = std::dynamic_pointer_cast<LogicalExpr>(expr);
  if (logical && logical->op() == LogicalOp::AND) {
    auto left = decompose_where(logical->left());
    auto right = decompose_where(logical->right());

    DecomposeResult out;
    out.pushdowns.reserve(left.pushdowns.size() + right.pushdowns.size());
    append_pushdowns(out.pushdowns, left.pushdowns);
    append_pushdowns(out.pushdowns, right.pushdowns);
    out.residual =
        combine_with_and(std::move(left.residual), std::move(right.residual));
    return out;
  }

  return {{}, expr};
}

arrow::Status append_pushdown(WhereExecutionPlan& plan,
                              const AliasActivation& activation,
                              PlannedPredicate predicate) {
  if (!activation.traverse_index.has_value()) {
    if (activation.kind != AliasKind::Node) {
      return arrow::Status::Invalid("Root phase cannot host edge predicates");
    }
    plan.root_filters.push_back(std::move(predicate));
    return arrow::Status::OK();
  }

  auto& traverse_plan = plan.traverse_filters[*activation.traverse_index];
  if (activation.kind == AliasKind::Edge) {
    traverse_plan.edge_filters.push_back(std::move(predicate));
  } else {
    traverse_plan.target_filters.push_back(std::move(predicate));
  }
  return arrow::Status::OK();
}

}  // namespace

std::shared_ptr<WhereExpr> combine_predicates_with_and(
    const std::vector<PlannedPredicate>& predicates) {
  std::shared_ptr<WhereExpr> combined;
  for (const auto& predicate : predicates) {
    combined = combine_with_and(std::move(combined), predicate.expr);
  }
  return combined;
}

arrow::Result<WhereExecutionPlan> build_where_plan(
    const Query& query, const QueryState& query_state) {
  WhereExecutionPlan plan;
  plan.traverse_filters.resize(query_state.traversals.size());
  plan.residual_by_clause.resize(query.clauses().size());

  auto activation = build_alias_activation_map(query, query_state);

  for (size_t clause_index = 0; clause_index < query.clauses().size();
       ++clause_index) {
    const auto& clause = query.clauses()[clause_index];
    if (clause->type() != Clause::Type::WHERE) continue;

    auto where = std::dynamic_pointer_cast<WhereExpr>(clause);
    if (!where) {
      return arrow::Status::Invalid("Clause ", clause_index,
                                    " is WHERE but not a WhereExpr");
    }

    auto parts = decompose_where(where);
    for (auto& [alias, expr] : parts.pushdowns) {
      auto it = activation.find(alias);
      if (it == activation.end()) {
        return arrow::Status::KeyError(
            "Alias '", alias, "' is not registered for WHERE pushdown");
      }

      ARROW_RETURN_NOT_OK(append_pushdown(
          plan, it->second, PlannedPredicate{clause_index, std::move(expr)}));
    }
    plan.residual_by_clause[clause_index] = std::move(parts.residual);
  }

  return plan;
}

}  // namespace tundradb
