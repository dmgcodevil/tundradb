#ifndef JOIN_HPP
#define JOIN_HPP

#include <llvm/ADT/DenseSet.h>

#include <memory>

#include "query.hpp"

namespace tundradb {

/**
 * @brief Input data for join ID computation
 *
 * Captures the state accumulated during edge traversal so that
 * a JoinStrategy can decide which target (and source) IDs survive.
 */
struct JoinInput {
  // All node IDs currently in the source schema within query state
  const llvm::DenseSet<int64_t>& source_ids;

  // All node IDs that exist in the target table (full scan of target schema)
  const llvm::DenseSet<int64_t>& all_target_ids;

  // Source nodes that had at least one matching edge
  const llvm::DenseSet<int64_t>& matched_source_ids;

  // Target nodes that were reached via matching edges
  const llvm::DenseSet<int64_t>& matched_target_ids;

  // Target IDs already accumulated from a previous traversal that shares
  // the same target alias (e.g. multi-pattern queries). Empty on the first
  // pass.
  const llvm::DenseSet<int64_t>& existing_target_ids;

  // Source nodes that had NO matching edge
  const llvm::DenseSet<int64_t>& unmatched_source_ids;

  // Whether source and target resolve to the same concrete schema
  bool is_self_join;
};

/**
 * @brief Output of join ID computation
 */
struct JoinOutput {
  // Final set of target node IDs to store in query_state.ids[target]
  llvm::DenseSet<int64_t> target_ids;

  // Source IDs that should be removed from query_state (INNER join pruning)
  llvm::DenseSet<int64_t> source_ids_to_remove;

  // Whether the source table needs to be rebuilt after pruning
  bool rebuild_source_table = false;
};

/**
 * @brief Strategy interface for computing join results
 *
 * Each join type (INNER, LEFT, RIGHT, FULL) implements this interface
 * to determine which node IDs should be included in the query result.
 *
 * The strategy only computes IDs - it does not modify QueryState or
 * touch Arrow tables.  That keeps it pure, testable, and composable.
 */
class JoinStrategy {
 public:
  virtual ~JoinStrategy() = default;

  /**
   * Compute which target/source IDs survive this join.
   */
  [[nodiscard]] virtual JoinOutput compute(const JoinInput& input) const = 0;

  /**
   * Human-readable name for logging / debugging.
   */
  [[nodiscard]] virtual const char* name() const noexcept = 0;
};

/**
 * INNER JOIN
 *
 * Only matched targets survive.
 * Unmatched sources are pruned (and the source table is rebuilt).
 *
 * When existing_target_ids is non-empty (multi-pattern), the result is
 * the intersection of existing and newly matched target IDs.
 */
class InnerJoinStrategy final : public JoinStrategy {
 public:
  [[nodiscard]] JoinOutput compute(const JoinInput& input) const override;
  [[nodiscard]] const char* name() const noexcept override { return "INNER"; }
};

/**
 * LEFT JOIN
 *
 * All source nodes are kept. Target IDs are the union of matched
 * targets and any previously accumulated targets (multi-pattern).
 */
class LeftJoinStrategy final : public JoinStrategy {
 public:
  [[nodiscard]] JoinOutput compute(const JoinInput& input) const override;
  [[nodiscard]] const char* name() const noexcept override { return "LEFT"; }
};

/**
 * RIGHT JOIN  (self-join variant)
 *
 * target_ids = all_targets − matched_sources
 *
 * For self-joins the source and target live in the same schema, so
 * we exclude matched *source* IDs to prevent a node appearing both
 * as a matched source and as an unmatched target.
 */
class RightJoinSelfStrategy final : public JoinStrategy {
 public:
  [[nodiscard]] JoinOutput compute(const JoinInput& input) const override;
  [[nodiscard]] const char* name() const noexcept override {
    return "RIGHT_SELF";
  }
};

/**
 * RIGHT JOIN  (cross-schema variant)
 *
 * target_ids = matched_targets ∪ (all_targets − matched_targets)
 *            = all_targets (but computed in two steps so logging is clear)
 *
 * For cross-schema joins, IDs live in separate namespaces, so we compare
 * within the target schema only.
 */
class RightJoinCrossSchemaStrategy final : public JoinStrategy {
 public:
  [[nodiscard]] JoinOutput compute(const JoinInput& input) const override;
  [[nodiscard]] const char* name() const noexcept override {
    return "RIGHT_CROSS";
  }
};

/**
 * FULL OUTER JOIN
 *
 * Combines the RIGHT logic (all targets survive) with the LEFT logic
 * (all sources survive).  Delegates the target-side computation to an
 * inner RIGHT strategy (self or cross-schema).
 */
class FullJoinStrategy final : public JoinStrategy {
 public:
  explicit FullJoinStrategy(std::unique_ptr<JoinStrategy> right_strategy);

  [[nodiscard]] JoinOutput compute(const JoinInput& input) const override;
  [[nodiscard]] const char* name() const noexcept override { return "FULL"; }

 private:
  std::unique_ptr<JoinStrategy> right_strategy_;
};

/**
 * @brief Creates the appropriate JoinStrategy for a given TraverseType
 *        and join context (self-join vs. cross-schema).
 */
class JoinStrategyFactory {
 public:
  static std::unique_ptr<JoinStrategy> create(TraverseType type,
                                              bool is_self_join);
};

}  // namespace tundradb

#endif  // JOIN_HPP
