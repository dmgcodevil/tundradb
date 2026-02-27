#include "join.hpp"

#include "logger.hpp"
#include "utils.hpp"

namespace tundradb {

// ============================================================================
//  INNER JOIN
// ============================================================================
//
//  Semantics:  Keep only nodes that have a match on BOTH sides.
//
//  - Single-pattern example:
//
//    Graph:
//      User(0) --works_at--> Company(0)     (Google)
//      User(1) --works_at--> Company(1)     (Apple)
//      User(2)                              (no edge)
//      Company(2)                           (no incoming edge)
//
//    Query:  FROM u:User TRAVERSE u -works_at-> c:Company   (INNER)
//
//    After edge traversal:
//      matched_source_ids   = {0, 1}     <- users that had an edge
//      matched_target_ids   = {0, 1}     <- companies that were reached
//      unmatched_source_ids = {2}        <- User(2) had no edge
//      existing_target_ids  = {}         <- first time we see "c"
//
//    Strategy output:
//      target_ids           = {0, 1}     <- only matched companies
//      source_ids_to_remove = {2}        <- User(2) is pruned
//      rebuild_source_table = true
//
//    Result rows:  (User 0, Company 0), (User 1, Company 1)
//
//  - Multi-pattern example:
//
//    Two traversals share the same target alias "c":
//      TRAVERSE a -works_at-> c:Company
//      TRAVERSE b -works_at-> c:Company
//
//    After 1st traversal:  existing_target_ids = {0}     (from pattern a)
//    After 2nd traversal:  matched_target_ids  = {0, 1}  (from pattern b)
//
//    We INTERSECT existing ∩ matched = {0}
//    Only Company(0) survives both patterns.
//
// ============================================================================

JoinOutput InnerJoinStrategy::compute(const JoinInput& input) const {
  JoinOutput out;

  if (input.existing_target_ids.empty()) {
    // First traversal targeting this alias: keep all matched
    out.target_ids = input.matched_target_ids;
  } else {
    // Multi-pattern: intersect with what previous traversals produced
    dense_intersection(input.existing_target_ids, input.matched_target_ids,
                       out.target_ids);
  }

  // Source pruning: remove sources that had no match
  out.source_ids_to_remove = input.unmatched_source_ids;
  out.rebuild_source_table = !out.source_ids_to_remove.empty();

  IF_DEBUG_ENABLED {
    log_debug(
        "INNER JOIN: {} matched targets, {} sources to remove, "
        "rebuild_source={}",
        out.target_ids.size(), out.source_ids_to_remove.size(),
        out.rebuild_source_table);
  }

  return out;
}

// ============================================================================
//  LEFT JOIN
// ============================================================================
//
//  Semantics:  Keep ALL source nodes (even if they have no match).
//              Target side = only nodes that were actually reached.
//              Sources without a match get NULL columns for the target.
//
//  - Example:
//
//    Graph:
//      User(0) --works_at--> Company(0)
//      User(1)                             (no edge)
//
//    Query:  FROM u:User LEFT TRAVERSE u -works_at-> c:Company
//
//    After edge traversal:
//      matched_source_ids  = {0}
//      matched_target_ids  = {0}
//      unmatched_source_ids= {1}
//      existing_target_ids = {}
//
//    Strategy output:
//      target_ids          = {0}       <- only matched companies
//      source_ids_to_remove= {}        <- no pruning!
//      rebuild_source_table= false
//
//    Result rows:
//      (User 0, Company 0)            <- matched
//      (User 1, NULL)                 <- unmatched source, target is NULL
//
//  - Multi-pattern:
//
//    Two LEFT traversals share target "c":
//      Pattern 1 matched targets: {0}      -> existing = {0}
//      Pattern 2 matched targets: {0, 1}
//
//    Result: existing ∪ matched = {0, 1}   (union, not intersection!)
//
// ============================================================================

JoinOutput LeftJoinStrategy::compute(const JoinInput& input) const {
  JoinOutput out;

  // Start with whatever was accumulated from previous traversals
  out.target_ids = input.existing_target_ids;
  // Union in newly matched targets
  out.target_ids.insert(input.matched_target_ids.begin(),
                        input.matched_target_ids.end());

  // Sources are NEVER pruned in a LEFT join

  IF_DEBUG_ENABLED {
    log_debug("LEFT JOIN: {} target IDs (matched={}, existing={})",
              out.target_ids.size(), input.matched_target_ids.size(),
              input.existing_target_ids.size());
  }

  return out;
}

// ============================================================================
//  RIGHT JOIN - self-join variant
// ============================================================================
//
//  Semantics:  Keep ALL target nodes (even unmatched ones).
//              Unmatched targets get NULL columns for the source.
//
//  Why a separate "self" variant?
//  When source and target are the same schema (e.g., User -friends-> User),
//  the IDs live in the SAME namespace.  A node can be both a source and a
//  target.  If User(0) matched as a source, we must NOT also include it as
//  an "unmatched target" - that would produce a duplicate row.
//
//  Formula:  target_ids = all_targets − matched_sources
//
//  - Example:
//
//    Graph (self-join, same schema "User"):
//      User(0) --friends--> User(1)
//      User(0) --friends--> User(2)
//      User(3)                             (isolated node)
//
//    Query:  FROM u:User RIGHT TRAVERSE u -friends-> f:User
//
//    Note: u and f are aliases but both resolve to schema "User"
//
//    After edge traversal:
//      matched_source_ids  = {0}         <- User(0) had outgoing edges
//      matched_target_ids  = {1, 2}      <- users that were reached
//      all_target_ids      = {0,1,2,3}   <- every user in the table
//
//    Strategy: all_targets − matched_sources = {0,1,2,3} − {0} = {1,2,3}
//
//    Why not  all_targets − matched_targets = {0,3}?
//    Because we want User(1) and User(2) to appear as matched targets
//    with their source connections.  The "right" side is all targets
//    that are NOT already covered as matched sources.
//
//    Result rows:
//      (User 0, User 1)   <- matched
//      (User 0, User 2)   <- matched
//      (NULL,   User 3)   <- unmatched target
//
// ============================================================================

JoinOutput RightJoinSelfStrategy::compute(const JoinInput& input) const {
  JoinOutput out;

  dense_difference(input.all_target_ids, input.matched_source_ids,
                   out.target_ids);

  IF_DEBUG_ENABLED {
    log_debug(
        "RIGHT JOIN (self): all_targets={}, matched_sources={}, result={}",
        input.all_target_ids.size(), input.matched_source_ids.size(),
        out.target_ids.size());
  }

  return out;
}

// ============================================================================
//  RIGHT JOIN - cross-schema variant
// ============================================================================
//
//  Semantics:  Same as RIGHT (keep all targets), but source and target are
//              DIFFERENT schemas.  IDs live in separate namespaces, so there
//              is no collision risk.  We simply include all target IDs.
//
//  Formula:  target_ids = matched_targets ∪ (all_targets − matched_targets)
//                       = all_targets
//
//  We compute it in two steps (matched + unmatched) so the debug log shows
//  exactly which targets were reached and which were not.
//
//  - Example:
//
//    Graph:
//      User(0) --works_at--> Company(0)   (Google)
//      User(1) --works_at--> Company(0)   (Google)
//      Company(1)                         (Apple, no incoming edges)
//
//    Query:  FROM u:User RIGHT TRAVERSE u -works_at-> c:Company
//
//    After edge traversal:
//      matched_source_ids  = {0, 1}
//      matched_target_ids  = {0}          <- only Google was reached
//      all_target_ids      = {0, 1}       <- Google + Apple
//
//    Strategy: matched ∪ unmatched = {0} ∪ {1} = {0, 1}
//
//    Result rows:
//      (User 0, Company 0)     <- matched (Google)
//      (User 1, Company 0)     <- matched (Google)
//      (NULL,   Company 1)     <- unmatched target (Apple)
//
//    Why not use the self-join formula here?
//
//    With per-schema IDs, User(0) and Company(0) both have id=0 but they
//    are in different schemas.  If we did all_targets − matched_sources:
//      {0, 1} − {0, 1} = {}   <- WRONG!  We'd lose all companies!
//
// ============================================================================

JoinOutput RightJoinCrossSchemaStrategy::compute(const JoinInput& input) const {
  JoinOutput out;

  out.target_ids = input.matched_target_ids;
  llvm::DenseSet<int64_t> unmatched;
  dense_difference(input.all_target_ids, input.matched_target_ids, unmatched);
  out.target_ids.insert(unmatched.begin(), unmatched.end());

  IF_DEBUG_ENABLED {
    log_debug(
        "RIGHT JOIN (cross): matched_targets={}, unmatched_targets={}, "
        "total={}",
        input.matched_target_ids.size(), unmatched.size(),
        out.target_ids.size());
  }

  return out;
}

// ============================================================================
//  FULL OUTER JOIN
// ============================================================================
//
//  Semantics:  Keep ALL nodes from BOTH sides.
//              = LEFT (all sources kept) + RIGHT (all targets kept)
//
//  Implementation: delegates to the appropriate RIGHT strategy for the
//  target side.  The source side is handled by NOT pruning any sources
//  (source_ids_to_remove stays empty).
//
//  - Cross-schema example:
//
//    Graph:
//      User(0) --works_at--> Company(0)
//      User(1)                             (no edge)
//      Company(1)                          (no incoming edge)
//
//    Query:  FROM u:User FULL TRAVERSE u -works_at-> c:Company
//
//    After edge traversal:
//      matched_source_ids  = {0}
//      matched_target_ids  = {0}
//      unmatched_source_ids= {1}
//      all_target_ids      = {0, 1}
//
//    RIGHT strategy (cross):  target_ids = {0} ∪ {1} = {0, 1}
//    FULL adds:               source_ids_to_remove = {}  (no pruning)
//
//    Result rows:
//      (User 0, Company 0)     <- matched
//      (User 1, NULL)          <- unmatched source
//      (NULL,   Company 1)     <- unmatched target
//
//  - Self-join example:
//
//    Graph:
//      User(0) --friends--> User(1)
//      User(2)                             (isolated)
//
//    Query:  FROM u:User FULL TRAVERSE u -friends-> f:User
//
//    RIGHT strategy (self):  all_targets − matched_sources = {0,1,2} − {0}
//                            = {1, 2}
//    FULL adds:              source_ids_to_remove = {}
//
//    Result rows:
//      (User 0, User 1)       <- matched
//      (NULL,   User 2)       <- unmatched target
//      Note: User(0) appears as source only, not duplicated as target
//
// ============================================================================

FullJoinStrategy::FullJoinStrategy(std::unique_ptr<JoinStrategy> right_strategy)
    : right_strategy_(std::move(right_strategy)) {}

JoinOutput FullJoinStrategy::compute(const JoinInput& input) const {
  // Target side: delegate to the appropriate RIGHT strategy
  JoinOutput out = right_strategy_->compute(input);

  // Source side: all sources survive (no pruning)
  // source_ids_to_remove stays empty, rebuild_source_table stays false

  IF_DEBUG_ENABLED {
    log_debug("FULL JOIN (via {}): {} target IDs", right_strategy_->name(),
              out.target_ids.size());
  }

  return out;
}

// ============================================================================
//  Factory
// ============================================================================
//
//  Picks the right strategy based on TraverseType and whether source/target
//  resolve to the same concrete schema (self-join).
//
//  The key branching:
//    INNER  -> InnerJoinStrategy                         (always)
//    LEFT   -> LeftJoinStrategy                          (always)
//    RIGHT  -> RightJoinSelfStrategy                     (if self-join)
//           -> RightJoinCrossSchemaStrategy              (if cross-schema)
//    FULL   -> FullJoinStrategy wrapping RightJoinSelf   (if self-join)
//           -> FullJoinStrategy wrapping RightJoinCross  (if cross-schema)
//
// ============================================================================

std::unique_ptr<JoinStrategy> JoinStrategyFactory::create(TraverseType type,
                                                          bool is_self_join) {
  switch (type) {
    case TraverseType::Inner:
      return std::make_unique<InnerJoinStrategy>();

    case TraverseType::Left:
      return std::make_unique<LeftJoinStrategy>();

    case TraverseType::Right:
      if (is_self_join) {
        return std::make_unique<RightJoinSelfStrategy>();
      }
      return std::make_unique<RightJoinCrossSchemaStrategy>();

    case TraverseType::Full:
      if (is_self_join) {
        return std::make_unique<FullJoinStrategy>(
            std::make_unique<RightJoinSelfStrategy>());
      }
      return std::make_unique<FullJoinStrategy>(
          std::make_unique<RightJoinCrossSchemaStrategy>());
  }

  // Unreachable if the enum is exhaustive, but just in case:
  return std::make_unique<InnerJoinStrategy>();
}

}  // namespace tundradb
