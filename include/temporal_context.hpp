#ifndef TEMPORAL_CONTEXT_HPP
#define TEMPORAL_CONTEXT_HPP

#include <cstdint>
#include <unordered_map>

#include "node_arena.hpp"

namespace tundradb {

// Forward declarations
class Node;
class NodeArena;
struct NodeHandle;
struct VersionInfo;

/**
 * Temporal snapshot: specifies a point in VALIDTIME and TXNTIME.
 */
struct TemporalSnapshot {
  uint64_t valid_time;  // VALIDTIME: when the fact was true in the domain
  uint64_t tx_time;     // TXNTIME: when the database knew about the fact

  // Default: current time for both axes
  TemporalSnapshot() : valid_time(UINT64_MAX), tx_time(UINT64_MAX) {}

  TemporalSnapshot(uint64_t vt, uint64_t tt) : valid_time(vt), tx_time(tt) {}

  // Helper: create snapshot with only VALIDTIME (TXNTIME = current)
  static TemporalSnapshot as_of_valid(uint64_t vt) { return {vt, UINT64_MAX}; }

  // Helper: create snapshot with only TXNTIME (VALIDTIME = current)
  static TemporalSnapshot as_of_tx(uint64_t tt) { return {UINT64_MAX, tt}; }

  // Check if this is a "current" snapshot (no time-travel)
  [[nodiscard]] bool is_current() const {
    return valid_time == UINT64_MAX && tx_time == UINT64_MAX;
  }
};

/**
 * TemporalContext: per-query state for time-travel queries.
 *
 * Responsibilities:
 * - Store the temporal snapshot (valid_time, tx_time)
 * - Cache resolved versions per node to avoid repeated traversals
 * - Provide version resolution for nodes
 *
 * Usage:
 *   TemporalContext ctx(snapshot);
 *   auto view = node->view(&ctx);
 *   view.get_value_ptr(field);  // Uses resolved version
 */
class TemporalContext {
 private:
  TemporalSnapshot snapshot_;

  // Cache: node_id -> resolved VersionInfo*
  // This avoids re-traversing version chains for the same node
  std::unordered_map<int64_t, VersionInfo*> version_cache_;

 public:
  explicit TemporalContext(TemporalSnapshot snapshot) : snapshot_(snapshot) {}

  // Get the snapshot
  [[nodiscard]] const TemporalSnapshot& snapshot() const { return snapshot_; }

  /**
   * Resolve the visible version for a node at this snapshot.
   * Returns nullptr if no version is visible (e.g., node didn't exist yet).
   *
   * Note: naive O(n) implementation, we should use binary search
   */
  VersionInfo* resolve_version(int64_t node_id, const NodeHandle& handle) {
    if (const auto it = version_cache_.find(node_id);
        it != version_cache_.end()) {
      return it->second;
    }

    // Find a visible version by traversing the chain
    VersionInfo* resolved = find_visible_version(handle, snapshot_);

    // Cache result (even if nullptr)
    version_cache_[node_id] = resolved;

    return resolved;
  }

  // Clear cache (useful between query stages or for testing)
  void clear_cache() { version_cache_.clear(); }

 private:
  /**
   * Find the visible version in the chain at the given snapshot.
   *
   * A version is visible if:
   *   valid_from <= snapshot.valid_time < valid_to
   *   AND
   *   tx_from <= snapshot.tx_time < tx_to
   *
   *   returns nullptr if no version is visible
   */
  static VersionInfo* find_visible_version(const NodeHandle& handle,
                                           const TemporalSnapshot& snapshot) {
    VersionInfo* version = handle.version_info_;

    // If snapshot is "current", just return head version
    if (snapshot.is_current()) {
      return version;
    }

    // Traverse chain looking for a visible version
    while (version != nullptr) {
      // Handle UINT64_MAX as "now" (current time)
      const uint64_t vt = snapshot.valid_time;
      const uint64_t tt = snapshot.tx_time;

      // If time is UINT64_MAX, treat as current (always visible)
      const bool valid_match =
          (vt == UINT64_MAX) ||
          (version->valid_from <= vt && vt < version->valid_to);

      const bool tx_match =
          (tt == UINT64_MAX) || (version->tx_from <= tt && tt < version->tx_to);

      if (valid_match && tx_match) {
        return version;  // Found visible version
      }

      version = version->prev;
    }

    return nullptr;  // No visible version at this snapshot
  }
};

}  // namespace tundradb

#endif  // TEMPORAL_CONTEXT_HPP
