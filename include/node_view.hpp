#ifndef NODE_VIEW_HPP
#define NODE_VIEW_HPP

#include <arrow/api.h>

#include "node_arena.hpp"
#include "schema.hpp"
#include "temporal_context.hpp"
#include "types.hpp"

namespace tundradb {

// Forward declarations
class Node;

/**
 * NodeView: lightweight view of a Node at a specific temporal snapshot.
 *
 * Purpose:
 * - Provides the same field access interface as Node
 * - Resolves version once (at construction), then uses it for all field reads
 * - Avoids per-field map lookups in TemporalContext
 *
 * Lifecycle:
 * - Created by Node::view(TemporalContext*)
 * - Should be short-lived (lifetime tied to query execution)
 *
 * Usage:
 *   TemporalContext ctx(TemporalSnapshot::as_of_valid(timestamp));
 *   auto view = node->view(&ctx);
 *   auto age = view.get_value_ptr(age_field);  // Uses resolved version
 */
class NodeView {
 private:
  Node* node_;                     // Back-reference to Node (for node_id, etc.)
  VersionInfo* resolved_version_;  // Resolved once at construction
  NodeArena* arena_;               // For field resolution
  std::shared_ptr<SchemaLayout>
      layout_;  // Schema layout (shared_ptr for proper lifetime)

 public:
  /**
   * Constructor: resolves version immediately.
   *
   * If ctx is nullptr, uses current version (no time-travel).
   * If no version is visible at snapshot, resolved_version_ will be nullptr.
   */
  NodeView(Node* node, VersionInfo* resolved_version, NodeArena* arena,
           std::shared_ptr<SchemaLayout> layout)
      : node_(node),
        resolved_version_(resolved_version),
        arena_(arena),
        layout_(std::move(layout)) {}

  /**
   * Get field value pointer (same interface as Node).
   *
   * If resolved_version_ is nullptr:
   * - Node is non-versioned -> read from base node via Node::get_value_ptr
   *
   * Otherwise:
   * - Uses arena to resolve field from version chain
   * - Starts from resolved_version_ (already filtered by time)
   */
  arrow::Result<const char*> get_value_ptr(
      const std::shared_ptr<Field>& field) const;

  /**
   * Get field value as ValueRef (lightweight reference to data).
   */
  arrow::Result<ValueRef> get_value_ref(
      const std::shared_ptr<Field>& field) const {
    auto ptr_result = get_value_ptr(field);
    if (!ptr_result.ok()) {
      return ptr_result.status();
    }

    return ValueRef{ptr_result.ValueOrDie(), field->type()};
  }

  /**
   * Get field value (copies data into Value).
   */
  arrow::Result<Value> get_value(const std::shared_ptr<Field>& field) const;

  /**
   * Check if this view represents a visible node.
   *
   * Returns true if:
   * - Node is non-versioned (resolved_version_ == nullptr && arena exists)
   * - Node is versioned and has a visible version at the snapshot
   *
   * Returns false if:
   * - Node is versioned but didn't exist at the temporal snapshot
   */
  [[nodiscard]] bool is_visible() const;

  /**
   * Get the resolved version info.
   */
  [[nodiscard]] const VersionInfo* get_resolved_version() const {
    return resolved_version_;
  }

  /**
   * Get the underlying node.
   */
  [[nodiscard]] Node* get_node() const { return node_; }
};

}  // namespace tundradb

#endif  // NODE_VIEW_HPP
