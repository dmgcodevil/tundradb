#ifndef EDGE_VIEW_HPP
#define EDGE_VIEW_HPP

#include <arrow/api.h>

#include <unordered_map>

#include "common/types.hpp"
#include "memory/node_arena.hpp"
#include "schema/schema.hpp"

namespace tundradb {

class Edge;

/// A read-only, point-in-time projection of an Edge.
///
/// When temporal versioning is active, the view resolves field values to
/// the version that was current at the requested snapshot.  Structural
/// fields (id, source_id, target_id, created_ts) are always delegated to
/// the owning Edge.
class EdgeView {
 private:
  Edge* edge_;
  VersionInfo* resolved_version_;
  std::shared_ptr<SchemaLayout> layout_;

 public:
  EdgeView(Edge* edge, VersionInfo* resolved_version,
           std::shared_ptr<SchemaLayout> layout)
      : edge_(edge),
        resolved_version_(resolved_version),
        layout_(std::move(layout)) {}

  /// Read a field value, resolving through the version chain if needed.
  arrow::Result<Value> get_value(const std::shared_ptr<Field>& field) const;

  /// Return a raw pointer to the field's in-memory representation.
  arrow::Result<const char*> get_value_ptr(
      const std::shared_ptr<Field>& field) const;

  /// Return a lightweight non-owning reference to the field value.
  arrow::Result<ValueRef> get_value_ref(
      const std::shared_ptr<Field>& field) const;

  /// True when the edge has a visible version at the requested snapshot.
  [[nodiscard]] bool is_visible() const;

  /// The resolved version (nullptr for non-versioned or current edges).
  [[nodiscard]] const VersionInfo* get_resolved_version() const {
    return resolved_version_;
  }

  /// The underlying Edge pointer.
  [[nodiscard]] Edge* get_edge() const { return edge_; }
};

}  // namespace tundradb

#endif  // EDGE_VIEW_HPP
