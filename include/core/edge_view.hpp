#ifndef EDGE_VIEW_HPP
#define EDGE_VIEW_HPP

#include <arrow/api.h>

#include <unordered_map>

#include "memory/node_arena.hpp"
#include "schema/schema.hpp"
#include "common/types.hpp"

namespace tundradb {

class Edge;

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

  arrow::Result<Value> get_value(const std::shared_ptr<Field>& field) const;
  arrow::Result<const char*> get_value_ptr(
      const std::shared_ptr<Field>& field) const;
  arrow::Result<ValueRef> get_value_ref(
      const std::shared_ptr<Field>& field) const;

  [[nodiscard]] bool is_visible() const;

  [[nodiscard]] const VersionInfo* get_resolved_version() const {
    return resolved_version_;
  }

  [[nodiscard]] Edge* get_edge() const { return edge_; }
};

}  // namespace tundradb

#endif  // EDGE_VIEW_HPP
