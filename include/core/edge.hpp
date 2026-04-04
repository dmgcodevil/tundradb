#ifndef EDGE_HPP
#define EDGE_HPP

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/types.hpp"
#include "core/edge_view.hpp"
#include "core/update_type.hpp"
#include "memory/node_arena.hpp"
#include "memory/schema_layout.hpp"
#include "query/temporal_context.hpp"
#include "schema/schema.hpp"

namespace tundradb {

/// A directed, typed relationship between two nodes.
///
/// Edges always carry structural fields (id, source_id, target_id, type,
/// created_ts).  When an edge schema has been registered the edge also
/// owns an arena-backed property store accessed through get_value / update.
class Edge {
 private:
  const int64_t id_;
  const int64_t source_id_;
  const int64_t target_id_;
  const std::string type_;
  const int64_t created_ts_;

  std::unique_ptr<NodeHandle> handle_;
  std::shared_ptr<NodeArena> arena_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;

 public:
  /// Construct a lightweight edge without property storage.
  Edge(int64_t id, int64_t source_id, int64_t target_id, std::string type,
       int64_t created_ts)
      : id_(id),
        source_id_(source_id),
        target_id_(target_id),
        type_(std::move(type)),
        created_ts_(created_ts) {}

  /// Construct an arena-backed edge with property fields.
  Edge(int64_t id, int64_t source_id, int64_t target_id, std::string type,
       int64_t created_ts, std::unique_ptr<NodeHandle> handle,
       std::shared_ptr<NodeArena> arena, std::shared_ptr<Schema> schema,
       std::shared_ptr<SchemaLayout> layout)
      : id_(id),
        source_id_(source_id),
        target_id_(target_id),
        type_(std::move(type)),
        created_ts_(created_ts),
        handle_(std::move(handle)),
        arena_(std::move(arena)),
        schema_(std::move(schema)),
        layout_(std::move(layout)) {}

  Edge(Edge&&) = default;
  Edge& operator=(Edge&&) = delete;
  Edge(const Edge&) = delete;
  Edge& operator=(const Edge&) = delete;

  [[nodiscard]] int64_t get_id() const { return id_; }
  [[nodiscard]] int64_t get_source_id() const { return source_id_; }
  [[nodiscard]] int64_t get_target_id() const { return target_id_; }
  [[nodiscard]] const std::string& get_type() const { return type_; }
  [[nodiscard]] int64_t get_created_ts() const { return created_ts_; }

  /// True when the edge has a registered schema and arena-backed properties.
  [[nodiscard]] bool has_schema() const { return schema_ != nullptr; }
  [[nodiscard]] std::shared_ptr<Schema> get_schema() const { return schema_; }
  [[nodiscard]] std::shared_ptr<SchemaLayout> get_layout() const {
    return layout_;
  }
  [[nodiscard]] NodeHandle* get_handle() const { return handle_.get(); }
  [[nodiscard]] NodeArena* get_arena() const { return arena_.get(); }

  /// Read a field value by Field descriptor.
  [[nodiscard]] arrow::Result<Value> get_value(
      const std::shared_ptr<Field>& field) const;

  /// Return a raw pointer to the field's in-memory representation.
  [[nodiscard]] arrow::Result<const char*> get_value_ptr(
      const std::shared_ptr<Field>& field) const;

  /// Apply a batch of field updates atomically (one new version).
  arrow::Result<bool> update_fields(const std::vector<FieldUpdate>& updates);

  /// Update a single field (convenience wrapper around update_fields).
  arrow::Result<bool> update(const std::shared_ptr<Field>& field, Value value,
                             UpdateType update_type = UpdateType::SET);

  /// Create a point-in-time view of this edge.
  /// When @p ctx is nullptr the current (latest) version is used.
  EdgeView view(TemporalContext* ctx = nullptr);
};

}  // namespace tundradb
#endif  // EDGE_HPP
