#ifndef EDGE_HPP
#define EDGE_HPP

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "edge_view.hpp"
#include "entity_ops.hpp"
#include "node_arena.hpp"
#include "schema.hpp"
#include "schema_layout.hpp"
#include "temporal_context.hpp"
#include "types.hpp"
#include "update_type.hpp"

namespace tundradb {
class Edge {
 private:
  const int64_t id_;
  const int64_t source_id_;
  const int64_t target_id_;
  const std::string type_;
  const int64_t created_ts_;

  std::unordered_map<std::string, Value> data_;
  std::unordered_map<std::string, Value> properties_;

  std::unique_ptr<NodeHandle> handle_;
  std::shared_ptr<NodeArena> arena_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;

 public:
  Edge(int64_t id, int64_t source_id, int64_t target_id, std::string type,
       int64_t created_ts)
      : id_(id),
        source_id_(source_id),
        target_id_(target_id),
        type_(std::move(type)),
        created_ts_(created_ts) {}

  Edge(int64_t id, int64_t source_id, int64_t target_id, std::string type,
       int64_t created_ts, std::unordered_map<std::string, Value> data,
       std::unordered_map<std::string, Value> properties)
      : id_(id),
        source_id_(source_id),
        target_id_(target_id),
        type_(std::move(type)),
        created_ts_(created_ts),
        data_(std::move(data)),
        properties_(std::move(properties)) {}

  Edge(int64_t id, int64_t source_id, int64_t target_id, std::string type,
       int64_t created_ts, std::unique_ptr<NodeHandle> handle,
       std::shared_ptr<NodeArena> arena, std::shared_ptr<Schema> schema,
       std::shared_ptr<SchemaLayout> layout,
       std::unordered_map<std::string, Value> properties = {})
      : id_(id),
        source_id_(source_id),
        target_id_(target_id),
        type_(std::move(type)),
        created_ts_(created_ts),
        properties_(std::move(properties)),
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

  [[nodiscard]] bool has_schema() const { return schema_ != nullptr; }
  [[nodiscard]] std::shared_ptr<Schema> get_schema() const { return schema_; }
  [[nodiscard]] std::shared_ptr<SchemaLayout> get_layout() const {
    return layout_;
  }
  [[nodiscard]] NodeHandle* get_handle() const { return handle_.get(); }
  [[nodiscard]] NodeArena* get_arena() const { return arena_.get(); }

  // --- Unified field access (via entity_ops) ---

  [[nodiscard]] arrow::Result<Value> get_value(
      const std::shared_ptr<Field>& field) const {
    return entity_ops::get_value(field, handle_.get(), arena_.get(), layout_,
                                 data_, properties_);
  }

  arrow::Result<bool> update_fields(const std::vector<FieldUpdate>& updates) {
    return entity_ops::apply_updates(handle_.get(), arena_.get(), layout_,
                                     data_, properties_, updates);
  }

  arrow::Result<bool> update(const std::shared_ptr<Field>& field, Value value,
                             UpdateType update_type = UpdateType::SET) {
    return update_fields({{field, std::move(value), update_type}});
  }

  // --- Properties ---

  [[nodiscard]] const std::unordered_map<std::string, Value>& get_properties()
      const {
    return entity_ops::get_properties(handle_.get(), arena_.get(), properties_);
  }

  EdgeView view(TemporalContext* ctx = nullptr) {
    if (!ctx) {
      VersionInfo* vi = handle_ ? handle_->version_info_ : nullptr;
      return {this, vi, layout_};
    }
    VersionInfo* resolved = ctx->resolve_version(id_, *handle_);
    return {this, resolved, layout_};
  }
};

// ============================================================================
// EdgeView inline implementations (after Edge is fully defined)
// ============================================================================

inline arrow::Result<Value> EdgeView::get_value(
    const std::shared_ptr<Field>& field) const {
  if (resolved_version_ == nullptr) {
    return edge_->get_value(field);
  }
  const NodeHandle* handle = edge_->get_handle();
  assert(handle != nullptr && "Versioned edge must have a handle");
  return entity_ops::get_value_at_version(field, *handle, resolved_version_,
                                          layout_);
}

inline const std::unordered_map<std::string, Value>& EdgeView::get_properties()
    const {
  if (resolved_version_ == nullptr) {
    return edge_->get_properties();
  }
  return entity_ops::get_properties_at_version(resolved_version_,
                                               edge_->get_properties());
}

inline bool EdgeView::is_visible() const {
  const NodeHandle* handle = edge_->get_handle();
  if (!handle || !handle->is_versioned()) {
    return true;
  }
  return resolved_version_ != nullptr;
}

}  // namespace tundradb
#endif  // EDGE_HPP
