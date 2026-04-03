#ifndef EDGE_HPP
#define EDGE_HPP

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "edge_view.hpp"
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

  [[nodiscard]] bool has_schema() const { return schema_ != nullptr; }
  [[nodiscard]] std::shared_ptr<Schema> get_schema() const { return schema_; }
  [[nodiscard]] std::shared_ptr<SchemaLayout> get_layout() const {
    return layout_;
  }
  [[nodiscard]] NodeHandle* get_handle() const { return handle_.get(); }
  [[nodiscard]] NodeArena* get_arena() const { return arena_.get(); }

  // --- Unified field access ---

  [[nodiscard]] arrow::Result<Value> get_value(
      const std::shared_ptr<Field>& field) const {
    if (field && (field->name() == field_names::kId ||
                  field->name() == field_names::kEdgeId)) {
      return Value{id_};
    }
    if (field && field->name() == field_names::kSourceId)
      return Value{source_id_};
    if (field && field->name() == field_names::kTargetId)
      return Value{target_id_};
    if (field && field->name() == field_names::kCreatedTs)
      return Value{created_ts_};
    if (!arena_ || !handle_) {
      return arrow::Status::Invalid(
          "get_value requires arena-backed edge with valid handle");
    }
    return NodeArena::get_value(*handle_, layout_, field);
  }

  [[nodiscard]] arrow::Result<const char*> get_value_ptr(
      const std::shared_ptr<Field>& field) const {
    if (!field) {
      return arrow::Status::Invalid("Field is null");
    }
    if (field->name() == field_names::kId ||
        field->name() == field_names::kEdgeId) {
      return reinterpret_cast<const char*>(&id_);
    }
    if (field->name() == field_names::kSourceId)
      return reinterpret_cast<const char*>(&source_id_);
    if (field->name() == field_names::kTargetId)
      return reinterpret_cast<const char*>(&target_id_);
    if (field->name() == field_names::kCreatedTs)
      return reinterpret_cast<const char*>(&created_ts_);
    if (arena_ && handle_) {
      return NodeArena::get_value_ptr(*handle_, layout_, field);
    }
    return arrow::Status::KeyError("Field not found: ", field->name());
  }

  arrow::Result<bool> update_fields(const std::vector<FieldUpdate>& updates) {
    if (!arena_ || !handle_) {
      return arrow::Status::Invalid(
          "update_fields requires arena-backed edge with valid handle");
    }
    return arena_->apply_updates(*handle_, layout_, updates);
  }

  arrow::Result<bool> update(const std::shared_ptr<Field>& field, Value value,
                             UpdateType update_type = UpdateType::SET) {
    return update_fields({{field, std::move(value), update_type}});
  }

  EdgeView view(TemporalContext* ctx = nullptr) {
    if (!ctx) {
      VersionInfo* vi = handle_ ? handle_->version_info_ : nullptr;
      return {this, vi, layout_};
    }
    VersionInfo* resolved = ctx->resolve_edge_version(id_, *handle_);
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
  return NodeArena::get_value_at_version(*handle, resolved_version_, layout_,
                                         field);
}

inline arrow::Result<const char*> EdgeView::get_value_ptr(
    const std::shared_ptr<Field>& field) const {
  if (resolved_version_ == nullptr || !layout_) {
    return edge_->get_value_ptr(field);
  }
  if (field && (field->name() == field_names::kId ||
                field->name() == field_names::kEdgeId ||
                field->name() == field_names::kSourceId ||
                field->name() == field_names::kTargetId ||
                field->name() == field_names::kCreatedTs)) {
    return edge_->get_value_ptr(field);
  }
  const NodeHandle* handle = edge_->get_handle();
  if (!handle) {
    return edge_->get_value_ptr(field);
  }
  return edge_->get_arena()->get_value_ptr_at_version(
      *handle, resolved_version_, layout_, field);
}

inline arrow::Result<ValueRef> EdgeView::get_value_ref(
    const std::shared_ptr<Field>& field) const {
  ARROW_ASSIGN_OR_RAISE(const auto ptr, get_value_ptr(field));
  return ValueRef{ptr, field->type()};
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
