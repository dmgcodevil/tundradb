#include "core/edge.hpp"

namespace tundradb {

// ---------------------------------------------------------------------------
// Edge
// ---------------------------------------------------------------------------

arrow::Result<Value> Edge::get_value(
    const std::shared_ptr<Field>& field) const {
  if (field && field->name() == field_names::kId) {
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

arrow::Result<const char*> Edge::get_value_ptr(
    const std::shared_ptr<Field>& field) const {
  if (!field) {
    return arrow::Status::Invalid("Field is null");
  }
  if (field->name() == field_names::kId) {
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

arrow::Result<bool> Edge::update_fields(
    const std::vector<FieldUpdate>& updates) {
  if (!arena_ || !handle_) {
    return arrow::Status::Invalid(
        "update_fields requires arena-backed edge with valid handle");
  }
  return arena_->apply_updates(*handle_, layout_, updates);
}

arrow::Result<bool> Edge::update(const std::shared_ptr<Field>& field,
                                 Value value, UpdateType update_type) {
  return update_fields({{field, std::move(value), update_type}});
}

EdgeView Edge::view(TemporalContext* ctx) {
  if (!ctx) {
    VersionInfo* vi = handle_ ? handle_->version_info_ : nullptr;
    return {this, vi, layout_};
  }
  VersionInfo* resolved = ctx->resolve_edge_version(id_, *handle_);
  return {this, resolved, layout_};
}

// ---------------------------------------------------------------------------
// EdgeView
// ---------------------------------------------------------------------------

arrow::Result<Value> EdgeView::get_value(
    const std::shared_ptr<Field>& field) const {
  if (resolved_version_ == nullptr) {
    return edge_->get_value(field);
  }
  const NodeHandle* handle = edge_->get_handle();
  assert(handle != nullptr && "Versioned edge must have a handle");
  return NodeArena::get_value_at_version(*handle, resolved_version_, layout_,
                                         field);
}

arrow::Result<const char*> EdgeView::get_value_ptr(
    const std::shared_ptr<Field>& field) const {
  if (resolved_version_ == nullptr || !layout_) {
    return edge_->get_value_ptr(field);
  }
  if (field && (field->name() == field_names::kId ||
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

arrow::Result<ValueRef> EdgeView::get_value_ref(
    const std::shared_ptr<Field>& field) const {
  ARROW_ASSIGN_OR_RAISE(const auto ptr, get_value_ptr(field));
  return ValueRef{ptr, field->type()};
}

bool EdgeView::is_visible() const {
  const NodeHandle* handle = edge_->get_handle();
  if (!handle || !handle->is_versioned()) {
    return true;
  }
  return resolved_version_ != nullptr;
}

}  // namespace tundradb
