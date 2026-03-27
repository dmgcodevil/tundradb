#ifndef ENTITY_OPS_HPP
#define ENTITY_OPS_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "field_update.hpp"
#include "node_arena.hpp"
#include "value_map_ops.hpp"

namespace tundradb {
namespace entity_ops {

// --- Writes ---

inline arrow::Result<bool> apply_updates(
    NodeHandle* handle, NodeArena* arena,
    const std::shared_ptr<SchemaLayout>& layout,
    std::unordered_map<std::string, Value>& data,
    std::unordered_map<std::string, Value>& properties,
    const std::vector<FieldUpdate>& updates) {
  // Mode A: arena + versioning -> arena handles everything
  if (arena && arena->is_versioning_enabled()) {
    assert(handle && "Mode A requires a valid handle");
    return arena->apply_updates(*handle, layout, updates);
  }

  // Mode B: arena, no versioning -> arena for schema, local map for dynamic
  if (arena) {
    assert(handle && "Mode B requires a valid handle");
    std::vector<FieldUpdate> schema_updates;
    for (const auto& upd : updates) {
      if (upd.field->is_dynamic()) {
        ARROW_RETURN_NOT_OK(value_map_ops::apply(properties, upd));
      } else {
        schema_updates.push_back(upd);
      }
    }
    if (!schema_updates.empty()) {
      return arena->apply_updates(*handle, layout, schema_updates);
    }
    return true;
  }

  // Mode C: no arena -> all to local maps
  for (const auto& upd : updates) {
    auto& target = upd.field->is_dynamic() ? properties : data;
    ARROW_RETURN_NOT_OK(value_map_ops::apply(target, upd));
  }
  return true;
}

// --- Reads ---

// NOTE: field/layout are shared_ptr (shared ownership, schema-lifetime
// objects). handle/arena are non-owning borrows — raw pointers to indicate
// nullable, caller-owned objects. Consider migrating to const-ref where
// non-null is guaranteed (e.g. Mode A/B).
inline arrow::Result<Value> get_value(
    const std::shared_ptr<Field>& field, const NodeHandle* handle,
    const NodeArena* arena, const std::shared_ptr<SchemaLayout>& layout,
    const std::unordered_map<std::string, Value>& data,
    const std::unordered_map<std::string, Value>& properties) {
  if (field->is_dynamic()) {
    // Mode A: read from version chain
    if (arena && arena->is_versioning_enabled() && handle) {
      return NodeArena::get_dynamic_field_value(*handle, field->name());
    }
    // Mode B & C: read from local map
    auto it = properties.find(field->name());
    return it != properties.end() ? it->second : Value{};
  }

  // Schema field: arena (Mode A & B) or data_ map (Mode C)
  if (arena && handle) {
    return NodeArena::get_field_value(*handle, layout, field);
  }
  auto it = data.find(field->name());
  if (it != data.end()) return it->second;
  return arrow::Status::KeyError("Field not found: ", field->name());
}

// --- Temporal reads (Mode A only, used by views) ---

inline arrow::Result<Value> get_value_at_version(
    const std::shared_ptr<Field>& field, const NodeHandle& handle,
    const VersionInfo* version, const std::shared_ptr<SchemaLayout>& layout) {
  if (field->is_dynamic()) {
    return NodeArena::get_dynamic_field_value_from_version(version,
                                                           field->name());
  }
  return NodeArena::get_field_value_from_version(handle, version, layout,
                                                 field);
}

inline const std::unordered_map<std::string, Value>& get_properties_at_version(
    const VersionInfo* version,
    const std::unordered_map<std::string, Value>& fallback) {
  auto* snap = NodeArena::get_properties_from_version(version);
  if (snap) return *snap;
  return fallback;
}

// --- Properties map access (current) ---

inline const std::unordered_map<std::string, Value>& get_properties(
    const NodeHandle* handle, const NodeArena* arena,
    const std::unordered_map<std::string, Value>& local_properties) {
  // Mode A: read from version chain snapshot
  if (arena && arena->is_versioning_enabled() && handle) {
    auto* snap = NodeArena::get_properties(*handle);
    if (snap) return *snap;
  }
  // Mode B & C: local map
  return local_properties;
}

}  // namespace entity_ops
}  // namespace tundradb

#endif  // ENTITY_OPS_HPP
