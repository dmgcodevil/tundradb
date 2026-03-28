#ifndef ENTITY_OPS_HPP
#define ENTITY_OPS_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "field_update.hpp"
#include "node_arena.hpp"

namespace tundradb {
namespace entity_ops {

// --- Writes ---

inline arrow::Result<bool> apply_updates(
    NodeHandle* handle, NodeArena* arena,
    const std::shared_ptr<SchemaLayout>& layout,
    std::unordered_map<std::string, Value>& data,
    const std::vector<FieldUpdate>& updates) {
  // Mode A/B: arena -> arena handles everything (all fields are schema fields)
  if (arena) {
    assert(handle && "Mode A/B requires a valid handle");
    return arena->apply_updates(*handle, layout, updates);
  }

  // Mode C: no arena -> all to local data map
  for (const auto& upd : updates) {
    const auto& key = upd.field->name();
    if (upd.op == UpdateType::SET) {
      if (upd.value.is_null()) {
        data.erase(key);
      } else {
        data[key] = upd.value;
      }
    } else if (upd.op == UpdateType::APPEND) {
      ARROW_RETURN_NOT_OK(data[key].append_element(upd.value));
    }
  }
  return true;
}

// --- Reads ---

inline arrow::Result<Value> get_value(
    const std::shared_ptr<Field>& field, const NodeHandle* handle,
    const NodeArena* arena, const std::shared_ptr<SchemaLayout>& layout,
    const std::unordered_map<std::string, Value>& data) {
  // Schema field: arena (Mode A & B) or data_ map (Mode C)
  if (arena && handle) {
    return NodeArena::get_value(*handle, layout, field);
  }
  auto it = data.find(field->name());
  if (it != data.end()) return it->second;
  return arrow::Status::KeyError("Field not found: ", field->name());
}

// --- Temporal reads (Mode A only, used by views) ---

inline arrow::Result<Value> get_value_at_version(
    const std::shared_ptr<Field>& field, const NodeHandle& handle,
    const VersionInfo* version, const std::shared_ptr<SchemaLayout>& layout) {
  return NodeArena::get_value_at_version(handle, version, layout, field);
}

}  // namespace entity_ops
}  // namespace tundradb

#endif  // ENTITY_OPS_HPP
