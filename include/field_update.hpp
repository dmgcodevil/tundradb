#ifndef FIELD_UPDATE_HPP
#define FIELD_UPDATE_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "schema.hpp"
#include "types.hpp"
#include "update_type.hpp"

namespace tundradb {

struct FieldUpdate {
  std::shared_ptr<Field> field;
  Value value;
  UpdateType op = UpdateType::SET;
  /// When set, the update targets a key inside a MAP field rather than
  /// replacing the entire field.  e.g. field="props", map_key="foo"
  /// means "set props['foo'] = value".
  std::optional<std::string> map_key;
};

struct IndexedFieldUpdate {
  uint16_t field_idx;
  Value value;
  UpdateType op;
  std::optional<std::string> map_key;
};

}  // namespace tundradb

#endif  // FIELD_UPDATE_HPP
