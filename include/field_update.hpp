#ifndef FIELD_UPDATE_HPP
#define FIELD_UPDATE_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "schema.hpp"
#include "types.hpp"
#include "update_type.hpp"

namespace tundradb {

struct FieldUpdate {
  std::shared_ptr<Field> field;
  Value value;
  UpdateType op = UpdateType::SET;
  /// Generic nested path inside a composite field.
  /// Examples:
  /// - field="props", nested_path={"foo"} means props["foo"].
  /// - field="struct_col", nested_path={"a","b"} means struct_col.a.b.
  std::vector<std::string> nested_path;
};

struct IndexedFieldUpdate {
  uint16_t field_idx;
  Value value;
  UpdateType op;
  std::vector<std::string> nested_path;
};

}  // namespace tundradb

#endif  // FIELD_UPDATE_HPP
