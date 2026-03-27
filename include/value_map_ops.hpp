#ifndef VALUE_MAP_OPS_HPP
#define VALUE_MAP_OPS_HPP

#include <arrow/status.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "field_update.hpp"

namespace tundradb {
namespace value_map_ops {

inline arrow::Status apply(std::unordered_map<std::string, Value>& map,
                           const FieldUpdate& update) {
  const auto& key = update.field->name();

  switch (update.op) {
    case UpdateType::SET:
      if (update.value.is_null()) {
        map.erase(key);
      } else {
        map[key] = update.value;
      }
      return arrow::Status::OK();

    case UpdateType::APPEND:
      // map[key] default-constructs a NA Value when absent;
      // append_element promotes NA → ARRAY automatically.
      return map[key].append_element(update.value);
  }
  return arrow::Status::Invalid("Unknown UpdateType");
}

inline arrow::Status apply(std::unordered_map<std::string, Value>& map,
                           const std::vector<FieldUpdate>& updates) {
  for (const auto& upd : updates) {
    ARROW_RETURN_NOT_OK(apply(map, upd));
  }
  return arrow::Status::OK();
}

}  // namespace value_map_ops
}  // namespace tundradb

#endif  // VALUE_MAP_OPS_HPP
