#ifndef FIELD_UPDATE_HPP
#define FIELD_UPDATE_HPP

#include <cstdint>
#include <memory>

#include "schema.hpp"
#include "types.hpp"
#include "update_type.hpp"

namespace tundradb {

struct FieldUpdate {
  std::shared_ptr<Field> field;
  Value value;
  UpdateType op = UpdateType::SET;
};

struct IndexedFieldUpdate {
  uint16_t field_idx;
  Value value;
  UpdateType op;
};

}  // namespace tundradb

#endif  // FIELD_UPDATE_HPP
