#include "edge.hpp"

namespace tundradb {
arrow::Result<std::shared_ptr<arrow::Array>> Edge::get_property(
    const std::string& name) const {
  if (this->properties.contains(name)) {
    return this->properties.at(name);
  }
  return arrow::Status::KeyError("Property " + name + " not found");
}

}  // namespace tundradb