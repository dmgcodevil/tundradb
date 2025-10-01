#include "query.hpp"

namespace tundradb {

// FieldRef implementation
FieldRef FieldRef::from_string(const std::string& field_str) {
  const size_t dot_pos = field_str.find('.');
  if (dot_pos != std::string::npos) {
    std::string variable = field_str.substr(0, dot_pos);
    std::string field_name = field_str.substr(dot_pos + 1);

    // Return unresolved FieldRef - will be resolved later in query processing
    return {variable, field_name};
  } else {
    // No variable prefix, treat entire string as field name
    return {"", field_str};
  }
}

}  // namespace tundradb