#include "../include/types.hpp"

#include "../include/string_arena.hpp"

namespace tundradb {

// Implementation of Value::as_string()
// This needs the full StringRef definition from string_arena.hpp
std::string Value::as_string() const {
  if (is_string_type(type_)) {
    if (std::holds_alternative<StringRef>(data_)) {
      return get<StringRef>().to_string();
    } else if (std::holds_alternative<std::string>(data_)) {
      return get<std::string>();
    }
  }
  return "";  // fallback for non-string types
}

// Implementation of ValueRef::as_string()
std::string ValueRef::as_string() const {
  if (is_string_type(type)) {
    const StringRef& str_ref = as_string_ref();
    return str_ref.to_string();
  }
  return "";
}

}  // namespace tundradb
