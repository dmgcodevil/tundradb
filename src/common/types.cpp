#include "common/types.hpp"

#include "memory/string_arena.hpp"

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

// ---------------------------------------------------------------------------
// Value — large methods moved from types.hpp
// ---------------------------------------------------------------------------

arrow::Status Value::append_element(Value element) {
  if (element.holds_raw_array()) {
    return append_all(std::move(element));
  }
  if (type_ == ValueType::NA) {
    type_ = ValueType::ARRAY;
    data_ = std::vector<Value>{std::move(element)};
    return arrow::Status::OK();
  }
  if (!holds_raw_array()) {
    return arrow::Status::TypeError(
        "APPEND: target value is not a raw array");
  }
  as_raw_array_mut().push_back(std::move(element));
  return arrow::Status::OK();
}

arrow::Status Value::append_all(Value array_value) {
  if (!array_value.holds_raw_array()) {
    return arrow::Status::TypeError(
        "APPEND_ALL: source value is not a raw array");
  }
  auto& src = array_value.as_raw_array_mut();
  if (type_ == ValueType::NA) {
    type_ = ValueType::ARRAY;
    data_ = std::move(src);
    return arrow::Status::OK();
  }
  if (!holds_raw_array()) {
    return arrow::Status::TypeError(
        "APPEND_ALL: target value is not a raw array");
  }
  auto& dest = as_raw_array_mut();
  dest.reserve(dest.size() + src.size());
  for (auto& v : src) {
    dest.push_back(std::move(v));
  }
  return arrow::Status::OK();
}

std::string Value::to_string() const {
  switch (type_) {
    case ValueType::NA:
      return "";
    case ValueType::INT32:
      return std::to_string(as_int32());
    case ValueType::INT64:
      return std::to_string(as_int64());
    case ValueType::DOUBLE:
      return std::to_string(as_double());
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
    case ValueType::STRING:
      return as_string();
    case ValueType::BOOL:
      return as_bool() ? "true" : "false";
    case ValueType::ARRAY: {
      if (holds_array_ref()) {
        const auto& arr = as_array_ref();
        std::string result = "[";
        for (uint32_t i = 0; i < arr.length(); ++i) {
          if (i > 0) result += ", ";
          auto elem =
              Value::read_value_from_memory(arr.element_ptr(i), arr.elem_type());
          result += elem.to_string();
        }
        result += "]";
        return result;
      }
      return "[]";
    }
    case ValueType::MAP: {
      if (holds_map_ref()) {
        const auto& m = as_map_ref();
        std::string result = "{";
        for (uint32_t i = 0; i < m.count(); ++i) {
          if (i > 0) result += ", ";
          const auto* e = m.entry_ptr(i);
          result += e->key.to_string();
          result += ": ";
          auto val = Value::read_value_from_memory(
              e->value, static_cast<ValueType>(e->value_type));
          result += val.to_string();
        }
        result += "}";
        return result;
      }
      if (holds_raw_map()) {
        std::string result = "{";
        bool first = true;
        for (const auto& [k, v] : as_raw_map()) {
          if (!first) result += ", ";
          first = false;
          result += k;
          result += ": ";
          result += v.to_string();
        }
        result += "}";
        return result;
      }
      return "{}";
    }
    default:
      return "";
  }
}

Value Value::read_value_from_memory(const char* ptr, const ValueType type) {
  if (ptr == nullptr) {
    return Value{};
  }
  switch (type) {
    case ValueType::INT64:
      return Value{*reinterpret_cast<const int64_t*>(ptr)};
    case ValueType::INT32:
      return Value{*reinterpret_cast<const int32_t*>(ptr)};
    case ValueType::DOUBLE:
      return Value{*reinterpret_cast<const double*>(ptr)};
    case ValueType::FLOAT:
      return Value{*reinterpret_cast<const float*>(ptr)};
    case ValueType::BOOL:
      return Value{*reinterpret_cast<const bool*>(ptr)};
    case ValueType::STRING:
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
      return Value{*reinterpret_cast<const StringRef*>(ptr), type};
    case ValueType::ARRAY:
      return Value{*reinterpret_cast<const ArrayRef*>(ptr)};
    case ValueType::MAP:
      return Value{*reinterpret_cast<const MapRef*>(ptr)};
    case ValueType::NA:
    default:
      return Value{};
  }
}

// ---------------------------------------------------------------------------
// ValueRef — large methods moved from types.hpp
// ---------------------------------------------------------------------------

arrow::Result<std::shared_ptr<arrow::Scalar>> ValueRef::as_scalar() const {
  switch (type) {
    case ValueType::INT32:
      return arrow::MakeScalar(as_int32());
    case ValueType::INT64:
      return arrow::MakeScalar(as_int64());
    case ValueType::DOUBLE:
      return arrow::MakeScalar(as_double());
    case ValueType::STRING:
      return arrow::MakeScalar(as_string_ref().to_string());
    case ValueType::BOOL:
      return arrow::MakeScalar(as_bool());
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    case ValueType::ARRAY:
      return arrow::Status::NotImplemented(
          "Array scalar conversion not yet implemented");
    case ValueType::MAP:
      return arrow::Status::NotImplemented(
          "Map scalar conversion not yet implemented");
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          to_string(type));
  }
}

bool ValueRef::operator==(const ValueRef& other) const {
  if (type != other.type) {
    std::cout << "different types. this: " << to_string(type)
              << ", other: " << to_string(other.type) << std::endl;
    return false;
  }
  if (data == nullptr && other.data == nullptr) return true;
  if (data == nullptr || other.data == nullptr) return false;

  switch (type) {
    case ValueType::NA:
      return true;
    case ValueType::INT32:
      return *reinterpret_cast<const int32_t*>(data) ==
             *reinterpret_cast<const int32_t*>(other.data);
    case ValueType::INT64:
      return *reinterpret_cast<const int64_t*>(data) ==
             *reinterpret_cast<const int64_t*>(other.data);
    case ValueType::FLOAT:
      return *reinterpret_cast<const float*>(data) ==
             *reinterpret_cast<const float*>(other.data);
    case ValueType::DOUBLE:
      return *reinterpret_cast<const double*>(data) ==
             *reinterpret_cast<const double*>(other.data);
    case ValueType::BOOL:
      return *reinterpret_cast<const bool*>(data) ==
             *reinterpret_cast<const bool*>(other.data);
    case ValueType::STRING: {
      const StringRef& str1 = *reinterpret_cast<const StringRef*>(data);
      const StringRef& str2 = *reinterpret_cast<const StringRef*>(other.data);
      return str1 == str2;
    }
    case ValueType::ARRAY: {
      const ArrayRef& arr1 = *reinterpret_cast<const ArrayRef*>(data);
      const ArrayRef& arr2 = *reinterpret_cast<const ArrayRef*>(other.data);
      return arr1 == arr2;
    }
    case ValueType::MAP: {
      const MapRef& m1 = *reinterpret_cast<const MapRef*>(data);
      const MapRef& m2 = *reinterpret_cast<const MapRef*>(other.data);
      return m1 == m2;
    }
    default:
      return false;
  }
}

std::string ValueRef::ToString() const {
  if (data == nullptr) return "NULL";

  switch (type) {
    case ValueType::NA:
      return "NULL";
    case ValueType::INT32:
      return std::to_string(as_int32());
    case ValueType::INT64:
      return std::to_string(as_int64());
    case ValueType::FLOAT:
      return std::to_string(as_float());
    case ValueType::DOUBLE:
      return std::to_string(as_double());
    case ValueType::BOOL:
      return as_bool() ? "true" : "false";
    case ValueType::FIXED_STRING16:
    case ValueType::FIXED_STRING32:
    case ValueType::FIXED_STRING64:
    case ValueType::STRING: {
      const StringRef& str_ref = as_string_ref();
      if (str_ref.is_null()) return "NULL";
      return "\"" + str_ref.to_string() + "\"";
    }
    case ValueType::ARRAY: {
      const ArrayRef& arr = as_array_ref();
      if (arr.is_null()) return "NULL";
      std::string result = "[";
      for (uint32_t i = 0; i < arr.length(); ++i) {
        if (i > 0) result += ", ";
        auto elem =
            Value::read_value_from_memory(arr.element_ptr(i), arr.elem_type());
        result += elem.to_string();
      }
      result += "]";
      return result;
    }
    case ValueType::MAP: {
      const MapRef& m = as_map_ref();
      if (m.is_null()) return "NULL";
      std::string result = "{";
      for (uint32_t i = 0; i < m.count(); ++i) {
        if (i > 0) result += ", ";
        const auto* e = m.entry_ptr(i);
        result += e->key.to_string();
        result += ": ";
        auto val = Value::read_value_from_memory(
            e->value, static_cast<ValueType>(e->value_type));
        result += val.to_string();
      }
      result += "}";
      return result;
    }
    default:
      return "UNKNOWN_TYPE";
  }
}

}  // namespace tundradb
