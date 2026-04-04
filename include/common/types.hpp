#ifndef TYPES_HPP
#define TYPES_HPP

#include <array>
#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <variant>
#include <vector>

// Arrow includes for type conversion functions
#include <arrow/api.h>

#include "memory/array_ref.hpp"
#include "memory/map_ref.hpp"
#include "memory/string_arena.hpp"

namespace tundradb {

class Value {
 public:
  Value() : type_(ValueType::NA), data_(std::monostate{}) {}
  explicit Value(double v) : type_(ValueType::DOUBLE), data_(v) {}
  explicit Value(std::string v)
      : type_(ValueType::STRING), data_(std::move(v)) {}
  explicit Value(StringRef v)
      : type_(ValueType::STRING),
        data_(v) {}  // Store as StringRef for all string types

  // Constructor for creating StringRef value with specific string type
  Value(StringRef v, const ValueType string_type)
      : type_(string_type), data_(v) {
    assert(is_string_type(string_type));
  }

  // Arena-backed array (already allocated in ArrayArena)
  explicit Value(ArrayRef v) : type_(ValueType::ARRAY), data_(std::move(v)) {}

  // Arena-backed map (already allocated in MapArena)
  explicit Value(MapRef v) : type_(ValueType::MAP), data_(std::move(v)) {}

  // Raw map data - will be converted to MapRef by NodeArena
  explicit Value(std::map<std::string, Value> v)
      : type_(ValueType::MAP), data_(std::move(v)) {}

  // Raw array data - will be converted to ArrayRef by NodeArena
  // (same pattern as std::string -> StringRef for strings)
  explicit Value(std::vector<Value> v)
      : type_(ValueType::ARRAY), data_(std::move(v)) {}

  explicit Value(bool v) : type_(ValueType::BOOL), data_(v) {}
  Value(int32_t i) : type_(ValueType::INT32), data_(i) {}  // Non-explicit
  Value(int64_t v) : type_(ValueType::INT64), data_(v) {}  // Non-explicit
  explicit Value(float f) : type_(ValueType::FLOAT), data_(f) {}
  Value(const char* s) : type_(ValueType::STRING), data_(std::string(s)) {}

  ValueType type() const { return type_; }

  template <typename T>
  const T& get() const {
    return std::get<T>(data_);
  }

  [[nodiscard]] int32_t as_int32() const { return get<int32_t>(); }
  [[nodiscard]] int64_t as_int64() const { return get<int64_t>(); }
  [[nodiscard]] double as_float() const { return get<float>(); }
  [[nodiscard]] double as_double() const { return get<double>(); }
  [[nodiscard]] std::string as_string() const;  // Implemented in source file
  [[nodiscard]] const StringRef& as_string_ref() const {
    return get<StringRef>();
  }
  [[nodiscard]] const ArrayRef& as_array_ref() const { return get<ArrayRef>(); }
  [[nodiscard]] const MapRef& as_map_ref() const { return get<MapRef>(); }
  [[nodiscard]] bool as_bool() const { return get<bool>(); }
  [[nodiscard]] bool is_null() const { return type_ == ValueType::NA; }

  // Check if the Value contains a StringRef (vs std::string)
  [[nodiscard]] bool holds_string_ref() const {
    return is_string_type(type_) && std::holds_alternative<StringRef>(data_);
  }

  // Check if the Value contains a std::string
  [[nodiscard]] bool holds_std_string() const {
    return is_string_type(type_) && std::holds_alternative<std::string>(data_);
  }

  // Check if the Value contains an ArrayRef (arena-backed)
  [[nodiscard]] bool holds_array_ref() const {
    return type_ == ValueType::ARRAY && std::holds_alternative<ArrayRef>(data_);
  }

  // Check if the Value contains a MapRef (arena-backed)
  [[nodiscard]] bool holds_map_ref() const {
    return type_ == ValueType::MAP && std::holds_alternative<MapRef>(data_);
  }

  // Check if the Value contains a raw map (std::map<std::string, Value>)
  [[nodiscard]] bool holds_raw_map() const {
    return type_ == ValueType::MAP &&
           std::holds_alternative<std::map<std::string, Value>>(data_);
  }

  // Check if the Value contains a raw array (std::vector<Value>)
  [[nodiscard]] bool holds_raw_array() const {
    return type_ == ValueType::ARRAY &&
           std::holds_alternative<std::vector<Value>>(data_);
  }

  [[nodiscard]] const std::vector<Value>& as_raw_array() const {
    return get<std::vector<Value>>();
  }

  std::vector<Value>& as_raw_array_mut() {
    return std::get<std::vector<Value>>(data_);
  }

  [[nodiscard]] const std::map<std::string, Value>& as_raw_map() const {
    return get<std::map<std::string, Value>>();
  }

  std::map<std::string, Value>& as_raw_map_mut() {
    return std::get<std::map<std::string, Value>>(data_);
  }

  arrow::Status append_element(Value element) {
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

  /** Concatenate: [1,2] + [3,4] -> [1,2,3,4]. */
  arrow::Status append_all(Value array_value) {
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

  // Convert the Value to its raw string representation (without quotes for
  // strings)
  [[nodiscard]] std::string to_string() const {
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
            auto elem = Value::read_value_from_memory(arr.element_ptr(i),
                                                      arr.elem_type());
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

  static Value read_value_from_memory(const char* ptr, const ValueType type) {
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
        // All string types stored as StringRef, but preserve the field's
        // declared type
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

  // Equality operator
  bool operator==(const Value& other) const {
    if (type_ != other.type_) {
      return false;
    }
    return data_ == other.data_;
  }

  bool operator!=(const Value& other) const { return !(*this == other); }

 private:
  ValueType type_;
  std::variant<std::monostate, int32_t, int64_t, float, double, std::string,
               StringRef, ArrayRef, MapRef, std::vector<Value>,
               std::map<std::string, Value>, bool>
      data_;
};

struct ValueRef {
  const char* data;
  ValueType type;

  ValueRef() : data(nullptr), type(ValueType::NA) {}

  explicit ValueRef(ValueType type) : data(nullptr), type(type) {}

  ValueRef(const char* ptr, ValueType type) : data(ptr), type(type) {}

  [[nodiscard]] int32_t as_int32() const {
    return *reinterpret_cast<const int32_t*>(data);
  }

  [[nodiscard]] int64_t as_int64() const {
    return *reinterpret_cast<const int64_t*>(data);
  }

  [[nodiscard]] double as_double() const {
    return *reinterpret_cast<const double*>(data);
  }

  [[nodiscard]] float as_float() const {
    return *reinterpret_cast<const float*>(data);
  }

  [[nodiscard]] bool as_bool() const {
    return *reinterpret_cast<const bool*>(data);
  }

  [[nodiscard]] std::string as_string() const;  // Implemented in source file

  [[nodiscard]] const StringRef& as_string_ref() const {
    return *reinterpret_cast<const StringRef*>(data);
  }

  [[nodiscard]] const ArrayRef& as_array_ref() const {
    return *reinterpret_cast<const ArrayRef*>(data);
  }

  [[nodiscard]] const MapRef& as_map_ref() const {
    return *reinterpret_cast<const MapRef*>(data);
  }

  arrow::Result<std::shared_ptr<arrow::Scalar>> as_scalar() const {
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

  bool operator==(const ValueRef& other) const {
    if (type != other.type) {
      std::cout << "different types. this: " << to_string(type)
                << ", other: " << to_string(other.type) << std::endl;
      return false;
    }

    // Both null
    if (data == nullptr && other.data == nullptr) {
      return true;
    }

    // One null, one not null
    if (data == nullptr || other.data == nullptr) {
      return false;
    }

    // Compare values based on type
    switch (type) {
      case ValueType::NA:
        return true;  // Both are NA

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
        return false;  // Unknown type
    }
  }

  bool operator!=(const ValueRef& other) const { return !(*this == other); }

  [[nodiscard]] bool equals(const ValueRef& other) const {
    return *this == other;
  }

  // todo rename
  std::string ToString() const {
    if (data == nullptr) {
      return "NULL";
    }

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
        if (str_ref.is_null()) {
          return "NULL";
        }
        // Use StringRef's to_string() method
        return "\"" + str_ref.to_string() + "\"";
      }
      case ValueType::ARRAY: {
        const ArrayRef& arr = as_array_ref();
        if (arr.is_null()) return "NULL";
        std::string result = "[";
        for (uint32_t i = 0; i < arr.length(); ++i) {
          if (i > 0) result += ", ";
          auto elem = Value::read_value_from_memory(arr.element_ptr(i),
                                                    arr.elem_type());
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
};

// Stream operator for ValueType
inline std::ostream& operator<<(std::ostream& os, const ValueType type) {
  return os << to_string(type);
}

// Stream operator for Value
inline std::ostream& operator<<(std::ostream& os, const Value& value) {
  return os << value.to_string();
}

}  // namespace tundradb

#endif  // TYPES_HPP
