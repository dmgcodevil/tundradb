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

/// A type-erased, variant-backed value that can hold any field type.
///
/// Supports scalar types (int32, int64, float, double, bool, string),
/// arena-backed containers (StringRef, ArrayRef, MapRef), and raw
/// containers (std::vector<Value>, std::map<std::string, Value>) that
/// are converted to arena refs on insertion into the arena.
class Value {
 public:
  /// Construct a NULL value (ValueType::NA).
  Value() : type_(ValueType::NA), data_(std::monostate{}) {}
  explicit Value(double v) : type_(ValueType::DOUBLE), data_(v) {}
  explicit Value(std::string v)
      : type_(ValueType::STRING), data_(std::move(v)) {}
  /// Construct from an arena-backed string reference.
  explicit Value(StringRef v) : type_(ValueType::STRING), data_(v) {}

  /// Construct from a StringRef with an explicit string subtype
  /// (e.g. FIXED_STRING16).
  Value(StringRef v, const ValueType string_type)
      : type_(string_type), data_(v) {
    assert(is_string_type(string_type));
  }

  /// Construct from an arena-backed array reference.
  explicit Value(ArrayRef v) : type_(ValueType::ARRAY), data_(std::move(v)) {}
  /// Construct from an arena-backed map reference.
  explicit Value(MapRef v) : type_(ValueType::MAP), data_(std::move(v)) {}
  /// Construct from raw map data (converted to MapRef on arena insertion).
  explicit Value(std::map<std::string, Value> v)
      : type_(ValueType::MAP), data_(std::move(v)) {}
  /// Construct from raw array data (converted to ArrayRef on arena insertion).
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

  /// True when the string variant holds a StringRef (arena-backed).
  [[nodiscard]] bool holds_string_ref() const {
    return is_string_type(type_) && std::holds_alternative<StringRef>(data_);
  }

  /// True when the string variant holds a std::string (heap-allocated).
  [[nodiscard]] bool holds_std_string() const {
    return is_string_type(type_) && std::holds_alternative<std::string>(data_);
  }

  /// True when the array variant holds an ArrayRef (arena-backed).
  [[nodiscard]] bool holds_array_ref() const {
    return type_ == ValueType::ARRAY && std::holds_alternative<ArrayRef>(data_);
  }

  /// True when the map variant holds a MapRef (arena-backed).
  [[nodiscard]] bool holds_map_ref() const {
    return type_ == ValueType::MAP && std::holds_alternative<MapRef>(data_);
  }

  /// True when the map variant holds a raw std::map (not yet in arena).
  [[nodiscard]] bool holds_raw_map() const {
    return type_ == ValueType::MAP &&
           std::holds_alternative<std::map<std::string, Value>>(data_);
  }

  /// True when the array variant holds a raw std::vector (not yet in arena).
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

  /// Append a single element (or splice another raw array) to this array.
  arrow::Status append_element(Value element);
  /// Concatenate: [1,2] + [3,4] -> [1,2,3,4].
  arrow::Status append_all(Value array_value);

  /// Convert the value to a human-readable string (no quotes around strings).
  [[nodiscard]] std::string to_string() const;

  /// Reinterpret a raw memory pointer as a Value of the given type.
  static Value read_value_from_memory(const char* ptr, ValueType type);

  bool operator==(const Value& other) const {
    if (type_ != other.type_) return false;
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

/// A lightweight, non-owning reference to a typed value in arena memory.
///
/// ValueRef avoids copying by pointing directly into the arena.  The
/// caller must ensure the referenced memory outlives the ValueRef.
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

  /// Convert the referenced value to an Arrow Scalar for compute kernels.
  arrow::Result<std::shared_ptr<arrow::Scalar>> as_scalar() const;

  bool operator==(const ValueRef& other) const;
  bool operator!=(const ValueRef& other) const { return !(*this == other); }
  [[nodiscard]] bool equals(const ValueRef& other) const {
    return *this == other;
  }

  /// Human-readable string representation (strings are quoted).
  std::string ToString() const;
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
