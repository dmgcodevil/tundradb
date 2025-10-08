#ifndef TYPES_HPP
#define TYPES_HPP

#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <variant>

// Arrow includes for type conversion functions
#include <arrow/api.h>
#include <arrow/type.h>

#include "string_arena.hpp"

namespace tundradb {

class Value {
 public:
  Value() : type_(ValueType::NA), data_(std::monostate{}) {}
  // explicit Value(int32_t i) : type_(ValueType::Int32), data_(i) {}
  // explicit Value(int64_t v) : type_(ValueType::Int64), data_(v) {}
  explicit Value(double v) : type_(ValueType::DOUBLE), data_(v) {}
  explicit Value(std::string v)
      : type_(ValueType::STRING), data_(std::move(v)) {}
  explicit Value(StringRef v)
      : type_(ValueType::STRING),
        data_(v) {}  // Store as StringRef for all string types

  // Constructor for creating StringRef value with specific string type
  Value(StringRef v, const ValueType string_type)
      : type_(string_type), data_(v) {
    // Ensure it's actually a string type
    assert(is_string_type(string_type));
  }
  explicit Value(bool v) : type_(ValueType::BOOL), data_(v) {}
  // Value(int i) : type_(ValueType::Int32), data_(i) {}
  Value(int32_t i) : type_(ValueType::INT32), data_(i) {}  // Non-explicit
  Value(int64_t v) : type_(ValueType::INT64), data_(v) {}  // Non-explicit
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
      case ValueType::BOOL:
        return Value{*reinterpret_cast<const bool*>(ptr)};
      case ValueType::STRING:
      case ValueType::FIXED_STRING16:
      case ValueType::FIXED_STRING32:
      case ValueType::FIXED_STRING64:
        // All string types stored as StringRef, but preserve the field's
        // declared type
        return Value{*reinterpret_cast<const StringRef*>(ptr), type};
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
               StringRef, bool>
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

        // Use StringRef's operator== which handles all edge cases
        return str1 == str2;
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

static constexpr ValueType arrow_type_to_value_type(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  switch (arrow_type->id()) {
    case arrow::Type::INT32:
    case arrow::Type::INT16:
    case arrow::Type::INT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT8:
      return ValueType::INT32;

    case arrow::Type::INT64:
    case arrow::Type::UINT64:
    case arrow::Type::UINT32:  // Could overflow int32, safer as int64
      return ValueType::INT64;

    case arrow::Type::FLOAT:
      return ValueType::FLOAT;
    case arrow::Type::DOUBLE:
      return ValueType::DOUBLE;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return ValueType::STRING;
    case arrow::Type::BOOL:
      return ValueType::BOOL;
    case arrow::Type::NA:
      return ValueType::NA;
    default:
      return ValueType::NA;
  }
}

static std::shared_ptr<arrow::DataType> value_type_to_arrow_type(
    const ValueType type) {
  switch (type) {
    case ValueType::NA:
      return arrow::null();
    case ValueType::INT32:
      return arrow::int32();
    case ValueType::INT64:
      return arrow::int64();
    case ValueType::FLOAT:
      return arrow::float32();
    case ValueType::DOUBLE:
      return arrow::float64();
    case ValueType::STRING:
      return arrow::utf8();
    case ValueType::BOOL:
      return arrow::boolean();
    default:
      return arrow::utf8();  // Default fallback
  }
}

}  // namespace tundradb

#endif  // TYPES_HPP
