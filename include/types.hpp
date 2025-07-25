#ifndef TYPES_HPP
#define TYPES_HPP

#include <string>
#include <variant>

namespace tundradb {

enum class ValueType { Null, Int32, Int64, Float, Double, String, Bool };

inline std::string to_string(ValueType type) {
  switch (type) {
    case ValueType::Null:
      return "Null";
    case ValueType::Int32:
      return "Int32";
    case ValueType::Int64:
      return "Int64";
    case ValueType::Double:
      return "Double";
    case ValueType::String:
      return "String";
    case ValueType::Bool:
      return "Bool";
    default:
      return "Unknown";
  }
}

class Value {
 public:
  Value() : type_(ValueType::Null), data_(std::monostate{}) {}
  // explicit Value(int32_t i) : type_(ValueType::Int32), data_(i) {}
  // explicit Value(int64_t v) : type_(ValueType::Int64), data_(v) {}
  explicit Value(double v) : type_(ValueType::Double), data_(v) {}
  explicit Value(std::string v)
      : type_(ValueType::String), data_(std::move(v)) {}
  explicit Value(bool v) : type_(ValueType::Bool), data_(v) {}
  // Value(int i) : type_(ValueType::Int32), data_(i) {}
  Value(int32_t i) : type_(ValueType::Int32), data_(i) {}  // Non-explicit
  Value(int64_t v) : type_(ValueType::Int64), data_(v) {}  // Non-explicit
  Value(const char* s) : type_(ValueType::String), data_(std::string(s)) {}

  ValueType type() const { return type_; }

  template <typename T>
  const T& get() const {
    return std::get<T>(data_);
  }

  [[nodiscard]] int32_t as_int32() const { return get<int32_t>(); }
  [[nodiscard]] int64_t as_int64() const { return get<int64_t>(); }
  [[nodiscard]] double as_float() const { return get<float>(); }
  [[nodiscard]] double as_double() const { return get<double>(); }
  [[nodiscard]] const std::string& as_string() const {
    return get<std::string>();
  }
  [[nodiscard]] bool as_bool() const { return get<bool>(); }
  [[nodiscard]] bool is_null() const { return type_ == ValueType::Null; }

  // Convert the Value to its raw string representation (without quotes for
  // strings)
  [[nodiscard]] std::string to_string() const {
    switch (type_) {
      case ValueType::Null:
        return "";
      case ValueType::Int32:
        return std::to_string(as_int32());
      case ValueType::Int64:
        return std::to_string(as_int64());
      case ValueType::Double:
        return std::to_string(as_double());
      case ValueType::String:
        return as_string();  // No quotes
      case ValueType::Bool:
        return as_bool() ? "true" : "false";
      default:
        return "";
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
               bool>
      data_;
};

// Stream operator for ValueType
inline std::ostream& operator<<(std::ostream& os, ValueType type) {
  return os << to_string(type);
}

// Stream operator for Value
inline std::ostream& operator<<(std::ostream& os, const Value& value) {
  return os << value.to_string();
}

static ValueType arrow_type_to_value_type(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  switch (arrow_type->id()) {
    case arrow::Type::INT32:
    case arrow::Type::INT16:
    case arrow::Type::INT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT8:
      return ValueType::Int32;

    case arrow::Type::INT64:
    case arrow::Type::UINT64:
    case arrow::Type::UINT32:  // Could overflow int32, safer as int64
      return ValueType::Int64;

    case arrow::Type::FLOAT:
      return ValueType::Float;
    case arrow::Type::DOUBLE:
      return ValueType::Double;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return ValueType::String;
    case arrow::Type::BOOL:
      return ValueType::Bool;
    case arrow::Type::NA:
      return ValueType::Null;
    default:
      // For unsupported types, default to String representation
      return ValueType::String;
  }
}

static std::shared_ptr<arrow::DataType> value_type_to_arrow_type(
    ValueType type) {
  switch (type) {
    case ValueType::Null:
      return arrow::null();
    case ValueType::Int32:
      return arrow::int32();
    case ValueType::Int64:
      return arrow::int64();
    case ValueType::Float:
      return arrow::float32();
    case ValueType::Double:
      return arrow::float64();
    case ValueType::String:
      return arrow::utf8();
    case ValueType::Bool:
      return arrow::boolean();
    default:
      return arrow::utf8();  // Default fallback
  }
}

}  // namespace tundradb

#endif  // TYPES_HPP
