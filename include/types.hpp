#ifndef TYPES_HPP
#define TYPES_HPP

#include <array>
#include <cassert>
#include <cstring>
#include <string>
#include <variant>

// Arrow includes for type conversion functions
#include <arrow/api.h>
#include <arrow/type.h>

namespace tundradb {

/**
 * String reference that points into a string arena
 * Lightweight reference instead of inline storage
 */
struct StringRef {
  const char* data;   // Direct pointer to string data in arena
  uint32_t length;    // Length of the string
  uint32_t arena_id;  // Which string arena contains this string

  StringRef() : data(nullptr), length(0), arena_id(0) {}
  StringRef(const char* ptr, const uint32_t len, const uint32_t id = 0)
      : data(ptr), length(len), arena_id(id) {}

  bool is_null() const { return length == 0 || data == nullptr; }

  std::string to_string() const {
    return data ? std::string(data, length) : std::string();
  }

  bool operator==(const StringRef& other) const {
    return data == other.data && length == other.length &&
           arena_id == other.arena_id;
  }

  bool operator!=(const StringRef& other) const { return !(*this == other); }
};

enum class ValueType {
  NA,
  INT32,
  INT64,
  FLOAT,
  DOUBLE,
  STRING,          // Variable length (uses StringArena)
  FIXED_STRING16,  // 16 char max (uses StringArena)
  FIXED_STRING32,  // 32 char max (uses StringArena)
  FIXED_STRING64,  // 64 char max (uses StringArena)
  BOOL
};

/**
 * Get the maximum size for fixed-size string types
 */
inline size_t get_string_max_size(const ValueType type) {
  switch (type) {
    case ValueType::FIXED_STRING16:
      return 16;
    case ValueType::FIXED_STRING32:
      return 32;
    case ValueType::FIXED_STRING64:
      return 64;
    case ValueType::STRING:
      return SIZE_MAX;  // No limit for variable strings
    default:
      return 0;  // Not a string type
  }
}

/**
 * Check if a ValueType is a string type
 */
inline bool is_string_type(const ValueType type) {
  return type == ValueType::STRING || type == ValueType::FIXED_STRING16 ||
         type == ValueType::FIXED_STRING32 || type == ValueType::FIXED_STRING64;
}

static size_t get_type_size(const ValueType type) {
  if (is_string_type(type)) {
    // ALL string types stored as StringRef in node layout
    return sizeof(StringRef);  // 12 bytes on 64-bit systems
  }
  switch (type) {
    case ValueType::INT64:
      return 8;
    case ValueType::INT32:
      return 4;
    case ValueType::DOUBLE:
      return 8;
    case ValueType::BOOL:
      return 1;
    default:
      return 0;
  }
}

static size_t get_type_alignment(const ValueType type) {
  if (is_string_type(type)) {
    // ALL string types stored as StringRef in node layout
    return alignof(StringRef);  // Usually 8 bytes (pointer alignment)
  }
  switch (type) {
    case ValueType::INT64:
      return 8;
    case ValueType::INT32:
      return 4;
    case ValueType::DOUBLE:
      return 8;
    case ValueType::BOOL:
      return 1;
    default:
      return 1;
  }
}

inline std::string to_string(const ValueType type) {
  switch (type) {
    case ValueType::NA:
      return "Null";
    case ValueType::INT32:
      return "Int32";
    case ValueType::INT64:
      return "Int64";
    case ValueType::DOUBLE:
      return "Double";
    case ValueType::STRING:
      return "String";
    case ValueType::FIXED_STRING16:
      return "FixedString16";
    case ValueType::FIXED_STRING32:
      return "FixedString32";
    case ValueType::FIXED_STRING64:
      return "FixedString64";
    case ValueType::BOOL:
      return "Bool";
    default:
      return "Unknown";
  }
}

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
  [[nodiscard]] std::string as_string() const {
    if (is_string_type(type_)) {
      if (std::holds_alternative<StringRef>(data_)) {
        return get<StringRef>().to_string();
      } else if (std::holds_alternative<std::string>(data_)) {
        return get<std::string>();
      }
    }
    return "";  // fallback for non-string types
  }
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
