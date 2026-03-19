#ifndef TYPE_DESCRIPTOR_HPP
#define TYPE_DESCRIPTOR_HPP

#include <cstdint>
#include <string>

#include "value_type.hpp"

namespace tundradb {

/**
 * Describes a complete type, including parameterized types like arrays.
 *
 * For primitive types, only base_type is meaningful.
 * For ARRAY, element_type and fixed_size carry the array parameters.
 * For STRING, max_string_size optionally caps string length.
 *
 * Examples:
 *   TypeDescriptor::int32()                        -> INT32
 *   TypeDescriptor::string()                       -> STRING (variable)
 *   TypeDescriptor::string(64)                     -> STRING with max 64 bytes
 *   TypeDescriptor::array(ValueType::INT32)        -> ARRAY<INT32> (dynamic)
 *   TypeDescriptor::array(ValueType::INT32, 10)    -> ARRAY<INT32, 10> (fixed)
 *   TypeDescriptor::array(ValueType::STRING)       -> ARRAY<STRING>
 */
struct TypeDescriptor {
  ValueType base_type = ValueType::NA;
  ValueType element_type = ValueType::NA;  // for ARRAY: element type
  uint32_t fixed_size = 0;                 // for ARRAY: 0=dynamic, N=fixed cap
  uint32_t max_string_size = 0;            // for STRING: 0=unlimited

  // ========================================================================
  // Factory methods
  // ========================================================================

  static TypeDescriptor na() { return {ValueType::NA}; }
  static TypeDescriptor int32() { return {ValueType::INT32}; }
  static TypeDescriptor int64() { return {ValueType::INT64}; }
  static TypeDescriptor float32() { return {ValueType::FLOAT}; }
  static TypeDescriptor float64() { return {ValueType::DOUBLE}; }
  static TypeDescriptor boolean() { return {ValueType::BOOL}; }

  static TypeDescriptor string(uint32_t max_size = 0) {
    return {ValueType::STRING, ValueType::NA, 0, max_size};
  }

  static TypeDescriptor array(ValueType elem, uint32_t fixed = 0) {
    return {ValueType::ARRAY, elem, fixed, 0};
  }

  /**
   * Create a TypeDescriptor from a legacy ValueType.
   * Handles FIXED_STRING* by converting to STRING with max_string_size.
   */
  static TypeDescriptor from_value_type(ValueType vt) {
    switch (vt) {
      case ValueType::FIXED_STRING16:
        return string(16);
      case ValueType::FIXED_STRING32:
        return string(32);
      case ValueType::FIXED_STRING64:
        return string(64);
      default:
        return {vt};
    }
  }

  // ========================================================================
  // Quick checks (for hot paths - no virtual dispatch)
  // ========================================================================

  [[nodiscard]] bool is_primitive() const {
    return base_type != ValueType::NA && base_type != ValueType::ARRAY &&
           !is_string();
  }

  [[nodiscard]] bool is_string() const {
    return base_type == ValueType::STRING ||
           base_type == ValueType::FIXED_STRING16 ||
           base_type == ValueType::FIXED_STRING32 ||
           base_type == ValueType::FIXED_STRING64;
  }

  [[nodiscard]] bool is_array() const { return base_type == ValueType::ARRAY; }

  [[nodiscard]] bool is_null() const { return base_type == ValueType::NA; }

  [[nodiscard]] bool is_fixed_size_array() const {
    return is_array() && fixed_size > 0;
  }

  [[nodiscard]] bool is_dynamic_array() const {
    return is_array() && fixed_size == 0;
  }

  // ========================================================================
  // Size and alignment (delegates to ValueType for primitives/strings)
  // ========================================================================

  /** Storage size in the node slot (bytes). */
  [[nodiscard]] size_t storage_size() const { return get_type_size(base_type); }

  /** Required memory alignment. */
  [[nodiscard]] size_t storage_alignment() const {
    return get_type_alignment(base_type);
  }

  // ========================================================================
  // String representation
  // ========================================================================

  [[nodiscard]] std::string to_string() const {
    if (is_array()) {
      std::string result = "ARRAY<" + tundradb::to_string(element_type);
      if (fixed_size > 0) {
        result += ", " + std::to_string(fixed_size);
      }
      result += ">";
      return result;
    }
    if (is_string() && max_string_size > 0) {
      return "STRING(" + std::to_string(max_string_size) + ")";
    }
    return tundradb::to_string(base_type);
  }

  // ========================================================================
  // Comparison
  // ========================================================================

  bool operator==(const TypeDescriptor& other) const {
    return base_type == other.base_type && element_type == other.element_type &&
           fixed_size == other.fixed_size &&
           max_string_size == other.max_string_size;
  }

  bool operator!=(const TypeDescriptor& other) const {
    return !(*this == other);
  }
};

}  // namespace tundradb

#endif  // TYPE_DESCRIPTOR_HPP
