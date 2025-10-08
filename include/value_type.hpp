#ifndef VALUE_TYPE_HPP
#define VALUE_TYPE_HPP

#include <cstddef>
#include <string>

namespace tundradb {

/**
 * Enumeration of all supported value types in TundraDB.
 * 
 * This enum is the foundation of TundraDB's type system and is used throughout
 * the codebase for type checking, schema validation, and memory layout.
 */
enum class ValueType {
  NA,              // Null/undefined value
  INT32,           // 32-bit signed integer
  INT64,           // 64-bit signed integer
  FLOAT,           // 32-bit floating point
  DOUBLE,          // 64-bit floating point
  STRING,          // Variable length string (uses StringArena pool 3)
  FIXED_STRING16,  // Fixed-size string up to 16 bytes (uses StringArena pool 0)
  FIXED_STRING32,  // Fixed-size string up to 32 bytes (uses StringArena pool 1)
  FIXED_STRING64,  // Fixed-size string up to 64 bytes (uses StringArena pool 2)
  BOOL             // Boolean value
};

/**
 * Check if a ValueType represents a string type.
 * All string types (fixed and variable length) are stored using StringRef.
 */
inline bool is_string_type(const ValueType type) {
  return type == ValueType::STRING || 
         type == ValueType::FIXED_STRING16 ||
         type == ValueType::FIXED_STRING32 || 
         type == ValueType::FIXED_STRING64;
}

/**
 * Get the maximum size for fixed-size string types.
 * Returns SIZE_MAX for variable-length strings.
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
 * Convert ValueType to human-readable string.
 */
inline std::string to_string(const ValueType type) {
  switch (type) {
    case ValueType::NA:
      return "Null";
    case ValueType::INT32:
      return "Int32";
    case ValueType::INT64:
      return "Int64";
    case ValueType::FLOAT:
      return "Float";
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

/**
 * Get the storage size in bytes for a given ValueType.
 * For string types, this returns sizeof(StringRef) as strings are stored by reference.
 */
inline size_t get_type_size(const ValueType type) {
  // String types are stored as StringRef (16 bytes)
  if (is_string_type(type)) {
    return 16;  // sizeof(StringRef) = 16 bytes
  }
  
  switch (type) {
    case ValueType::INT64:
      return 8;
    case ValueType::INT32:
      return 4;
    case ValueType::DOUBLE:
      return 8;
    case ValueType::FLOAT:
      return 4;
    case ValueType::BOOL:
      return 1;
    default:
      return 0;
  }
}

/**
 * Get the memory alignment requirement for a given ValueType.
 */
inline size_t get_type_alignment(const ValueType type) {
  // String types are stored as StringRef (8-byte pointer alignment)
  if (is_string_type(type)) {
    return 8;  // alignof(StringRef) = 8 bytes (pointer alignment)
  }
  
  switch (type) {
    case ValueType::INT64:
      return 8;
    case ValueType::INT32:
      return 4;
    case ValueType::DOUBLE:
      return 8;
    case ValueType::FLOAT:
      return 4;
    case ValueType::BOOL:
      return 1;
    default:
      return 1;
  }
}

}  // namespace tundradb

#endif  // VALUE_TYPE_HPP

