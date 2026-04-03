#pragma once

#include <arrow/api.h>

#include <cstdint>
#include <memory>

namespace tundradb {

/**
 * MAP value union type codes used by `map<utf8, dense_union<...>>`.
 *
 * These codes are persisted in Arrow dense-union arrays and must stay stable
 * across writers/readers. Changing a code requires an explicit data migration.
 */
inline constexpr int8_t kMapUnionInt32 = 0;
inline constexpr int8_t kMapUnionInt64 = 1;
inline constexpr int8_t kMapUnionFloat = 2;
inline constexpr int8_t kMapUnionDouble = 3;
inline constexpr int8_t kMapUnionBool = 4;
inline constexpr int8_t kMapUnionString = 5;

/**
 * Returns the canonical Arrow dense-union type for MAP values.
 *
 * The union members and their type codes are:
 * - 0: `i32`  -> `int32`
 * - 1: `i64`  -> `int64`
 * - 2: `f32`  -> `float32`
 * - 3: `f64`  -> `float64`
 * - 4: `bool` -> `bool`
 * - 5: `str`  -> `utf8`
 */
inline std::shared_ptr<arrow::DataType> map_union_value_type() {
  return arrow::dense_union(
      {arrow::field("i32", arrow::int32()), arrow::field("i64", arrow::int64()),
       arrow::field("f32", arrow::float32()),
       arrow::field("f64", arrow::float64()),
       arrow::field("bool", arrow::boolean()),
       arrow::field("str", arrow::utf8())},
      {kMapUnionInt32, kMapUnionInt64, kMapUnionFloat, kMapUnionDouble,
       kMapUnionBool, kMapUnionString});
}

}  // namespace tundradb
