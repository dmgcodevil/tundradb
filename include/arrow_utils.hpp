#ifndef ARROW_UTILS_HPP
#define ARROW_UTILS_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "types.hpp"

namespace tundradb {

// Forward declarations
class Schema;
class Node;
class WhereExpr;
class TemporalContext;

/**
 * @brief Extracts the set of "id" column values from an Arrow table.
 *
 * @param table A table containing an Int64 "id" column.
 * @return A DenseSet of all row IDs, or an error if the column is missing.
 */
arrow::Result<llvm::DenseSet<int64_t>> get_ids_from_table(
    const std::shared_ptr<arrow::Table>& table);

/**
 * @brief Initialises the Arrow Compute function registry.
 *
 * Must be called once before any Arrow compute operations are used.
 *
 * @return True on success.
 */
bool initialize_arrow_compute();

/**
 * @brief Creates a single-element Int64 Arrow array.
 *
 * @param value The integer value.
 * @return A one-element array.
 */
arrow::Result<std::shared_ptr<arrow::Array>> create_int64_array(int64_t value);

/**
 * @brief Creates a single-element String Arrow array.
 *
 * @param value The string value.
 * @return A one-element array.
 */
arrow::Result<std::shared_ptr<arrow::Array>> create_str_array(
    const std::string& value);

/**
 * @brief Creates a single-element null Arrow array of the given type.
 *
 * @param type The Arrow data type.
 * @return A one-element array containing a null value.
 */
arrow::Result<std::shared_ptr<arrow::Array>> create_null_array(
    const std::shared_ptr<arrow::DataType>& type);

/**
 * @brief Prepends a field to an Arrow schema.
 *
 * @param field The field to insert at position 0.
 * @param target_schema The existing schema.
 * @return A new schema with @p field followed by @p target_schema fields.
 */
std::shared_ptr<arrow::Schema> prepend_field(
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Schema>& target_schema);

/** @deprecated Scheduled for removal. Use prepend_field() directly. */
std::shared_ptr<arrow::Schema> prepend_id_field(
    const std::shared_ptr<arrow::Schema>& target_schema);

/**
 * @brief Converts a Value to an Arrow Scalar.
 *
 * @param value The database value to convert.
 * @return The corresponding Arrow scalar, or an error for unsupported types.
 */
arrow::Result<std::shared_ptr<arrow::Scalar>> value_to_arrow_scalar(
    const Value& value);

/**
 * @brief Converts a raw pointer + type tag to an Arrow Scalar.
 *
 * @param ptr Pointer to the raw value bytes.
 * @param type The value's type tag.
 * @return The corresponding Arrow scalar, or an error for unsupported types.
 */
arrow::Result<std::shared_ptr<arrow::Scalar>> value_ptr_to_arrow_scalar(
    const char* ptr, ValueType type);

/**
 * @brief Translates a WhereExpr into an Arrow compute Expression.
 *
 * @param condition The WHERE expression tree.
 * @param strip_var If true, variable prefixes (e.g. "u.") are removed from
 * field names.
 * @return An Arrow Expression suitable for compute::Filter.
 */
arrow::compute::Expression where_condition_to_expression(
    const WhereExpr& condition, bool strip_var);

/**
 * @brief Materialises a set of nodes into an Arrow table.
 *
 * Creates a table with a single "id" column followed by the schema's fields.
 *
 * @param schema The database schema for the nodes.
 * @param nodes The nodes to serialise.
 * @return The resulting table.
 */
arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_nodes(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes);

/**
 * @brief Creates an empty Arrow table matching the given schema.
 *
 * @param schema The Arrow schema.
 * @return A zero-row table with the correct columns and types.
 */
arrow::Result<std::shared_ptr<arrow::Table>> create_empty_table(
    const std::shared_ptr<arrow::Schema>& schema);

/**
 * @brief Appends an ArrayRef's contents to an Arrow ListBuilder.
 *
 * Handles null arrays (appends null), empty arrays (appends empty list),
 * and populated arrays (appends each element based on elem_type).
 *
 * @param arr_ref The array reference to append.
 * @param list_builder The Arrow ListBuilder to append into.
 * @return OK on success, or an error if an element type is unsupported.
 */
arrow::Status append_array_to_list_builder(const ArrayRef& arr_ref,
                                           arrow::ListBuilder* list_builder);

/**
 * @brief Appends a MapRef's contents to an Arrow MapBuilder.
 *
 * MAP values are represented as map<utf8, dense_union<...>> to preserve
 * typed values in Arrow.
 *
 * @param map_ref The map reference to append.
 * @param map_builder The Arrow MapBuilder to append into.
 * @return OK on success, or an error for unsupported/corrupt value types.
 */
arrow::Status append_map_to_map_builder(const MapRef& map_ref,
                                        arrow::MapBuilder* map_builder);

/**
 * @brief Converts a MAP dense-union item at @p idx into Value.
 *
 * Expects MAP item storage shaped like `map<utf8, dense_union<...>>`.
 * Returns `std::nullopt` for null union slot or null child value.
 */
arrow::Result<std::optional<Value>> map_item_to_value(
    const std::shared_ptr<arrow::Array>& items, int64_t idx);

/**
 * @brief Converts one array/list element at @p idx into Value.
 *
 * Returns `Value{}` for null element slots.
 */
arrow::Result<Value> array_element_to_value(
    const std::shared_ptr<arrow::Array>& values, int64_t idx);

/**
 * @brief Filters an Arrow table using a WhereExpr predicate.
 *
 * @param table The table to filter.
 * @param condition The WHERE expression.
 * @param strip_var If true, variable prefixes are stripped before matching.
 * @return The filtered table, or an error on compute failure.
 */
arrow::Result<std::shared_ptr<arrow::Table>> filter(
    const std::shared_ptr<arrow::Table>& table, const WhereExpr& condition,
    bool strip_var);

}  // namespace tundradb

#endif  // ARROW_UTILS_HPP