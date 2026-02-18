#ifndef ARROW_UTILS_HPP
#define ARROW_UTILS_HPP

#include <arrow/api.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>

namespace tundradb {

arrow::Result<llvm::DenseSet<int64_t>> get_ids_from_table(
    const std::shared_ptr<arrow::Table>& table);

// Initialize Arrow Compute module - should be called once at startup
bool initialize_arrow_compute();

// Arrow utility functions
arrow::Result<std::shared_ptr<arrow::Array>> create_int64_array(
    const int64_t value);
arrow::Result<std::shared_ptr<arrow::Array>> create_str_array(
    const std::string& value);
arrow::Result<std::shared_ptr<arrow::Array>> create_null_array(
    const std::shared_ptr<arrow::DataType>& type);
std::shared_ptr<arrow::Schema> prepend_field(
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Schema>& target_schema);

/**
 * Creates a new schema by prepending an "id" field of type Int64 to the target
 * schema.
 * @deprecated for remove
 * @param target_schema The original schema to modify
 * @return A new schema with "id" field prepended
 */
std::shared_ptr<arrow::Schema> prepend_id_field(
    const std::shared_ptr<arrow::Schema>& target_schema);

}  // namespace tundradb

#endif  // ARROW_UTILS_HPP