#include "table_info.hpp"

#include <algorithm>

namespace tundradb {

TableInfo::TableInfo(const std::shared_ptr<arrow::Table>& table)
    : num_rows_(table->num_rows()) {
  chunk_boundaries_.reserve(table->num_columns());

  for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
    const auto column = table->column(col_idx);
    std::vector<int64_t> boundaries;
    boundaries.reserve(column->num_chunks() + 1);

    int64_t current_boundary = 0;
    boundaries.push_back(current_boundary);

    for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
      current_boundary += column->chunk(chunk_idx)->length();
      boundaries.push_back(current_boundary);
    }

    chunk_boundaries_.push_back(std::move(boundaries));
  }
}

TableInfo::ChunkInfo TableInfo::get_chunk_info(const int column_index,
                                               const int64_t row_index) const {
  if (column_index < 0 ||
      column_index >= static_cast<int>(chunk_boundaries_.size())) {
    throw std::out_of_range("Column index out of range");
  }

  if (row_index < 0 || row_index >= num_rows_) {
    throw std::out_of_range("Row index out of range");
  }

  const auto& boundaries = chunk_boundaries_[column_index];
  const auto it = std::ranges::upper_bound(boundaries, row_index);
  const int chunk_index = std::distance(boundaries.begin(), it) - 1;

  return ChunkInfo{chunk_index, row_index - boundaries[chunk_index]};
}

}  // namespace tundradb