#ifndef TABLE_INFO_HPP
#define TABLE_INFO_HPP

#include <arrow/api.h>
#include <arrow/table.h>

#include <vector>

namespace tundradb {

/// Precomputed per-column chunk boundaries for mapping logical row indices to
/// Arrow chunks.
class TableInfo {
 public:
  /// Builds cumulative row boundaries for each column from the table’s chunked
  /// arrays.
  explicit TableInfo(const std::shared_ptr<arrow::Table>& table);

  /// Identifies which Arrow chunk holds a logical row and the offset inside
  /// that chunk.
  struct ChunkInfo {
    int chunk_index;
    int64_t offset_in_chunk;
  };
  /// Maps logical row_index in column column_index to a chunk and in-chunk
  /// offset; throws if out of range.
  [[nodiscard]] ChunkInfo get_chunk_info(int column_index,
                                         int64_t row_index) const;

  /// Number of columns (same as the source table).
  int num_columns() const { return chunk_boundaries_.size(); }

  /// Total logical row count (same as table->num_rows() at construction).
  [[nodiscard]] int64_t num_rows() const { return num_rows_; }

 private:
  std::vector<std::vector<int64_t>> chunk_boundaries_;
  int64_t num_rows_;
};

}  // namespace tundradb

#endif  // TABLE_INFO_HPP