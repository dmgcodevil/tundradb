#ifndef TABLE_INFO_HPP
#define TABLE_INFO_HPP

#include <arrow/api.h>
#include <arrow/table.h>

#include <vector>

namespace tundradb {

class TableInfo {
 public:
  explicit TableInfo(const std::shared_ptr<arrow::Table>& table);

  struct ChunkInfo {
    int chunk_index;
    int64_t offset_in_chunk;
  };
  [[nodiscard]] ChunkInfo get_chunk_info(int column_index,
                                         int64_t row_index) const;

  int num_columns() const { return chunk_boundaries_.size(); }

  [[nodiscard]] int64_t num_rows() const { return num_rows_; }

 private:
  std::vector<std::vector<int64_t>> chunk_boundaries_;
  int64_t num_rows_;
};

}  // namespace tundradb

#endif  // TABLE_INFO_HPP