#include "table_info.hpp"

#include <arrow/api.h>
#include <arrow/table.h>
#include <gtest/gtest.h>

namespace tundradb {

class TableInfoTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a schema
    auto schema = arrow::schema({arrow::field("col1", arrow::int64())});

    // Create arrays with different chunk sizes
    arrow::Int64Builder builder1;
    ASSERT_TRUE(builder1.Append(1).ok());
    ASSERT_TRUE(builder1.Append(2).ok());
    std::shared_ptr<arrow::Array> array1;
    ASSERT_TRUE(builder1.Finish(&array1).ok());

    arrow::Int64Builder builder2;
    ASSERT_TRUE(builder2.Append(3).ok());
    ASSERT_TRUE(builder2.Append(4).ok());
    std::shared_ptr<arrow::Array> array2;
    ASSERT_TRUE(builder2.Finish(&array2).ok());

    arrow::Int64Builder builder3;
    ASSERT_TRUE(builder3.Append(5).ok());
    std::shared_ptr<arrow::Array> array3;
    ASSERT_TRUE(builder3.Finish(&array3).ok());

    // Combine arrays into a chunked array
    std::vector<std::shared_ptr<arrow::Array>> chunks = {array1, array2,
                                                         array3};
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);

    // Create table
    table_ = arrow::Table::Make(schema, {chunked_array});
  }

  void TearDown() override {
    // Clean up any resources
    table_.reset();
  }

  std::shared_ptr<arrow::Table> table_;
};

TEST_F(TableInfoTest, BasicFunctionality) {
  TableInfo info(table_);

  // Test first chunk
  auto loc1 = info.get_chunk_info(0, 0);
  EXPECT_EQ(loc1.chunk_index, 0);
  EXPECT_EQ(loc1.offset_in_chunk, 0);

  auto loc2 = info.get_chunk_info(0, 1);
  EXPECT_EQ(loc2.chunk_index, 0);
  EXPECT_EQ(loc2.offset_in_chunk, 1);

  // Test second chunk
  auto loc3 = info.get_chunk_info(0, 2);
  EXPECT_EQ(loc3.chunk_index, 1);
  EXPECT_EQ(loc3.offset_in_chunk, 0);

  auto loc4 = info.get_chunk_info(0, 3);
  EXPECT_EQ(loc4.chunk_index, 1);
  EXPECT_EQ(loc4.offset_in_chunk, 1);

  // Test third chunk (smaller size)
  auto loc5 = info.get_chunk_info(0, 4);
  EXPECT_EQ(loc5.chunk_index, 2);
  EXPECT_EQ(loc5.offset_in_chunk, 0);
}

TEST_F(TableInfoTest, OutOfRange) {
  TableInfo info(table_);

  // Test out of range column
  EXPECT_THROW(info.get_chunk_info(1, 0), std::out_of_range);
  EXPECT_THROW(info.get_chunk_info(-1, 0), std::out_of_range);

  // Test out of range row
  EXPECT_THROW(info.get_chunk_info(0, -1), std::out_of_range);
  EXPECT_THROW(info.get_chunk_info(0, 5), std::out_of_range);
}

TEST_F(TableInfoTest, TableProperties) {
  TableInfo info(table_);

  EXPECT_EQ(info.num_columns(), 1);
  EXPECT_EQ(info.num_rows(), 5);
}

}  // namespace tundradb