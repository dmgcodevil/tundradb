#include "schema_utils.hpp"

#include <arrow/api.h>
#include <gtest/gtest.h>

namespace tundradb {

class SchemaUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a test schema
    original_schema_ = arrow::schema({arrow::field("name", arrow::utf8()),
                                      arrow::field("age", arrow::int32())});
  }

  std::shared_ptr<arrow::Schema> original_schema_;
};

TEST_F(SchemaUtilsTest, PrependIdField) {
  // Create a new schema with id field prepended
  auto new_schema = prepend_id_field(original_schema_);

  // Check that the new schema has one more field
  EXPECT_EQ(new_schema->num_fields(), original_schema_->num_fields() + 1);

  // Check that the first field is "id" of type Int64
  EXPECT_EQ(new_schema->field(0)->name(), "id");
  EXPECT_EQ(new_schema->field(0)->type()->id(), arrow::Type::INT64);

  // Check that the remaining fields are unchanged
  EXPECT_EQ(new_schema->field(1)->name(), "name");
  EXPECT_EQ(new_schema->field(2)->name(), "age");
}

TEST_F(SchemaUtilsTest, PrependIdFieldEmptySchema) {
  // Create an empty schema
  auto empty_schema = arrow::schema({});

  // Create a new schema with id field prepended
  auto new_schema = prepend_id_field(empty_schema);

  // Check that the new schema has exactly one field
  EXPECT_EQ(new_schema->num_fields(), 1);

  // Check that the field is "id" of type Int64
  EXPECT_EQ(new_schema->field(0)->name(), "id");
  EXPECT_EQ(new_schema->field(0)->type()->id(), arrow::Type::INT64);
}

}  // namespace tundradb