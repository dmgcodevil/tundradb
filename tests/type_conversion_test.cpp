#include <arrow/api.h>
#include <gtest/gtest.h>

#include "../include/schema.hpp"
#include "../include/types.hpp"

using namespace tundradb;

class TypeRoundTripTest : public ::testing::Test {
 protected:
  void round_trip(const std::string& name, const TypeDescriptor& td,
                  arrow::Type::type expected_arrow_type,
                  ValueType expected_back_type) {
    auto field = std::make_shared<Field>(name, td);
    auto arrow_res = field->to_arrow();
    ASSERT_TRUE(arrow_res.ok())
        << name << ": " << arrow_res.status().ToString();
    auto arrow_field = arrow_res.ValueOrDie();

    EXPECT_EQ(arrow_field->name(), name);
    EXPECT_EQ(arrow_field->type()->id(), expected_arrow_type)
        << name << ": expected Arrow type "
        << static_cast<int>(expected_arrow_type) << " got "
        << static_cast<int>(arrow_field->type()->id());

    auto back_res = Field::from_arrow(arrow_field);
    ASSERT_TRUE(back_res.ok()) << name << ": " << back_res.status().ToString();
    Field back = back_res.ValueOrDie();

    EXPECT_EQ(back.name(), name);
    EXPECT_EQ(back.type(), expected_back_type)
        << name << ": expected ValueType "
        << static_cast<int>(expected_back_type) << " got "
        << static_cast<int>(back.type());
  }

  void round_trip_scalar(const std::string& name, ValueType vt,
                         arrow::Type::type expected_arrow) {
    round_trip(name, TypeDescriptor{vt}, expected_arrow, vt);
  }
};

// --- Scalar types ---

TEST_F(TypeRoundTripTest, Bool) {
  round_trip_scalar("b", ValueType::BOOL, arrow::Type::BOOL);
}

TEST_F(TypeRoundTripTest, Int32) {
  round_trip_scalar("i", ValueType::INT32, arrow::Type::INT32);
}

TEST_F(TypeRoundTripTest, Int64) {
  round_trip_scalar("l", ValueType::INT64, arrow::Type::INT64);
}

TEST_F(TypeRoundTripTest, Float) {
  round_trip_scalar("f", ValueType::FLOAT, arrow::Type::FLOAT);
}

TEST_F(TypeRoundTripTest, Double) {
  round_trip_scalar("d", ValueType::DOUBLE, arrow::Type::DOUBLE);
}

TEST_F(TypeRoundTripTest, String) {
  round_trip_scalar("s", ValueType::STRING, arrow::Type::STRING);
}

// --- Fixed-string variants all map to Arrow utf8 and come back as STRING ---

TEST_F(TypeRoundTripTest, FixedString16) {
  round_trip("fs16", TypeDescriptor{ValueType::FIXED_STRING16},
             arrow::Type::STRING, ValueType::STRING);
}

TEST_F(TypeRoundTripTest, FixedString32) {
  round_trip("fs32", TypeDescriptor{ValueType::FIXED_STRING32},
             arrow::Type::STRING, ValueType::STRING);
}

TEST_F(TypeRoundTripTest, FixedString64) {
  round_trip("fs64", TypeDescriptor{ValueType::FIXED_STRING64},
             arrow::Type::STRING, ValueType::STRING);
}

// --- Array types ---

TEST_F(TypeRoundTripTest, ArrayInt32) {
  round_trip("ai", TypeDescriptor::array(ValueType::INT32), arrow::Type::LIST,
             ValueType::ARRAY);
}

TEST_F(TypeRoundTripTest, ArrayInt64) {
  round_trip("al", TypeDescriptor::array(ValueType::INT64), arrow::Type::LIST,
             ValueType::ARRAY);
}

TEST_F(TypeRoundTripTest, ArrayFloat) {
  round_trip("af", TypeDescriptor::array(ValueType::FLOAT), arrow::Type::LIST,
             ValueType::ARRAY);
}

TEST_F(TypeRoundTripTest, ArrayDouble) {
  round_trip("ad", TypeDescriptor::array(ValueType::DOUBLE), arrow::Type::LIST,
             ValueType::ARRAY);
}

TEST_F(TypeRoundTripTest, ArrayBool) {
  round_trip("ab", TypeDescriptor::array(ValueType::BOOL), arrow::Type::LIST,
             ValueType::ARRAY);
}

TEST_F(TypeRoundTripTest, ArrayString) {
  round_trip("as", TypeDescriptor::array(ValueType::STRING), arrow::Type::LIST,
             ValueType::ARRAY);
}

// --- Fixed-size array ---

TEST_F(TypeRoundTripTest, FixedSizeArrayInt32) {
  round_trip("fai", TypeDescriptor::array(ValueType::INT32, 10),
             arrow::Type::FIXED_SIZE_LIST, ValueType::ARRAY);
}

// --- MAP type ---

TEST_F(TypeRoundTripTest, Map) {
  round_trip("m", TypeDescriptor::properties(), arrow::Type::MAP,
             ValueType::MAP);
}

// --- Arrow-incoming types that widen (e.g. INT8 -> INT32) ---

TEST_F(TypeRoundTripTest, ArrowInt8ToInt32) {
  auto arrow_field = arrow::field("tiny", arrow::int8());
  auto back_res = Field::from_arrow(arrow_field);
  ASSERT_TRUE(back_res.ok());
  EXPECT_EQ(back_res.ValueOrDie().type(), ValueType::INT32);
}

TEST_F(TypeRoundTripTest, ArrowInt16ToInt32) {
  auto arrow_field = arrow::field("small", arrow::int16());
  auto back_res = Field::from_arrow(arrow_field);
  ASSERT_TRUE(back_res.ok());
  EXPECT_EQ(back_res.ValueOrDie().type(), ValueType::INT32);
}

TEST_F(TypeRoundTripTest, ArrowUint8ToInt64) {
  auto arrow_field = arrow::field("u8", arrow::uint8());
  auto back_res = Field::from_arrow(arrow_field);
  ASSERT_TRUE(back_res.ok());
  EXPECT_EQ(back_res.ValueOrDie().type(), ValueType::INT64);
}

TEST_F(TypeRoundTripTest, ArrowUint64ToInt64) {
  auto arrow_field = arrow::field("u64", arrow::uint64());
  auto back_res = Field::from_arrow(arrow_field);
  ASSERT_TRUE(back_res.ok());
  EXPECT_EQ(back_res.ValueOrDie().type(), ValueType::INT64);
}

TEST_F(TypeRoundTripTest, ArrowLargeStringToString) {
  auto arrow_field = arrow::field("ls", arrow::large_utf8());
  auto back_res = Field::from_arrow(arrow_field);
  ASSERT_TRUE(back_res.ok());
  EXPECT_EQ(back_res.ValueOrDie().type(), ValueType::STRING);
}

// --- Full schema round-trip ---

TEST_F(TypeRoundTripTest, FullSchemaRoundTrip) {
  llvm::SmallVector<std::shared_ptr<Field>, 4> fields;
  fields.push_back(std::make_shared<Field>("b", ValueType::BOOL));
  fields.push_back(std::make_shared<Field>("i32", ValueType::INT32));
  fields.push_back(std::make_shared<Field>("i64", ValueType::INT64));
  fields.push_back(std::make_shared<Field>("f32", ValueType::FLOAT));
  fields.push_back(std::make_shared<Field>("f64", ValueType::DOUBLE));
  fields.push_back(std::make_shared<Field>("s", ValueType::STRING));
  fields.push_back(
      std::make_shared<Field>("arr", TypeDescriptor::array(ValueType::INT32)));
  fields.push_back(std::make_shared<Field>("m", TypeDescriptor::properties()));

  auto schema = std::make_shared<Schema>("All", 0, fields);
  auto arrow_schema = Schema::to_arrow(schema);

  ASSERT_EQ(arrow_schema->num_fields(), 8);
  EXPECT_EQ(arrow_schema->field(0)->type()->id(), arrow::Type::BOOL);
  EXPECT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::INT32);
  EXPECT_EQ(arrow_schema->field(2)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(arrow_schema->field(3)->type()->id(), arrow::Type::FLOAT);
  EXPECT_EQ(arrow_schema->field(4)->type()->id(), arrow::Type::DOUBLE);
  EXPECT_EQ(arrow_schema->field(5)->type()->id(), arrow::Type::STRING);
  EXPECT_EQ(arrow_schema->field(6)->type()->id(), arrow::Type::LIST);
  EXPECT_EQ(arrow_schema->field(7)->type()->id(), arrow::Type::MAP);

  auto back_res = Schema::from_arrow("All", arrow_schema);
  ASSERT_TRUE(back_res.ok()) << back_res.status().ToString();
  auto back = back_res.ValueOrDie();

  ASSERT_EQ(back->num_fields(), 8);
  EXPECT_EQ(back->field(0)->type(), ValueType::BOOL);
  EXPECT_EQ(back->field(1)->type(), ValueType::INT32);
  EXPECT_EQ(back->field(2)->type(), ValueType::INT64);
  EXPECT_EQ(back->field(3)->type(), ValueType::FLOAT);
  EXPECT_EQ(back->field(4)->type(), ValueType::DOUBLE);
  EXPECT_EQ(back->field(5)->type(), ValueType::STRING);
  EXPECT_EQ(back->field(6)->type(), ValueType::ARRAY);
  EXPECT_EQ(back->field(7)->type(), ValueType::MAP);
}
