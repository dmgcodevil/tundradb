#include <gtest/gtest.h>

#include <climits>

#include "schema/type_descriptor.hpp"
#include "common/value_type.hpp"

using namespace tundradb;

// ---------------------------------------------------------------------------
// TypeDescriptor — factories and fields
// ---------------------------------------------------------------------------

TEST(TypeDescriptorTest, FactoryNa) {
  auto t = TypeDescriptor::na();
  EXPECT_EQ(t.base_type, ValueType::NA);
  EXPECT_EQ(t.element_type, ValueType::NA);
  EXPECT_EQ(t.fixed_size, 0u);
  EXPECT_EQ(t.max_string_size, 0u);
}

TEST(TypeDescriptorTest, FactoryInt32) {
  auto t = TypeDescriptor::int32();
  EXPECT_EQ(t.base_type, ValueType::INT32);
}

TEST(TypeDescriptorTest, FactoryInt64) {
  auto t = TypeDescriptor::int64();
  EXPECT_EQ(t.base_type, ValueType::INT64);
}

TEST(TypeDescriptorTest, FactoryFloat32) {
  auto t = TypeDescriptor::float32();
  EXPECT_EQ(t.base_type, ValueType::FLOAT);
}

TEST(TypeDescriptorTest, FactoryFloat64) {
  auto t = TypeDescriptor::float64();
  EXPECT_EQ(t.base_type, ValueType::DOUBLE);
}

TEST(TypeDescriptorTest, FactoryBoolean) {
  auto t = TypeDescriptor::boolean();
  EXPECT_EQ(t.base_type, ValueType::BOOL);
}

TEST(TypeDescriptorTest, FactoryStringUnlimited) {
  auto t = TypeDescriptor::string();
  EXPECT_EQ(t.base_type, ValueType::STRING);
  EXPECT_EQ(t.max_string_size, 0u);
}

TEST(TypeDescriptorTest, FactoryStringMax64) {
  auto t = TypeDescriptor::string(64);
  EXPECT_EQ(t.base_type, ValueType::STRING);
  EXPECT_EQ(t.max_string_size, 64u);
}

TEST(TypeDescriptorTest, FactoryArrayDynamicInt32) {
  auto t = TypeDescriptor::array(ValueType::INT32);
  EXPECT_EQ(t.base_type, ValueType::ARRAY);
  EXPECT_EQ(t.element_type, ValueType::INT32);
  EXPECT_EQ(t.fixed_size, 0u);
}

TEST(TypeDescriptorTest, FactoryArrayFixedInt32) {
  auto t = TypeDescriptor::array(ValueType::INT32, 10);
  EXPECT_EQ(t.base_type, ValueType::ARRAY);
  EXPECT_EQ(t.element_type, ValueType::INT32);
  EXPECT_EQ(t.fixed_size, 10u);
}

TEST(TypeDescriptorTest, FactoryProperties) {
  auto t = TypeDescriptor::properties();
  EXPECT_EQ(t.base_type, ValueType::MAP);
}

// ---------------------------------------------------------------------------
// TypeDescriptor — from_value_type
// ---------------------------------------------------------------------------

TEST(TypeDescriptorTest, FromValueTypeFixedString16) {
  auto t = TypeDescriptor::from_value_type(ValueType::FIXED_STRING16);
  EXPECT_EQ(t.base_type, ValueType::STRING);
  EXPECT_EQ(t.max_string_size, 16u);
}

TEST(TypeDescriptorTest, FromValueTypeFixedString32) {
  auto t = TypeDescriptor::from_value_type(ValueType::FIXED_STRING32);
  EXPECT_EQ(t.base_type, ValueType::STRING);
  EXPECT_EQ(t.max_string_size, 32u);
}

TEST(TypeDescriptorTest, FromValueTypeFixedString64) {
  auto t = TypeDescriptor::from_value_type(ValueType::FIXED_STRING64);
  EXPECT_EQ(t.base_type, ValueType::STRING);
  EXPECT_EQ(t.max_string_size, 64u);
}

TEST(TypeDescriptorTest, FromValueTypeRegularInt32) {
  auto t = TypeDescriptor::from_value_type(ValueType::INT32);
  EXPECT_EQ(t.base_type, ValueType::INT32);
  EXPECT_EQ(t.max_string_size, 0u);
}

// ---------------------------------------------------------------------------
// TypeDescriptor — type checks (categories)
// ---------------------------------------------------------------------------

TEST(TypeDescriptorTest, IsPrimitive) {
  EXPECT_TRUE(TypeDescriptor::int32().is_primitive());
  EXPECT_TRUE(TypeDescriptor::int64().is_primitive());
  EXPECT_TRUE(TypeDescriptor::float32().is_primitive());
  EXPECT_TRUE(TypeDescriptor::float64().is_primitive());
  EXPECT_TRUE(TypeDescriptor::boolean().is_primitive());
  EXPECT_FALSE(TypeDescriptor::na().is_primitive());
  EXPECT_FALSE(TypeDescriptor::string().is_primitive());
  EXPECT_FALSE(TypeDescriptor::array(ValueType::INT32).is_primitive());
  EXPECT_FALSE(TypeDescriptor::properties().is_primitive());
}

TEST(TypeDescriptorTest, IsString) {
  EXPECT_TRUE(TypeDescriptor::string().is_string());
  EXPECT_TRUE(TypeDescriptor::string(64).is_string());
  TypeDescriptor fs16{ValueType::FIXED_STRING16};
  TypeDescriptor fs32{ValueType::FIXED_STRING32};
  TypeDescriptor fs64{ValueType::FIXED_STRING64};
  EXPECT_TRUE(fs16.is_string());
  EXPECT_TRUE(fs32.is_string());
  EXPECT_TRUE(fs64.is_string());
  EXPECT_FALSE(TypeDescriptor::int32().is_string());
}

TEST(TypeDescriptorTest, IsArray) {
  EXPECT_TRUE(TypeDescriptor::array(ValueType::INT32).is_array());
  EXPECT_TRUE(TypeDescriptor::array(ValueType::INT32, 5).is_array());
  EXPECT_FALSE(TypeDescriptor::int32().is_array());
}

TEST(TypeDescriptorTest, IsMap) {
  EXPECT_TRUE(TypeDescriptor::properties().is_map());
  EXPECT_FALSE(TypeDescriptor::int32().is_map());
}

TEST(TypeDescriptorTest, IsNull) {
  EXPECT_TRUE(TypeDescriptor::na().is_null());
  EXPECT_FALSE(TypeDescriptor::int32().is_null());
}

TEST(TypeDescriptorTest, IsFixedSizeArrayAndDynamicArray) {
  auto dyn = TypeDescriptor::array(ValueType::INT32);
  auto fixed = TypeDescriptor::array(ValueType::BOOL, 7);
  EXPECT_TRUE(dyn.is_dynamic_array());
  EXPECT_FALSE(dyn.is_fixed_size_array());
  EXPECT_TRUE(fixed.is_fixed_size_array());
  EXPECT_FALSE(fixed.is_dynamic_array());
  EXPECT_FALSE(TypeDescriptor::int32().is_fixed_size_array());
  EXPECT_FALSE(TypeDescriptor::int32().is_dynamic_array());
}

// ---------------------------------------------------------------------------
// TypeDescriptor — storage_size / storage_alignment
// ---------------------------------------------------------------------------

TEST(TypeDescriptorTest, StorageSizeAndAlignment) {
  EXPECT_EQ(TypeDescriptor::na().storage_size(), 0u);
  EXPECT_EQ(TypeDescriptor::na().storage_alignment(), 1u);

  EXPECT_EQ(TypeDescriptor::int32().storage_size(), 4u);
  EXPECT_EQ(TypeDescriptor::int32().storage_alignment(), 4u);

  EXPECT_EQ(TypeDescriptor::int64().storage_size(), 8u);
  EXPECT_EQ(TypeDescriptor::int64().storage_alignment(), 8u);

  EXPECT_EQ(TypeDescriptor::float32().storage_size(), 4u);
  EXPECT_EQ(TypeDescriptor::float32().storage_alignment(), 4u);

  EXPECT_EQ(TypeDescriptor::float64().storage_size(), 8u);
  EXPECT_EQ(TypeDescriptor::float64().storage_alignment(), 8u);

  EXPECT_EQ(TypeDescriptor::boolean().storage_size(), 1u);
  EXPECT_EQ(TypeDescriptor::boolean().storage_alignment(), 1u);

  EXPECT_EQ(TypeDescriptor::string().storage_size(), 16u);
  EXPECT_EQ(TypeDescriptor::string().storage_alignment(), 8u);

  EXPECT_EQ(TypeDescriptor::array(ValueType::INT32).storage_size(), 16u);
  EXPECT_EQ(TypeDescriptor::array(ValueType::INT32).storage_alignment(), 8u);

  EXPECT_EQ(TypeDescriptor::properties().storage_size(), 16u);
  EXPECT_EQ(TypeDescriptor::properties().storage_alignment(), 8u);
}

// ---------------------------------------------------------------------------
// TypeDescriptor — to_string
// ---------------------------------------------------------------------------

TEST(TypeDescriptorTest, ToStringPrimitivesAndMap) {
  EXPECT_EQ(TypeDescriptor::na().to_string(), "Null");
  EXPECT_EQ(TypeDescriptor::int32().to_string(), "Int32");
  EXPECT_EQ(TypeDescriptor::int64().to_string(), "Int64");
  EXPECT_EQ(TypeDescriptor::float32().to_string(), "Float");
  EXPECT_EQ(TypeDescriptor::float64().to_string(), "Double");
  EXPECT_EQ(TypeDescriptor::boolean().to_string(), "Bool");
  EXPECT_EQ(TypeDescriptor::properties().to_string(), "Map");
}

TEST(TypeDescriptorTest, ToStringStrings) {
  EXPECT_EQ(TypeDescriptor::string().to_string(), "String");
  EXPECT_EQ(TypeDescriptor::string(64).to_string(), "STRING(64)");
  TypeDescriptor fs{ValueType::FIXED_STRING32};
  EXPECT_EQ(fs.to_string(), "FixedString32");
}

TEST(TypeDescriptorTest, ToStringArrays) {
  EXPECT_EQ(TypeDescriptor::array(ValueType::INT32).to_string(),
            "ARRAY<Int32>");
  EXPECT_EQ(TypeDescriptor::array(ValueType::INT32, 10).to_string(),
            "ARRAY<Int32, 10>");
  EXPECT_EQ(TypeDescriptor::array(ValueType::STRING).to_string(),
            "ARRAY<String>");
  EXPECT_EQ(TypeDescriptor::array(ValueType::STRING, 3).to_string(),
            "ARRAY<String, 3>");
}

// ---------------------------------------------------------------------------
// TypeDescriptor — comparison
// ---------------------------------------------------------------------------

TEST(TypeDescriptorTest, EqualityAndInequality) {
  auto a = TypeDescriptor::int32();
  auto b = TypeDescriptor::int32();
  auto c = TypeDescriptor::int64();
  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a != b);
  EXPECT_FALSE(a == c);
  EXPECT_TRUE(a != c);

  auto s1 = TypeDescriptor::string(10);
  auto s2 = TypeDescriptor::string(10);
  auto s3 = TypeDescriptor::string(11);
  EXPECT_TRUE(s1 == s2);
  EXPECT_FALSE(s1 == s3);

  auto ar1 = TypeDescriptor::array(ValueType::BOOL, 2);
  auto ar2 = TypeDescriptor::array(ValueType::BOOL, 2);
  auto ar3 = TypeDescriptor::array(ValueType::BOOL, 3);
  auto ar4 = TypeDescriptor::array(ValueType::INT32, 2);
  EXPECT_TRUE(ar1 == ar2);
  EXPECT_FALSE(ar1 == ar3);
  EXPECT_FALSE(ar1 == ar4);
}

// ---------------------------------------------------------------------------
// ValueType — to_string (all enumerators)
// ---------------------------------------------------------------------------

TEST(ValueTypeTest, ToStringAllValues) {
  EXPECT_EQ(to_string(ValueType::NA), "Null");
  EXPECT_EQ(to_string(ValueType::INT32), "Int32");
  EXPECT_EQ(to_string(ValueType::INT64), "Int64");
  EXPECT_EQ(to_string(ValueType::FLOAT), "Float");
  EXPECT_EQ(to_string(ValueType::DOUBLE), "Double");
  EXPECT_EQ(to_string(ValueType::STRING), "String");
  EXPECT_EQ(to_string(ValueType::FIXED_STRING16), "FixedString16");
  EXPECT_EQ(to_string(ValueType::FIXED_STRING32), "FixedString32");
  EXPECT_EQ(to_string(ValueType::FIXED_STRING64), "FixedString64");
  EXPECT_EQ(to_string(ValueType::BOOL), "Bool");
  EXPECT_EQ(to_string(ValueType::ARRAY), "Array");
  EXPECT_EQ(to_string(ValueType::MAP), "Map");
}

TEST(ValueTypeTest, ToStringUnknownEnumerator) {
  auto invalid = static_cast<ValueType>(9999);
  EXPECT_EQ(to_string(invalid), "Unknown");
}

// ---------------------------------------------------------------------------
// ValueType — get_type_size / get_type_alignment
// ---------------------------------------------------------------------------

TEST(ValueTypeTest, GetTypeSize) {
  EXPECT_EQ(get_type_size(ValueType::NA), 0u);
  EXPECT_EQ(get_type_size(ValueType::INT32), 4u);
  EXPECT_EQ(get_type_size(ValueType::INT64), 8u);
  EXPECT_EQ(get_type_size(ValueType::FLOAT), 4u);
  EXPECT_EQ(get_type_size(ValueType::DOUBLE), 8u);
  EXPECT_EQ(get_type_size(ValueType::BOOL), 1u);
  EXPECT_EQ(get_type_size(ValueType::STRING), 16u);
  EXPECT_EQ(get_type_size(ValueType::FIXED_STRING16), 16u);
  EXPECT_EQ(get_type_size(ValueType::FIXED_STRING32), 16u);
  EXPECT_EQ(get_type_size(ValueType::FIXED_STRING64), 16u);
  EXPECT_EQ(get_type_size(ValueType::ARRAY), 16u);
  EXPECT_EQ(get_type_size(ValueType::MAP), 16u);
}

TEST(ValueTypeTest, GetTypeAlignment) {
  EXPECT_EQ(get_type_alignment(ValueType::NA), 1u);
  EXPECT_EQ(get_type_alignment(ValueType::INT32), 4u);
  EXPECT_EQ(get_type_alignment(ValueType::INT64), 8u);
  EXPECT_EQ(get_type_alignment(ValueType::FLOAT), 4u);
  EXPECT_EQ(get_type_alignment(ValueType::DOUBLE), 8u);
  EXPECT_EQ(get_type_alignment(ValueType::BOOL), 1u);
  EXPECT_EQ(get_type_alignment(ValueType::STRING), 8u);
  EXPECT_EQ(get_type_alignment(ValueType::FIXED_STRING16), 8u);
  EXPECT_EQ(get_type_alignment(ValueType::ARRAY), 8u);
  EXPECT_EQ(get_type_alignment(ValueType::MAP), 8u);
}

// ---------------------------------------------------------------------------
// ValueType — is_string_type / is_array_type / is_map_type
// ---------------------------------------------------------------------------

TEST(ValueTypeTest, IsStringType) {
  EXPECT_FALSE(is_string_type(ValueType::NA));
  EXPECT_FALSE(is_string_type(ValueType::INT32));
  EXPECT_TRUE(is_string_type(ValueType::STRING));
  EXPECT_TRUE(is_string_type(ValueType::FIXED_STRING16));
  EXPECT_TRUE(is_string_type(ValueType::FIXED_STRING32));
  EXPECT_TRUE(is_string_type(ValueType::FIXED_STRING64));
  EXPECT_FALSE(is_string_type(ValueType::ARRAY));
  EXPECT_FALSE(is_string_type(ValueType::MAP));
}

TEST(ValueTypeTest, IsArrayType) {
  EXPECT_TRUE(is_array_type(ValueType::ARRAY));
  EXPECT_FALSE(is_array_type(ValueType::MAP));
  EXPECT_FALSE(is_array_type(ValueType::STRING));
}

TEST(ValueTypeTest, IsMapType) {
  EXPECT_TRUE(is_map_type(ValueType::MAP));
  EXPECT_FALSE(is_map_type(ValueType::ARRAY));
  EXPECT_FALSE(is_map_type(ValueType::INT32));
}

// ---------------------------------------------------------------------------
// ValueType — get_string_max_size (completes value_type.hpp coverage)
// ---------------------------------------------------------------------------

TEST(ValueTypeTest, GetStringMaxSize) {
  EXPECT_EQ(get_string_max_size(ValueType::STRING), SIZE_MAX);
  EXPECT_EQ(get_string_max_size(ValueType::FIXED_STRING16), 16u);
  EXPECT_EQ(get_string_max_size(ValueType::FIXED_STRING32), 32u);
  EXPECT_EQ(get_string_max_size(ValueType::FIXED_STRING64), 64u);
  EXPECT_EQ(get_string_max_size(ValueType::INT32), 0u);
}
