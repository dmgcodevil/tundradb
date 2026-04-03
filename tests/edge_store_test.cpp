#include "edge_store.hpp"

#include <gtest/gtest.h>

#include "edge.hpp"

namespace tundradb {

class EdgeStoreTest : public ::testing::Test {
 protected:
  void SetUp() override { store = std::make_unique<EdgeStore>(0); }

  void TearDown() override { store.reset(); }
  std::unique_ptr<EdgeStore> store;
};

TEST_F(EdgeStoreTest, CreateAndAddEdge) {
  auto edge_res = store->create_edge(1, "test_type", 2);
  ASSERT_TRUE(edge_res.ok());
  auto edge = edge_res.ValueOrDie();

  auto add_res = store->add(edge);
  ASSERT_TRUE(add_res.ok());
  ASSERT_TRUE(add_res.ValueOrDie());

  auto get_res = store->get(edge->get_id());
  ASSERT_TRUE(get_res.ok());
  auto retrieved_edge = get_res.ValueOrDie();
  ASSERT_EQ(retrieved_edge->get_id(), edge->get_id());
  ASSERT_EQ(retrieved_edge->get_source_id(), 1);
  ASSERT_EQ(retrieved_edge->get_target_id(), 2);
  ASSERT_EQ(retrieved_edge->get_type(), "test_type");
}

TEST_F(EdgeStoreTest, GetOutgoingAndIncomingEdges) {
  auto edge1_res = store->create_edge(1, "test_type", 2);
  auto edge2_res = store->create_edge(1, "test_type", 3);
  auto edge3_res = store->create_edge(2, "test_type", 1);

  ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge3_res.ValueOrDie()).ok());

  auto outgoing_res = store->get_outgoing_edges(1, "test_type");
  ASSERT_TRUE(outgoing_res.ok());
  auto outgoing_edges = outgoing_res.ValueOrDie();
  ASSERT_EQ(outgoing_edges.size(), 2);

  auto incoming_res = store->get_incoming_edges(1, "test_type");
  ASSERT_TRUE(incoming_res.ok());
  auto incoming_edges = incoming_res.ValueOrDie();
  ASSERT_EQ(incoming_edges.size(), 1);
}

TEST_F(EdgeStoreTest, GetByType) {
  auto edge1_res = store->create_edge(1, "type1", 2);
  auto edge2_res = store->create_edge(2, "type2", 3);

  ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());

  auto type1_res = store->get_by_type("type1");
  ASSERT_TRUE(type1_res.ok());
  ASSERT_EQ(type1_res.ValueOrDie().size(), 1);
  ASSERT_EQ(type1_res.ValueOrDie()[0]->get_type(), "type1");

  auto type2_res = store->get_by_type("type2");
  ASSERT_TRUE(type2_res.ok());
  ASSERT_EQ(type2_res.ValueOrDie().size(), 1);
  ASSERT_EQ(type2_res.ValueOrDie()[0]->get_type(), "type2");
}

TEST_F(EdgeStoreTest, RemoveEdge) {
  auto edge_res = store->create_edge(1, "test_type", 2);
  ASSERT_TRUE(edge_res.ok());
  auto edge = edge_res.ValueOrDie();
  ASSERT_TRUE(store->add(edge).ok());

  auto remove_res = store->remove(edge->get_id());
  ASSERT_TRUE(remove_res.ok());

  auto get_res = store->get(edge->get_id());
  ASSERT_FALSE(get_res.ok());
}

TEST_F(EdgeStoreTest, GetEdgeTypes) {
  auto edge1_res = store->create_edge(1, "type1", 2);
  auto edge2_res = store->create_edge(2, "type2", 3);

  ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());

  auto types = store->get_edge_types();
  ASSERT_EQ(types.size(), 2);
  ASSERT_TRUE(types.find("type1") != types.end());
  ASSERT_TRUE(types.find("type2") != types.end());
}

TEST_F(EdgeStoreTest, GetTable) {
  auto edge1_res = store->create_edge(1, "test_type", 2);
  auto edge2_res = store->create_edge(2, "test_type", 3);
  auto edge3_res = store->create_edge(3, "test_type", 1);

  ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge3_res.ValueOrDie()).ok());

  auto table_res = store->get_table("test_type");
  ASSERT_TRUE(table_res.ok());
  auto table = table_res.ValueOrDie();

  ASSERT_EQ(table->num_columns(), 4);
  ASSERT_EQ(table->num_rows(), 3);

  auto id_array =
      std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  auto source_id_array =
      std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
  auto target_id_array =
      std::static_pointer_cast<arrow::Int64Array>(table->column(2)->chunk(0));

  ASSERT_EQ(id_array->Value(0), edge1_res.ValueOrDie()->get_id());
  ASSERT_EQ(source_id_array->Value(0), 1);
  ASSERT_EQ(target_id_array->Value(0), 2);
}

TEST_F(EdgeStoreTest, GetTableWithEmptyType) {
  auto edge1_res = store->create_edge(1, "type1", 2);
  auto edge2_res = store->create_edge(2, "type2", 3);

  ASSERT_TRUE(store->add(edge1_res.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(edge2_res.ValueOrDie()).ok());

  auto table_res = store->get_table("");
  ASSERT_FALSE(table_res.ok());
}

TEST_F(EdgeStoreTest, GetTableWithNonExistentType) {
  auto table_res = store->get_table("non_existent");
  ASSERT_FALSE(table_res.ok());
}

TEST_F(EdgeStoreTest, ConcurrentAccess) {
  const int NUM_THREADS = 8;
  const int OPERATIONS_PER_THREAD = 1000;
  std::vector<std::thread> threads;

  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back([this, i]() {
      for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
        auto edge_res = store->create_edge(i, "test_type", j);
        ASSERT_TRUE(edge_res.ok());
        ASSERT_TRUE(store->add(edge_res.ValueOrDie()).ok());
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  auto table_res = store->get_table("test_type");
  ASSERT_TRUE(table_res.ok());
  auto table = table_res.ValueOrDie();
  ASSERT_EQ(table->num_rows(), NUM_THREADS * OPERATIONS_PER_THREAD);
}

// --- Edge property tests ---

class EdgePropertyTest : public ::testing::Test {
 protected:
  void SetUp() override { store = std::make_unique<EdgeStore>(0); }
  void TearDown() override { store.reset(); }
  std::unique_ptr<EdgeStore> store;
};

TEST_F(EdgePropertyTest, RegisterEdgeSchema) {
  auto res = store->register_edge_schema(
      "works_at", {std::make_shared<Field>("role", ValueType::STRING),
                   std::make_shared<Field>("since", ValueType::INT64)});
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(store->has_edge_schema("works_at"));

  auto schema = store->get_edge_schema("works_at");
  ASSERT_NE(schema, nullptr);
  ASSERT_EQ(schema->num_fields(), 2);
  ASSERT_EQ(schema->field(0)->name(), "role");
  ASSERT_EQ(schema->field(1)->name(), "since");
}

TEST_F(EdgePropertyTest, DuplicateSchemaRegistration) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at", {std::make_shared<Field>("role", ValueType::STRING)})
          .ok());
  auto res = store->register_edge_schema(
      "works_at", {std::make_shared<Field>("role", ValueType::STRING)});
  ASSERT_FALSE(res.ok());
}

TEST_F(EdgePropertyTest, CreateEdgeWithProperties) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at", {std::make_shared<Field>("role", ValueType::STRING),
                           std::make_shared<Field>("since", ValueType::INT64)})
          .ok());

  auto edge_res = store->create_edge(1, "works_at", 2,
                                     {{"role", Value(std::string("engineer"))},
                                      {"since", Value(int64_t(2020))}});
  ASSERT_TRUE(edge_res.ok());

  auto edge = edge_res.ValueOrDie();
  auto schema = store->get_edge_schema("works_at");
  ASSERT_NE(schema, nullptr);

  auto role_val = edge->get_value(schema->get_field("role"));
  ASSERT_TRUE(role_val.ok());
  ASSERT_EQ(role_val.ValueOrDie().as_string(), "engineer");

  auto since_val = edge->get_value(schema->get_field("since"));
  ASSERT_TRUE(since_val.ok());
  ASSERT_EQ(since_val.ValueOrDie().as_int64(), 2020);
}

TEST_F(EdgePropertyTest, CreateEdgeWithUnknownProperty) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at", {std::make_shared<Field>("role", ValueType::STRING)})
          .ok());

  auto res = store->create_edge(
      1, "works_at", 2, {{"unknown_field", Value(std::string("test"))}});
  ASSERT_FALSE(res.ok());
}

TEST_F(EdgePropertyTest, CreateEdgeWithTypeMismatch) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at", {std::make_shared<Field>("since", ValueType::INT64)})
          .ok());

  auto res = store->create_edge(
      1, "works_at", 2, {{"since", Value(std::string("not_a_number"))}});
  ASSERT_FALSE(res.ok());
}

TEST_F(EdgePropertyTest, CreateEdgeWithPropertiesNoSchema) {
  auto res = store->create_edge(1, "friend", 2, {{"weight", Value(1.0)}});
  ASSERT_FALSE(res.ok());
}

TEST_F(EdgePropertyTest, SchemaLessEdgeNoProperties) {
  auto edge_res = store->create_edge(1, "friend", 2);
  ASSERT_TRUE(edge_res.ok());
}

TEST_F(EdgePropertyTest, GetTableWithProperties) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at", {std::make_shared<Field>("role", ValueType::STRING),
                           std::make_shared<Field>("since", ValueType::INT64)})
          .ok());

  auto e1 = store->create_edge(1, "works_at", 10,
                               {{"role", Value(std::string("engineer"))},
                                {"since", Value(int64_t(2020))}});
  auto e2 = store->create_edge(2, "works_at", 11,
                               {{"role", Value(std::string("manager"))},
                                {"since", Value(int64_t(2018))}});
  auto e3 = store->create_edge(3, "works_at", 12,
                               {{"role", Value(std::string("intern"))}});

  ASSERT_TRUE(store->add(e1.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(e2.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(e3.ValueOrDie()).ok());

  auto table_res = store->get_table("works_at");
  ASSERT_TRUE(table_res.ok());
  auto table = table_res.ValueOrDie();

  // 4 structural + 2 property columns
  ASSERT_EQ(table->num_columns(), 6);
  ASSERT_EQ(table->num_rows(), 3);

  // Verify schema field names
  ASSERT_EQ(table->schema()->field(4)->name(), "role");
  ASSERT_EQ(table->schema()->field(5)->name(), "since");

  // Verify property values
  auto role_col =
      std::static_pointer_cast<arrow::StringArray>(table->column(4)->chunk(0));
  auto since_col =
      std::static_pointer_cast<arrow::Int64Array>(table->column(5)->chunk(0));

  ASSERT_EQ(role_col->GetString(0), "engineer");
  ASSERT_EQ(role_col->GetString(1), "manager");
  ASSERT_EQ(role_col->GetString(2), "intern");

  ASSERT_EQ(since_col->Value(0), 2020);
  ASSERT_EQ(since_col->Value(1), 2018);
  ASSERT_TRUE(since_col->IsNull(2));  // e3 didn't set "since"
}

TEST_F(EdgePropertyTest, MixedSchemaAndSchemaLessEdges) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at", {std::make_shared<Field>("role", ValueType::STRING)})
          .ok());

  auto typed_edge = store->create_edge(1, "works_at", 10,
                                       {{"role", Value(std::string("dev"))}});
  auto plain_edge = store->create_edge(1, "friend", 2);

  ASSERT_TRUE(store->add(typed_edge.ValueOrDie()).ok());
  ASSERT_TRUE(store->add(plain_edge.ValueOrDie()).ok());

  // works_at table has property columns
  auto works_at_table = store->get_table("works_at");
  ASSERT_TRUE(works_at_table.ok());
  ASSERT_EQ(works_at_table.ValueOrDie()->num_columns(), 5);

  // friend table has structural columns only (no schema fields)
  auto friend_table = store->get_table("friend");
  ASSERT_TRUE(friend_table.ok());
  ASSERT_EQ(friend_table.ValueOrDie()->num_columns(), 4);
}

TEST_F(EdgePropertyTest, NullableProperties) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "works_at",
              {std::make_shared<Field>("role", ValueType::STRING, true),
               std::make_shared<Field>("since", ValueType::INT64, true)})
          .ok());

  // Edge with null role
  auto e1 =
      store->create_edge(1, "works_at", 10, {{"since", Value(int64_t(2022))}});
  ASSERT_TRUE(store->add(e1.ValueOrDie()).ok());

  auto table_res = store->get_table("works_at");
  ASSERT_TRUE(table_res.ok());
  auto table = table_res.ValueOrDie();

  auto role_col =
      std::static_pointer_cast<arrow::StringArray>(table->column(4)->chunk(0));
  ASSERT_TRUE(role_col->IsNull(0));

  auto since_col =
      std::static_pointer_cast<arrow::Int64Array>(table->column(5)->chunk(0));
  ASSERT_EQ(since_col->Value(0), 2022);
}

TEST_F(EdgePropertyTest, AllPropertyTypes) {
  ASSERT_TRUE(
      store
          ->register_edge_schema(
              "typed_edge",
              {std::make_shared<Field>("str_field", ValueType::STRING),
               std::make_shared<Field>("int64_field", ValueType::INT64),
               std::make_shared<Field>("int32_field", ValueType::INT32),
               std::make_shared<Field>("double_field", ValueType::DOUBLE),
               std::make_shared<Field>("bool_field", ValueType::BOOL)})
          .ok());

  auto edge = store->create_edge(1, "typed_edge", 2,
                                 {{"str_field", Value(std::string("hello"))},
                                  {"int64_field", Value(int64_t(42))},
                                  {"int32_field", Value(int32_t(7))},
                                  {"double_field", Value(3.14)},
                                  {"bool_field", Value(true)}});
  ASSERT_TRUE(store->add(edge.ValueOrDie()).ok());

  auto table_res = store->get_table("typed_edge");
  ASSERT_TRUE(table_res.ok());
  auto table = table_res.ValueOrDie();

  ASSERT_EQ(table->num_columns(),
            9);  // 4 structural + 5 property

  auto str_col =
      std::static_pointer_cast<arrow::StringArray>(table->column(4)->chunk(0));
  auto i64_col =
      std::static_pointer_cast<arrow::Int64Array>(table->column(5)->chunk(0));
  auto i32_col =
      std::static_pointer_cast<arrow::Int32Array>(table->column(6)->chunk(0));
  auto dbl_col =
      std::static_pointer_cast<arrow::DoubleArray>(table->column(7)->chunk(0));
  auto bool_col =
      std::static_pointer_cast<arrow::BooleanArray>(table->column(8)->chunk(0));

  ASSERT_EQ(str_col->GetString(0), "hello");
  ASSERT_EQ(i64_col->Value(0), 42);
  ASSERT_EQ(i32_col->Value(0), 7);
  ASSERT_DOUBLE_EQ(dbl_col->Value(0), 3.14);
  ASSERT_TRUE(bool_col->Value(0));
}

}  // namespace tundradb
