#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "arrow/map_union_types.hpp"
#include "common/utils.hpp"
#include "main/database.hpp"
#include "query/query.hpp"

#define ASSERT_OK(expr) ASSERT_TRUE((expr).ok())

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {

class UpdateQueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto user_schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("city", arrow::utf8()),
        arrow::field("salary", arrow::int32()),
    });

    auto company_schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("size", arrow::int32()),
    });

    auto db_path = "update_query_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()->create("User", user_schema).ValueOrDie();
    db_->get_schema_registry()->create("Company", company_schema).ValueOrDie();

    create_test_data();
  }

  void create_test_data() {
    struct UserData {
      std::string name;
      int32_t age;
      std::string city;
      int32_t salary;
    };

    std::vector<UserData> users = {
        {"Alice", 25, "NYC", 80000},    // id: 0
        {"Bob", 35, "NYC", 120000},     // id: 1
        {"Charlie", 45, "SF", 150000},  // id: 2
        {"Diana", 30, "LA", 60000},     // id: 3
        {"Eve", 55, "NYC", 200000},     // id: 4
    };

    for (const auto& u : users) {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{u.name}},
          {"age", Value{u.age}},
          {"city", Value{u.city}},
          {"salary", Value{u.salary}},
      };
      db_->create_node("User", data).ValueOrDie();
    }

    std::unordered_map<std::string, Value> company_data = {
        {"name", Value{"TechCorp"s}},
        {"size", Value{int32_t(5000)}},
    };
    db_->create_node("Company", company_data).ValueOrDie();

    // Create work relationships: Alice and Bob work at TechCorp
    db_->connect(0, "WORKS_AT", 0).ValueOrDie();  // Alice → TechCorp
    db_->connect(1, "WORKS_AT", 0).ValueOrDie();  // Bob   → TechCorp
  }

  /// Query a single node by ID and return its field value.
  template <typename T>
  T get_field(const std::string& schema, int64_t id,
              const std::string& field_name) {
    const std::string alias = "_";
    auto query = Query::match(alias + ":" + schema).build();
    auto result = db_->query(query).ValueOrDie();
    auto table = result->table();
    auto ids = get_column_values<int64_t>(table, alias + ".id").ValueOrDie();
    auto vals =
        get_column_values<T>(table, alias + "." + field_name).ValueOrDie();
    for (size_t i = 0; i < ids.size(); ++i) {
      if (ids[i] == id) return vals[i];
    }
    throw std::runtime_error("Node not found: " + schema + "(" +
                             std::to_string(id) + ")");
  }

  std::shared_ptr<Database> db_;
};

// =========================================================================
// Builder API tests — Mode 1 (by ID)
// =========================================================================

TEST_F(UpdateQueryTest, BuilderOnRequiresAtLeastOneSet) {
  EXPECT_THROW((UpdateQuery::on("User", 0).build()), std::runtime_error);
}

TEST_F(UpdateQueryTest, BuilderStoresSchema) {
  auto uq = UpdateQuery::on("User", 0).set("age", Value(31)).build();
  EXPECT_EQ(uq.schema(), "User");
}

TEST_F(UpdateQueryTest, BuilderStoresNodeId) {
  auto uq = UpdateQuery::on("User", 42).set("age", Value(31)).build();
  ASSERT_TRUE(uq.node_id().has_value());
  EXPECT_EQ(uq.node_id().value(), 42);
}

TEST_F(UpdateQueryTest, BuilderStoresMultipleAssignments) {
  auto uq = UpdateQuery::on("User", 0)
                .set("age", Value(31))
                .set("name", Value("Bob"s))
                .build();
  EXPECT_EQ(uq.assignments().size(), 2);
  EXPECT_EQ(uq.assignments()[0].field_name, "age");
  EXPECT_EQ(uq.assignments()[1].field_name, "name");
}

TEST_F(UpdateQueryTest, BuilderDefaultUpdateTypeIsSET) {
  auto uq = UpdateQuery::on("User", 0).set("age", Value(31)).build();
  EXPECT_EQ(uq.update_type(), UpdateType::SET);
}

// =========================================================================
// Builder API tests — Mode 2 (by MATCH)
// =========================================================================

TEST_F(UpdateQueryTest, MatchRequiresAtLeastOneSet) {
  auto q = Query::match("u:User").build();
  EXPECT_THROW((UpdateQuery::match(q).build()), std::runtime_error);
}

TEST_F(UpdateQueryTest, MatchStoresQuery) {
  auto q = Query::match("u:User")
               .where("u.city", CompareOp::Eq, Value("NYC"s))
               .build();
  auto uq = UpdateQuery::match(q).set("u.age", Value(31)).build();
  EXPECT_TRUE(uq.has_match());
  EXPECT_FALSE(uq.node_id().has_value());
}

TEST_F(UpdateQueryTest, MatchTargetAliasesFromSetFields) {
  auto q =
      Query::match("u:User").traverse("u", "WORKS_AT", "c:Company").build();
  auto uq = UpdateQuery::match(q)
                .set("u.salary", Value(int32_t(0)))
                .set("c.size", Value(int32_t(9)))
                .build();
  auto aliases = uq.target_aliases();
  EXPECT_EQ(aliases.size(), 2);
  EXPECT_NE(std::find(aliases.begin(), aliases.end(), "u"), aliases.end());
  EXPECT_NE(std::find(aliases.begin(), aliases.end(), "c"), aliases.end());
}

// =========================================================================
// Database::update() — Mode 1 (by ID)
// =========================================================================

TEST_F(UpdateQueryTest, UpdateByIdSingleField) {
  auto uq = UpdateQuery::on("User", 0).set("age", Value(int32_t(99))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.updated_count, 1);
  EXPECT_EQ(ur.failed_count, 0);
  EXPECT_TRUE(ur.errors.empty());

  EXPECT_EQ(get_field<int32_t>("User", 0, "age"), 99);
}

TEST_F(UpdateQueryTest, UpdateByIdMultipleFields) {
  auto uq = UpdateQuery::on("User", 1)
                .set("age", Value(int32_t(40)))
                .set("city", Value("LA"s))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.updated_count, 1);
  EXPECT_EQ(ur.failed_count, 0);

  EXPECT_EQ(get_field<int32_t>("User", 1, "age"), 40);
  EXPECT_EQ(get_field<std::string>("User", 1, "city"), "LA");
}

TEST_F(UpdateQueryTest, UpdateByIdStringField) {
  auto uq = UpdateQuery::on("User", 0).set("name", Value("Alicia"s)).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  EXPECT_EQ(get_field<std::string>("User", 0, "name"), "Alicia");
}

TEST_F(UpdateQueryTest, UpdateByIdNonexistentNode) {
  auto uq = UpdateQuery::on("User", 999).set("age", Value(int32_t(99))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.updated_count, 0);
  EXPECT_EQ(ur.failed_count, 1);
  EXPECT_FALSE(ur.errors.empty());
}

TEST_F(UpdateQueryTest, UpdateByIdInvalidField) {
  auto uq = UpdateQuery::on("User", 0)
                .set("nonexistent_field", Value(int32_t(1)))
                .build();

  auto result = db_->update(uq);
  EXPECT_FALSE(result.ok());
}

TEST_F(UpdateQueryTest, UpdateByIdInvalidSchema) {
  auto uq =
      UpdateQuery::on("NoSuchSchema", 0).set("age", Value(int32_t(1))).build();

  auto result = db_->update(uq);
  EXPECT_FALSE(result.ok());
}

// =========================================================================
// Database::update() — Mode 2 (single alias)
// =========================================================================

TEST_F(UpdateQueryTest, UpdateByMatchSimpleWhere) {
  // All NYC users: Alice(0), Bob(1), Eve(4)
  auto q = Query::match("u:User")
               .where("u.city", CompareOp::Eq, Value("NYC"s))
               .build();
  auto uq =
      UpdateQuery::match(q).set("u.salary", Value(int32_t(999999))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.updated_count, 3);
  EXPECT_EQ(ur.failed_count, 0);

  EXPECT_EQ(get_field<int32_t>("User", 0, "salary"), 999999);  // Alice
  EXPECT_EQ(get_field<int32_t>("User", 1, "salary"), 999999);  // Bob
  EXPECT_EQ(get_field<int32_t>("User", 4, "salary"), 999999);  // Eve

  // Non-NYC users unchanged
  EXPECT_EQ(get_field<int32_t>("User", 2, "salary"), 150000);  // Charlie
  EXPECT_EQ(get_field<int32_t>("User", 3, "salary"), 60000);   // Diana
}

TEST_F(UpdateQueryTest, UpdateByMatchSingleResult) {
  auto q = Query::match("u:User")
               .where("u.name", CompareOp::Eq, Value("Alice"s))
               .build();
  auto uq = UpdateQuery::match(q).set("u.age", Value(int32_t(26))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  EXPECT_EQ(get_field<int32_t>("User", 0, "age"), 26);
}

TEST_F(UpdateQueryTest, UpdateByMatchNoResults) {
  auto q = Query::match("u:User")
               .where("u.name", CompareOp::Eq, Value("Nobody"s))
               .build();
  auto uq = UpdateQuery::match(q).set("u.age", Value(int32_t(0))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 0);
}

TEST_F(UpdateQueryTest, UpdateByMatchCompoundAnd) {
  // age > 30 AND city = "NYC" → Bob(35,NYC), Eve(55,NYC)
  auto q = Query::match("u:User")
               .where("u.age", CompareOp::Gt, Value(int32_t(30)))
               .and_where("u.city", CompareOp::Eq, Value("NYC"s))
               .build();
  auto uq = UpdateQuery::match(q).set("u.salary", Value(int32_t(0))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 2);

  EXPECT_EQ(get_field<int32_t>("User", 1, "salary"), 0);      // Bob
  EXPECT_EQ(get_field<int32_t>("User", 4, "salary"), 0);      // Eve
  EXPECT_EQ(get_field<int32_t>("User", 0, "salary"), 80000);  // Alice unchanged
}

TEST_F(UpdateQueryTest, UpdateByMatchMultipleSetFields) {
  auto q = Query::match("u:User")
               .where("u.name", CompareOp::Eq, Value("Alice"s))
               .build();
  auto uq = UpdateQuery::match(q)
                .set("u.age", Value(int32_t(26)))
                .set("u.city", Value("SF"s))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  EXPECT_EQ(get_field<int32_t>("User", 0, "age"), 26);
  EXPECT_EQ(get_field<std::string>("User", 0, "city"), "SF");
}

// =========================================================================
// Database::update() — Mode 2 with traversal
// =========================================================================

TEST_F(UpdateQueryTest, UpdateByMatchWithTraversal) {
  // Update users who work at TechCorp: Alice(0), Bob(1)
  auto q = Query::match("u:User")
               .traverse("u", "WORKS_AT", "c:Company")
               .where("c.name", CompareOp::Eq, Value("TechCorp"s))
               .build();
  auto uq = UpdateQuery::match(q).set("u.salary", Value(int32_t(777))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.updated_count, 2);

  EXPECT_EQ(get_field<int32_t>("User", 0, "salary"), 777);  // Alice
  EXPECT_EQ(get_field<int32_t>("User", 1, "salary"), 777);  // Bob
  EXPECT_EQ(get_field<int32_t>("User", 2, "salary"),
            150000);  // Charlie unchanged
}

// =========================================================================
// Database::update() — Mode 2 multi-schema (update both sides)
// =========================================================================

TEST_F(UpdateQueryTest, UpdateMultiSchemaViaTraversal) {
  // UPDATE users who work at TechCorp AND update TechCorp itself
  auto q = Query::match("u:User")
               .traverse("u", "WORKS_AT", "c:Company")
               .where("c.name", CompareOp::Eq, Value("TechCorp"s))
               .build();
  auto uq = UpdateQuery::match(q)
                .set("u.salary", Value(int32_t(111)))
                .set("c.size", Value(int32_t(9999)))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  // 2 user nodes + 1 company node (appears in each row but same ID)
  EXPECT_GE(ur.updated_count, 2);  // at least u(0) and u(1)
  EXPECT_EQ(ur.failed_count, 0);

  EXPECT_EQ(get_field<int32_t>("User", 0, "salary"), 111);    // Alice
  EXPECT_EQ(get_field<int32_t>("User", 1, "salary"), 111);    // Bob
  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 9999);  // TechCorp
}

// =========================================================================
// Database::update() — Mode 2 bad alias in SET
// =========================================================================

TEST_F(UpdateQueryTest, UpdateByMatchBadAliasInSet) {
  auto q = Query::match("u:User").build();
  auto uq = UpdateQuery::match(q)
                .set("x.salary", Value(int32_t(0)))  // "x" not in MATCH
                .build();

  auto result = db_->update(uq);
  EXPECT_FALSE(result.ok());
}

TEST_F(UpdateQueryTest, UpdateByMatchUnqualifiedFieldFails) {
  auto q = Query::match("u:User").build();
  auto uq = UpdateQuery::match(q)
                .set("salary", Value(int32_t(0)))  // missing alias
                .build();

  auto result = db_->update(uq);
  EXPECT_FALSE(result.ok());
}

// =========================================================================
// Different schema — Mode 1
// =========================================================================

TEST_F(UpdateQueryTest, UpdateCompanyById) {
  auto uq =
      UpdateQuery::on("Company", 0).set("size", Value(int32_t(9999))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 9999);
}

// =========================================================================
// Verify original values are untouched
// =========================================================================

TEST_F(UpdateQueryTest, UpdateDoesNotAffectOtherFields) {
  auto uq = UpdateQuery::on("User", 0).set("age", Value(int32_t(99))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  EXPECT_EQ(get_field<int32_t>("User", 0, "age"), 99);
  EXPECT_EQ(get_field<std::string>("User", 0, "name"), "Alice");
  EXPECT_EQ(get_field<std::string>("User", 0, "city"), "NYC");
  EXPECT_EQ(get_field<int32_t>("User", 0, "salary"), 80000);
}

TEST_F(UpdateQueryTest, UpdateDoesNotAffectOtherNodes) {
  auto uq = UpdateQuery::on("User", 0).set("age", Value(int32_t(99))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  EXPECT_EQ(get_field<int32_t>("User", 1, "age"), 35);
  EXPECT_EQ(get_field<int32_t>("User", 2, "age"), 45);
  EXPECT_EQ(get_field<int32_t>("User", 3, "age"), 30);
  EXPECT_EQ(get_field<int32_t>("User", 4, "age"), 55);
}

// =========================================================================
// Sequential updates
// =========================================================================

TEST_F(UpdateQueryTest, SequentialUpdatesAccumulate) {
  ASSERT_OK(db_->update(
      UpdateQuery::on("User", 0).set("age", Value(int32_t(50))).build()));
  ASSERT_OK(db_->update(
      UpdateQuery::on("User", 0).set("age", Value(int32_t(60))).build()));

  EXPECT_EQ(get_field<int32_t>("User", 0, "age"), 60);
}

TEST_F(UpdateQueryTest, UpdateByMatchSupportsMapKeySet) {
  auto map_value_type = map_union_value_type();
  auto map_schema = arrow::schema({
      arrow::field("name", arrow::utf8()),
      arrow::field("props", arrow::map(arrow::utf8(), map_value_type)),
  });
  db_->get_schema_registry()->create("MapUser", map_schema).ValueOrDie();
  db_->create_node("MapUser", {{"name", Value{"Nina"}}}).ValueOrDie();
  db_->create_node("MapUser", {{"name", Value{"Omar"}}}).ValueOrDie();

  auto q = Query::match("m:MapUser")
               .where("m.name", CompareOp::Eq, Value("Nina"s))
               .build();
  auto uq =
      UpdateQuery::match(q).set("m.props.score", Value(int32_t(99))).build();

  auto update_res = db_->update(uq);
  ASSERT_OK(update_res);
  EXPECT_EQ(update_res.ValueOrDie().updated_count, 1);

  auto props_field =
      db_->get_schema_registry()->get("MapUser").ValueOrDie()->get_field(
          "props");
  ASSERT_NE(props_field, nullptr);

  auto nina = db_->get_node_manager()->get_node("MapUser", 0).ValueOrDie();
  auto omar = db_->get_node_manager()->get_node("MapUser", 1).ValueOrDie();

  auto nina_props = nina->get_value(props_field).ValueOrDie();
  ASSERT_EQ(nina_props.type(), ValueType::MAP);
  EXPECT_EQ(nina_props.as_map_ref().get_value("score").get<int32_t>(), 99);

  auto omar_props = omar->get_value(props_field).ValueOrDie();
  ASSERT_TRUE(omar_props.is_null() ||
              !omar_props.as_map_ref().contains("score"));
}

TEST_F(UpdateQueryTest,
       UpdateByMatchNestedPathDepthGreaterThanOneNotImplemented) {
  auto map_value_type = map_union_value_type();
  auto map_schema = arrow::schema({
      arrow::field("name", arrow::utf8()),
      arrow::field("props", arrow::map(arrow::utf8(), map_value_type)),
  });
  db_->get_schema_registry()->create("MapUserDepth", map_schema).ValueOrDie();
  db_->create_node("MapUserDepth", {{"name", Value{"Nina"}}}).ValueOrDie();

  auto q = Query::match("m:MapUserDepth")
               .where("m.name", CompareOp::Eq, Value("Nina"s))
               .build();
  auto uq = UpdateQuery::match(q)
                .set("m.props.level1.level2", Value(int32_t(123)))
                .build();

  auto update_res = db_->update(uq);
  ASSERT_OK(update_res);
  const auto result = update_res.ValueOrDie();
  EXPECT_EQ(result.updated_count, 0);
  EXPECT_EQ(result.failed_count, 1);
  ASSERT_FALSE(result.errors.empty());
  EXPECT_NE(result.errors[0].find("NotImplemented"), std::string::npos);
}

}  // namespace tundradb
