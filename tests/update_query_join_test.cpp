#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../include/core.hpp"
#include "../include/query.hpp"
#include "../include/utils.hpp"

#define ASSERT_OK(expr) ASSERT_TRUE((expr).ok())

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {

// =========================================================================
// Fixture: cross-schema graph  (User --WORKS_AT--> Company)
// =========================================================================

class UpdateJoinCrossSchemaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto user_schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("employed", arrow::boolean()),
    });

    auto company_schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("size", arrow::int32()),
    });

    auto db_path = "update_join_cross_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()->create("User", user_schema).ValueOrDie();
    db_->get_schema_registry()->create("Company", company_schema).ValueOrDie();

    // Users:  Alice(0), Bob(1), Charlie(2)
    // All start with employed = false
    for (const auto& [name, age] : std::vector<std::pair<std::string, int32_t>>{
             {"Alice", 30}, {"Bob", 25}, {"Charlie", 40}}) {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{name}},
          {"age", Value{age}},
          {"employed", Value{false}},
      };
      db_->create_node("User", data).ValueOrDie();
    }

    // Companies:  Acme(0)  size=0,  Globex(1)  size=0
    for (const auto& name : {"Acme"s, "Globex"s}) {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{name}},
          {"size", Value{int32_t(0)}},
      };
      db_->create_node("Company", data).ValueOrDie();
    }

    // Edges: Alice(0) --WORKS_AT--> Acme(0)
    //        Bob(1)   --WORKS_AT--> Acme(0)
    db_->connect(0, "WORKS_AT", 0).ValueOrDie();
    db_->connect(1, "WORKS_AT", 0).ValueOrDie();
  }

  template <typename T>
  T get_field(const std::string& schema, int64_t id,
              const std::string& field_name) {
    auto query = Query::from("_:" + schema).build();
    auto result = db_->query(query).ValueOrDie();
    auto table = result->table();
    auto ids = get_column_values<int64_t>(table, "_.id").ValueOrDie();
    auto vals = get_column_values<T>(table, "_." + field_name).ValueOrDie();
    for (size_t i = 0; i < ids.size(); ++i) {
      if (ids[i] == id) return vals[i];
    }
    throw std::runtime_error("Node not found: " + schema + "(" +
                             std::to_string(id) + ")");
  }

  std::shared_ptr<Database> db_;
};

// -------------------------------------------------------------------------
// Cross-schema: set User.employed=true AND Company.size=1
// -------------------------------------------------------------------------

TEST_F(UpdateJoinCrossSchemaTest, UpdateBothSidesOfTraversal) {
  // Preconditions
  EXPECT_EQ(get_field<bool>("User", 0, "employed"), false);  // Alice
  EXPECT_EQ(get_field<bool>("User", 1, "employed"), false);  // Bob
  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 0);    // Acme

  // MATCH (u:User)-[:WORKS_AT]->(c:Company)
  //   WHERE c.name = "Acme"
  //   SET u.employed = true, c.size = 1
  auto q = Query::from("u:User")
               .traverse("u", "WORKS_AT", "c:Company")
               .where("c.name", CompareOp::Eq, Value("Acme"s))
               .build();
  auto uq = UpdateQuery::match(q)
                .set("u.employed", Value(true))
                .set("c.size", Value(int32_t(1)))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.failed_count, 0);
  EXPECT_TRUE(ur.errors.empty());

  // Users who work at Acme are now employed
  EXPECT_EQ(get_field<bool>("User", 0, "employed"), true);  // Alice ✓
  EXPECT_EQ(get_field<bool>("User", 1, "employed"), true);  // Bob   ✓

  // Charlie never matched — unchanged
  EXPECT_EQ(get_field<bool>("User", 2, "employed"), false);

  // Acme's size was updated
  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 1);

  // Globex was not part of the traversal — unchanged
  EXPECT_EQ(get_field<int32_t>("Company", 1, "size"), 0);
}

TEST_F(UpdateJoinCrossSchemaTest, UpdateOnlyUserSide) {
  // Only update User.employed, leave Company untouched
  auto q = Query::from("u:User").traverse("u", "WORKS_AT", "c:Company").build();
  auto uq = UpdateQuery::match(q).set("u.employed", Value(true)).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().failed_count, 0);

  EXPECT_EQ(get_field<bool>("User", 0, "employed"), true);
  EXPECT_EQ(get_field<bool>("User", 1, "employed"), true);
  EXPECT_EQ(get_field<bool>("User", 2, "employed"), false);  // no edge

  // Company unchanged
  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 0);
}

TEST_F(UpdateJoinCrossSchemaTest, UpdateOnlyCompanySide) {
  // Only update Company.size, leave User untouched
  auto q = Query::from("u:User")
               .traverse("u", "WORKS_AT", "c:Company")
               .where("c.name", CompareOp::Eq, Value("Acme"s))
               .build();
  auto uq = UpdateQuery::match(q).set("c.size", Value(int32_t(42))).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().failed_count, 0);

  // Users unchanged
  EXPECT_EQ(get_field<bool>("User", 0, "employed"), false);
  EXPECT_EQ(get_field<bool>("User", 1, "employed"), false);

  // Acme updated
  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 42);
}

TEST_F(UpdateJoinCrossSchemaTest, TraversalWithNoMatchUpdatesNothing) {
  // WHERE c.name = "NonExistent" → no rows
  auto q = Query::from("u:User")
               .traverse("u", "WORKS_AT", "c:Company")
               .where("c.name", CompareOp::Eq, Value("NonExistent"s))
               .build();
  auto uq = UpdateQuery::match(q)
                .set("u.employed", Value(true))
                .set("c.size", Value(int32_t(999)))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 0);

  // Everything unchanged
  EXPECT_EQ(get_field<bool>("User", 0, "employed"), false);
  EXPECT_EQ(get_field<int32_t>("Company", 0, "size"), 0);
}

// =========================================================================
// Fixture: same-schema graph  (User --FRIEND--> User)
// =========================================================================

class UpdateJoinSameSchemaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto user_schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("has_friend", arrow::boolean()),
    });

    auto db_path = "update_join_same_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()->create("User", user_schema).ValueOrDie();

    // Users:  Alice(0), Bob(1), Charlie(2), Diana(3)
    // All start with has_friend = false
    for (const auto& name : {"Alice"s, "Bob"s, "Charlie"s, "Diana"s}) {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{name}},
          {"has_friend", Value{false}},
      };
      db_->create_node("User", data).ValueOrDie();
    }

    // Edges (directed):
    //   Alice(0)  --FRIEND--> Bob(1)
    //   Alice(0)  --FRIEND--> Charlie(2)
    db_->connect(0, "FRIEND", 1).ValueOrDie();
    db_->connect(0, "FRIEND", 2).ValueOrDie();
    // Diana(3) has no friends
  }

  template <typename T>
  T get_field(const std::string& schema, int64_t id,
              const std::string& field_name) {
    auto query = Query::from("_:" + schema).build();
    auto result = db_->query(query).ValueOrDie();
    auto table = result->table();
    auto ids = get_column_values<int64_t>(table, "_.id").ValueOrDie();
    auto vals = get_column_values<T>(table, "_." + field_name).ValueOrDie();
    for (size_t i = 0; i < ids.size(); ++i) {
      if (ids[i] == id) return vals[i];
    }
    throw std::runtime_error("Node not found: " + schema + "(" +
                             std::to_string(id) + ")");
  }

  std::shared_ptr<Database> db_;
};

// -------------------------------------------------------------------------
// Same-schema: set has_friend=true on both sides of the friendship
// -------------------------------------------------------------------------

TEST_F(UpdateJoinSameSchemaTest, UpdateBothSidesOfFriendship) {
  // Preconditions
  EXPECT_EQ(get_field<bool>("User", 0, "has_friend"), false);
  EXPECT_EQ(get_field<bool>("User", 1, "has_friend"), false);
  EXPECT_EQ(get_field<bool>("User", 2, "has_friend"), false);
  EXPECT_EQ(get_field<bool>("User", 3, "has_friend"), false);

  // MATCH (u:User)-[:FRIEND]->(f:User)
  //   SET u.has_friend = true, f.has_friend = true
  auto q = Query::from("u:User").traverse("u", "FRIEND", "f:User").build();
  auto uq = UpdateQuery::match(q)
                .set("u.has_friend", Value(true))
                .set("f.has_friend", Value(true))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto ur = result.ValueOrDie();
  EXPECT_EQ(ur.failed_count, 0);

  // Alice is the source of both edges → updated via "u"
  EXPECT_EQ(get_field<bool>("User", 0, "has_friend"), true);

  // Bob and Charlie are targets → updated via "f"
  EXPECT_EQ(get_field<bool>("User", 1, "has_friend"), true);
  EXPECT_EQ(get_field<bool>("User", 2, "has_friend"), true);

  // Diana has no edges — unchanged
  EXPECT_EQ(get_field<bool>("User", 3, "has_friend"), false);
}

TEST_F(UpdateJoinSameSchemaTest, UpdateOnlySourceSide) {
  // Only update the source alias "u"
  auto q = Query::from("u:User").traverse("u", "FRIEND", "f:User").build();
  auto uq = UpdateQuery::match(q).set("u.has_friend", Value(true)).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().failed_count, 0);

  // Only Alice is the source
  EXPECT_EQ(get_field<bool>("User", 0, "has_friend"), true);

  // Bob and Charlie are only targets — not updated
  EXPECT_EQ(get_field<bool>("User", 1, "has_friend"), false);
  EXPECT_EQ(get_field<bool>("User", 2, "has_friend"), false);
}

TEST_F(UpdateJoinSameSchemaTest, UpdateOnlyTargetSide) {
  // Only update the target alias "f"
  auto q = Query::from("u:User").traverse("u", "FRIEND", "f:User").build();
  auto uq = UpdateQuery::match(q).set("f.has_friend", Value(true)).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().failed_count, 0);

  // Alice is source only — not updated via "f"
  EXPECT_EQ(get_field<bool>("User", 0, "has_friend"), false);

  // Bob and Charlie are targets
  EXPECT_EQ(get_field<bool>("User", 1, "has_friend"), true);
  EXPECT_EQ(get_field<bool>("User", 2, "has_friend"), true);

  // Diana untouched
  EXPECT_EQ(get_field<bool>("User", 3, "has_friend"), false);
}

TEST_F(UpdateJoinSameSchemaTest, UpdateWithWhereOnTarget) {
  // Only update friends named "Bob"
  auto q = Query::from("u:User")
               .traverse("u", "FRIEND", "f:User")
               .where("f.name", CompareOp::Eq, Value("Bob"s))
               .build();
  auto uq = UpdateQuery::match(q)
                .set("u.has_friend", Value(true))
                .set("f.has_friend", Value(true))
                .build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().failed_count, 0);

  // Alice → Bob matched
  EXPECT_EQ(get_field<bool>("User", 0, "has_friend"), true);  // Alice (source)
  EXPECT_EQ(get_field<bool>("User", 1, "has_friend"), true);  // Bob   (target)

  // Alice → Charlie did NOT match (WHERE f.name = "Bob")
  EXPECT_EQ(get_field<bool>("User", 2, "has_friend"), false);  // Charlie

  // Diana untouched
  EXPECT_EQ(get_field<bool>("User", 3, "has_friend"), false);
}

}  // namespace tundradb
