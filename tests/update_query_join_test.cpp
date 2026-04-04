#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "../include/core.hpp"
#include "../include/query.hpp"
#include "common/utils.hpp"

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
    db_->register_edge_schema(
           "WORKS_AT", {std::make_shared<Field>("since", ValueType::INT64),
                        std::make_shared<Field>("role", ValueType::STRING)})
        .ValueOrDie();

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
    db_->connect(0, "WORKS_AT", 0,
                 {{"since", Value{int64_t(2020)}}, {"role", Value{"eng"s}}})
        .ValueOrDie();
    db_->connect(1, "WORKS_AT", 0,
                 {{"since", Value{int64_t(2021)}}, {"role", Value{"pm"s}}})
        .ValueOrDie();
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
  ASSERT_TRUE(result.ok()) << result.status().ToString();

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

TEST_F(UpdateJoinCrossSchemaTest, UpdateWithEdgeAliasTraversal) {
  auto q = Query::from("u:User")
               .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner,
                         std::optional<std::string>{"e"})
               .where("c.name", CompareOp::Eq, Value("Acme"s))
               .build();
  auto uq = UpdateQuery::match(q).set("u.employed", Value(true)).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().failed_count, 0);

  EXPECT_EQ(get_field<bool>("User", 0, "employed"), true);
  EXPECT_EQ(get_field<bool>("User", 1, "employed"), true);
  EXPECT_EQ(get_field<bool>("User", 2, "employed"), false);
}

TEST_F(UpdateJoinCrossSchemaTest, FilterByEdgeFieldAndSelectEdgeFields) {
  auto query = Query::from("u:User")
                   .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner,
                             std::optional<std::string>{"e"})
                   .where("e.since", CompareOp::Gte, Value(int64_t(2021)))
                   .select({"u.name", "e.since", "e.role", "c.name"})
                   .build();
  auto result = db_->query(query);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto table = result.ValueOrDie()->table();

  auto names = get_column_values<std::string>(table, "u.name").ValueOrDie();
  auto since = get_column_values<int64_t>(table, "e.since").ValueOrDie();
  auto role = get_column_values<std::string>(table, "e.role").ValueOrDie();
  auto company = get_column_values<std::string>(table, "c.name").ValueOrDie();

  ASSERT_EQ(names.size(), 1u);
  EXPECT_EQ(names[0], "Bob");
  EXPECT_EQ(since[0], 2021);
  EXPECT_EQ(role[0], "pm");
  EXPECT_EQ(company[0], "Acme");
}

TEST_F(UpdateJoinCrossSchemaTest, UpdateEdgeFieldByMatchAlias) {
  auto q = Query::from("u:User")
               .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner,
                         std::optional<std::string>{"e"})
               .where("u.name", CompareOp::Eq, Value("Alice"s))
               .build();
  auto uq = UpdateQuery::match(q).set("e.since", Value(int64_t(2025))).build();

  auto update_res = db_->update(uq);
  ASSERT_TRUE(update_res.ok()) << update_res.status().ToString();
  EXPECT_EQ(update_res.ValueOrDie().failed_count, 0);
  EXPECT_EQ(update_res.ValueOrDie().updated_count, 1);

  auto verify = Query::from("u:User")
                    .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner,
                              std::optional<std::string>{"e"})
                    .where("u.name", CompareOp::Eq, Value("Alice"s))
                    .select({"e.since"})
                    .build();
  auto table = db_->query(verify).ValueOrDie()->table();
  auto vals = get_column_values<int64_t>(table, "e.since").ValueOrDie();
  ASSERT_EQ(vals.size(), 1u);
  EXPECT_EQ(vals[0], 2025);
}

TEST_F(UpdateJoinCrossSchemaTest, SelectEdgeAliasExpandsAllEdgeFields) {
  auto query = Query::from("u:User")
                   .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner,
                             std::optional<std::string>{"e"})
                   .select({"e"})
                   .build();
  auto result = db_->query(query);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto table = result.ValueOrDie()->table();

  ASSERT_NE(table->GetColumnByName("e._edge_id"), nullptr);
  ASSERT_NE(table->GetColumnByName("e.source_id"), nullptr);
  ASSERT_NE(table->GetColumnByName("e.target_id"), nullptr);
  ASSERT_NE(table->GetColumnByName("e.created_ts"), nullptr);
  ASSERT_NE(table->GetColumnByName("e.since"), nullptr);
  ASSERT_NE(table->GetColumnByName("e.role"), nullptr);

  auto edge_ids = get_column_values<int64_t>(table, "e._edge_id").ValueOrDie();
  auto src_ids = get_column_values<int64_t>(table, "e.source_id").ValueOrDie();
  auto dst_ids = get_column_values<int64_t>(table, "e.target_id").ValueOrDie();
  auto since = get_column_values<int64_t>(table, "e.since").ValueOrDie();
  auto role = get_column_values<std::string>(table, "e.role").ValueOrDie();

  ASSERT_EQ(edge_ids.size(), 2u);
  ASSERT_EQ(src_ids.size(), 2u);
  ASSERT_EQ(dst_ids.size(), 2u);
  ASSERT_EQ(since.size(), 2u);
  ASSERT_EQ(role.size(), 2u);

  std::set<int64_t> src_set(src_ids.begin(), src_ids.end());
  std::set<int64_t> dst_set(dst_ids.begin(), dst_ids.end());
  std::set<int64_t> since_set(since.begin(), since.end());
  std::set<std::string> role_set(role.begin(), role.end());
  EXPECT_EQ(src_set, (std::set<int64_t>{0, 1}));
  EXPECT_EQ(dst_set, (std::set<int64_t>{0}));
  EXPECT_EQ(since_set, (std::set<int64_t>{2020, 2021}));
  EXPECT_EQ(role_set, (std::set<std::string>{"eng", "pm"}));
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
