#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logger.hpp"
#include "main/database.hpp"
#include "query/query.hpp"
#include "storage/metadata.hpp"

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {

namespace {

std::shared_ptr<arrow::Schema> create_users_schema() {
  return arrow::schema({arrow::field("name", arrow::utf8()),
                        arrow::field("age", arrow::int64())});
}

std::shared_ptr<arrow::Schema> create_companies_schema() {
  return arrow::schema({arrow::field("name", arrow::utf8()),
                        arrow::field("size", arrow::int64())});
}

std::string scalar_to_test_string(
    const std::shared_ptr<arrow::Scalar>& scalar) {
  if (!scalar || !scalar->is_valid) {
    return "NULL";
  }
  return scalar->ToString();
}

std::string table_to_test_string(const std::shared_ptr<arrow::Table>& table) {
  std::vector<std::string> column_names;
  column_names.reserve(table->schema()->num_fields());
  for (const auto& field : table->schema()->fields()) {
    column_names.push_back(field->name());
  }

  std::vector<std::string> rows;
  rows.reserve(table->num_rows());
  for (int64_t row_index = 0; row_index < table->num_rows(); ++row_index) {
    std::ostringstream row;
    for (size_t column_index = 0; column_index < column_names.size();
         ++column_index) {
      if (column_index > 0) {
        row << " | ";
      }

      auto column = table->GetColumnByName(column_names[column_index]);
      auto scalar_result = column->GetScalar(row_index);
      EXPECT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      if (!scalar_result.ok()) {
        row << "<error>";
        continue;
      }
      row << scalar_to_test_string(scalar_result.ValueOrDie());
    }
    rows.push_back(row.str());
  }

  std::sort(rows.begin(), rows.end());

  std::ostringstream out;
  for (size_t i = 0; i < column_names.size(); ++i) {
    if (i > 0) {
      out << " | ";
    }
    out << column_names[i];
  }
  for (const auto& row : rows) {
    out << '\n' << row;
  }
  return out.str();
}

void create_user(const std::shared_ptr<Database>& db, const std::string& name,
                 int64_t age) {
  db->create_node("users", {{"name", Value{name}}, {"age", Value{age}}})
      .ValueOrDie();
}

void create_company(const std::shared_ptr<Database>& db,
                    const std::string& name, int64_t size) {
  db->create_node("companies", {{"name", Value{name}}, {"size", Value{size}}})
      .ValueOrDie();
}

}  // namespace

class JoinWhereTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto db_path = "join_where_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()
        ->create("users", create_users_schema())
        .ValueOrDie();
    db_->get_schema_registry()
        ->create("companies", create_companies_schema())
        .ValueOrDie();

    create_user(db_, "alex", 25);  // users(0)
    create_user(db_, "bob", 31);   // users(1)
    create_user(db_, "jeff", 33);  // users(2)

    create_company(db_, "google", 3000);  // companies(0)
    create_company(db_, "acme", 1200);    // companies(1)
    create_company(db_, "meta", 900);     // companies(2)

    db_->connect(0, "works-at", 0).ValueOrDie();  // alex -> google
    db_->connect(1, "works-at", 1).ValueOrDie();  // bob -> acme

    db_->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
    db_->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff
  }

  void expect_query_output(const Query& query, const std::string& query_text,
                           const std::string& expected_table) {
    auto result = db_->query(query);
    ASSERT_TRUE(result.ok()) << result.status().ToString();
    auto actual = table_to_test_string(result.ValueOrDie()->table());
    EXPECT_EQ(actual, expected_table) << "Query:\n" << query_text;
  }

  std::shared_ptr<Database> db_;
};

/*
TundraQL query:
MATCH (u:users)-[:works-at INNER]->(c:companies)
WHERE c.name = "google"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
*/
TEST_F(JoinWhereTest, InnerJoinTargetWhereFiltersMatchedRows) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at INNER]->(c:companies)\n"
      "WHERE c.name = \"google\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google";

  auto query =
      Query::match("u:users")
          .traverse("u", "works-at", "c:companies", TraverseType::Inner)
          .where("c.name", CompareOp::Eq, Value("google"s))
          .select({"u.name", "c.name"})
          .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at INNER]->(c:companies)
WHERE u.name = "alex"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
*/
TEST_F(JoinWhereTest, InnerJoinSourceWhereFiltersBeforeTraverse) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at INNER]->(c:companies)\n"
      "WHERE u.name = \"alex\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google";

  auto query =
      Query::match("u:users")
          .traverse("u", "works-at", "c:companies", TraverseType::Inner)
          .where("u.name", CompareOp::Eq, Value("alex"s))
          .select({"u.name", "c.name"})
          .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at INNER]->(c:companies)
WHERE u.name = "alex" AND c.name = "google"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
*/
TEST_F(JoinWhereTest, InnerJoinSourceAndTargetWhereMatchesSingleRow) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at INNER]->(c:companies)\n"
      "WHERE u.name = \"alex\" AND c.name = \"google\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google";

  auto query =
      Query::match("u:users")
          .traverse("u", "works-at", "c:companies", TraverseType::Inner)
          .where("u.name", CompareOp::Eq, Value("alex"s))
          .and_where("c.name", CompareOp::Eq, Value("google"s))
          .select({"u.name", "c.name"})
          .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at LEFT]->(c:companies)
WHERE u.name = "jeff"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
jeff | NULL
*/
TEST_F(JoinWhereTest, LeftJoinSourceWhereKeepsNullExtendedSourceRow) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at LEFT]->(c:companies)\n"
      "WHERE u.name = \"jeff\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "jeff | NULL";

  auto query = Query::match("u:users")
                   .traverse("u", "works-at", "c:companies", TraverseType::Left)
                   .where("u.name", CompareOp::Eq, Value("jeff"s))
                   .select({"u.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at LEFT]->(c:companies)
WHERE u.name = "alex" AND c.name = "google"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
*/
TEST_F(JoinWhereTest, LeftJoinSourceAndTargetWhereMatchesQualifiedRow) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at LEFT]->(c:companies)\n"
      "WHERE u.name = \"alex\" AND c.name = \"google\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google";

  auto query = Query::match("u:users")
                   .traverse("u", "works-at", "c:companies", TraverseType::Left)
                   .where("u.name", CompareOp::Eq, Value("alex"s))
                   .and_where("c.name", CompareOp::Eq, Value("google"s))
                   .select({"u.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at LEFT]->(c:companies)
WHERE c.name = "google" OR u.name = "jeff"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
jeff | NULL
*/
TEST_F(JoinWhereTest, LeftJoinMixedAliasOrRemainsResidual) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at LEFT]->(c:companies)\n"
      "WHERE c.name = \"google\" OR u.name = \"jeff\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google\n"
      "jeff | NULL";

  auto query = Query::match("u:users")
                   .traverse("u", "works-at", "c:companies", TraverseType::Left)
                   .where("c.name", CompareOp::Eq, Value("google"s))
                   .or_where("u.name", CompareOp::Eq, Value("jeff"s))
                   .select({"u.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at LEFT]->(c:companies)
WHERE c.name = "google"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
*/
TEST_F(JoinWhereTest,
       DISABLED_LeftJoinTargetWhereShouldFilterOutNullExtendedRows) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at LEFT]->(c:companies)\n"
      "WHERE c.name = \"google\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google";

  auto query = Query::match("u:users")
                   .traverse("u", "works-at", "c:companies", TraverseType::Left)
                   .where("c.name", CompareOp::Eq, Value("google"s))
                   .select({"u.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at RIGHT]->(c:companies)
WHERE u.name = "alex"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
alex | google
*/
TEST_F(JoinWhereTest, DISABLED_RightJoinSourceWhereDropsNullSourceRows) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at RIGHT]->(c:companies)\n"
      "WHERE u.name = \"alex\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "alex | google";

  auto query =
      Query::match("u:users")
          .traverse("u", "works-at", "c:companies", TraverseType::Right)
          .where("u.name", CompareOp::Eq, Value("alex"s))
          .select({"u.name", "c.name"})
          .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at RIGHT]->(c:companies)
WHERE c.name = "meta"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
NULL | meta
*/
TEST_F(JoinWhereTest, RightJoinTargetWhereKeepsUnmatchedTargetRow) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at RIGHT]->(c:companies)\n"
      "WHERE c.name = \"meta\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "NULL | meta";

  auto query =
      Query::match("u:users")
          .traverse("u", "works-at", "c:companies", TraverseType::Right)
          .where("c.name", CompareOp::Eq, Value("meta"s))
          .select({"u.name", "c.name"})
          .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at FULL]->(c:companies)
WHERE u.name = "jeff"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
jeff | NULL
*/
TEST_F(JoinWhereTest, DISABLED_FullJoinSourceWhereKeepsUnmatchedSourceRow) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at FULL]->(c:companies)\n"
      "WHERE u.name = \"jeff\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "jeff | NULL";

  auto query = Query::match("u:users")
                   .traverse("u", "works-at", "c:companies", TraverseType::Full)
                   .where("u.name", CompareOp::Eq, Value("jeff"s))
                   .select({"u.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:works-at FULL]->(c:companies)
WHERE c.name = "meta"
SELECT u.name, c.name;

Expected output table:
u.name | c.name
NULL | meta
*/
TEST_F(JoinWhereTest, DISABLED_FullJoinTargetWhereKeepsUnmatchedTargetRow) {
  const std::string query_text =
      "MATCH (u:users)-[:works-at FULL]->(c:companies)\n"
      "WHERE c.name = \"meta\"\n"
      "SELECT u.name, c.name;";
  const std::string expected_table =
      "u.name | c.name\n"
      "NULL | meta";

  auto query = Query::match("u:users")
                   .traverse("u", "works-at", "c:companies", TraverseType::Full)
                   .where("c.name", CompareOp::Eq, Value("meta"s))
                   .select({"u.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:friend INNER]->(f:users)-[:works-at INNER]->(c:companies)
WHERE f.name = "bob" AND c.name = "acme"
SELECT u.name, f.name, c.name;

Expected output table:
u.name | f.name | c.name
alex | bob | acme
*/
TEST_F(JoinWhereTest, TwoHopInnerJoinMiddleAndTargetWhere) {
  const std::string query_text =
      "MATCH (u:users)-[:friend INNER]->(f:users)-[:works-at INNER]->"
      "(c:companies)\n"
      "WHERE f.name = \"bob\" AND c.name = \"acme\"\n"
      "SELECT u.name, f.name, c.name;";
  const std::string expected_table =
      "u.name | f.name | c.name\n"
      "alex | bob | acme";

  auto query =
      Query::match("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Inner)
          .where("f.name", CompareOp::Eq, Value("bob"s))
          .and_where("c.name", CompareOp::Eq, Value("acme"s))
          .select({"u.name", "f.name", "c.name"})
          .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:friend INNER]->(f:users)-[:works-at LEFT]->(c:companies)
WHERE f.name = "jeff"
SELECT u.name, f.name, c.name;

Expected output table:
u.name | f.name | c.name
alex | jeff | NULL
*/
TEST_F(JoinWhereTest,
       DISABLED_TwoHopLeftJoinMiddleWhereKeepsNullExtendedTargetRow) {
  const std::string query_text =
      "MATCH (u:users)-[:friend INNER]->(f:users)-[:works-at LEFT]->"
      "(c:companies)\n"
      "WHERE f.name = \"jeff\"\n"
      "SELECT u.name, f.name, c.name;";
  const std::string expected_table =
      "u.name | f.name | c.name\n"
      "alex | jeff | NULL";

  auto query = Query::match("u:users")
                   .traverse("u", "friend", "f:users", TraverseType::Inner)
                   .traverse("f", "works-at", "c:companies", TraverseType::Left)
                   .where("f.name", CompareOp::Eq, Value("jeff"s))
                   .select({"u.name", "f.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

/*
TundraQL query:
MATCH (u:users)-[:friend INNER]->(f:users)-[:works-at LEFT]->(c:companies)
WHERE u.name = "alex" AND f.name = "bob" AND c.name = "acme"
SELECT u.name, f.name, c.name;

Expected output table:
u.name | f.name | c.name
alex | bob | acme
*/
TEST_F(JoinWhereTest, TwoHopLeftJoinRootMiddleAndTargetWhere) {
  const std::string query_text =
      "MATCH (u:users)-[:friend INNER]->(f:users)-[:works-at LEFT]->"
      "(c:companies)\n"
      "WHERE u.name = \"alex\" AND f.name = \"bob\" AND c.name = \"acme\"\n"
      "SELECT u.name, f.name, c.name;";
  const std::string expected_table =
      "u.name | f.name | c.name\n"
      "alex | bob | acme";

  auto query = Query::match("u:users")
                   .traverse("u", "friend", "f:users", TraverseType::Inner)
                   .traverse("f", "works-at", "c:companies", TraverseType::Left)
                   .where("u.name", CompareOp::Eq, Value("alex"s))
                   .and_where("f.name", CompareOp::Eq, Value("bob"s))
                   .and_where("c.name", CompareOp::Eq, Value("acme"s))
                   .select({"u.name", "f.name", "c.name"})
                   .build();

  expect_query_output(query, query_text, expected_table);
}

}  // namespace tundradb
