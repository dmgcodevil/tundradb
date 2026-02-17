#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "../include/core.hpp"
#include "../include/logger.hpp"
#include "../include/metadata.hpp"
#include "../include/query.hpp"

// Helper macro for Arrow operations
#define ASSERT_OK(expr) ASSERT_TRUE((expr).ok())

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {

struct User {
  std::string name;
  int64_t age;
};

struct Company {
  std::string name;
  int64_t size;
};

std::vector<std::shared_ptr<Node>> create_users(
    const std::shared_ptr<Database>& db, const std::vector<User>& users) {
  std::vector<std::shared_ptr<Node>> nodes;
  for (auto user : users) {
    std::unordered_map<std::string, Value> data = {{"name", Value{user.name}},
                                                   {"age", Value{user.age}}};

    auto node = db->create_node("users", data).ValueOrDie();
    nodes.push_back(node);
  }

  return nodes;
}

std::vector<std::shared_ptr<Node>> create_companies(
    const std::shared_ptr<Database>& db,
    const std::vector<Company>& companies) {
  std::vector<std::shared_ptr<Node>> nodes;
  for (auto company : companies) {
    std::unordered_map<std::string, Value> data = {
        {"name", Value{company.name}}, {"size", Value{company.size}}};

    auto node = db->create_node("companies", data).ValueOrDie();
    nodes.push_back(node);
  }

  return nodes;
}

std::shared_ptr<arrow::Schema> create_users_schema() {
  auto name_field = arrow::field("name", arrow::utf8());
  auto age_field = arrow::field("age", arrow::int64());
  return arrow::schema({name_field, age_field});
}

std::shared_ptr<arrow::Schema> create_companies_schema() {
  auto name_field = arrow::field("name", arrow::utf8());
  auto size_field = arrow::field("size", arrow::int64());
  return arrow::schema({name_field, size_field});
}

std::shared_ptr<Database> setup_test_db() {
  auto db_path = "test_db_" + std::to_string(now_millis());
  // auto db_path = "testdb_1744518985889565";
  auto config = make_config()
                    .with_db_path(db_path)
                    .with_shard_capacity(1000)
                    .with_chunk_size(1000)
                    .build();

  auto users_schema = create_users_schema();
  auto companies_schema = create_companies_schema();
  auto db = std::make_shared<Database>(config);
  db->get_schema_registry()->create("users", users_schema).ValueOrDie();
  db->get_schema_registry()->create("companies", companies_schema).ValueOrDie();
  const auto users =
      std::vector({User{"alex", 25}, User{"bob", 31}, User{"jeff", 33},
                   User{"sam", 21}, User{"matt", 40}});

  const auto companies = std::vector{
      {Company{"ibm", 1000}, Company{"google", 3000}, Company{"aws", 5000}}};
  create_users(db, users);
  create_companies(db, companies);
  return db;
}

TEST(JoinTest, MatchAll) {
  auto db = setup_test_db();
  Query query = Query::from("u:users").build();
  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging if needed
  std::cout << "Result Table:" << std::endl;
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));
  ASSERT_EQ(result_table->num_rows(), 5);
}

TEST(JoinTest, UserFriendCompanyInnerJoin) {
  auto db = setup_test_db();

  db->connect(0, "friend", 1).ValueOrDie();
  db->connect(1, "works-at", 1).ValueOrDie();

  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Inner)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging if needed
  std::cout << "JoinTest Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  ASSERT_EQ(result_table->num_rows(), 1);

  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
      expected_cells;

  expected_cells["u.id"] = arrow::MakeScalar((int64_t)0);
  expected_cells["u.name"] = arrow::MakeScalar("alex");
  expected_cells["u.age"] = arrow::MakeScalar((int64_t)25);

  expected_cells["f.id"] = arrow::MakeScalar((int64_t)1);
  expected_cells["f.name"] = arrow::MakeScalar("bob");
  expected_cells["f.age"] = arrow::MakeScalar((int64_t)31);

  expected_cells["c.id"] = arrow::MakeScalar((int64_t)1);
  expected_cells["c.name"] = arrow::MakeScalar("google");
  expected_cells["c.size"] = arrow::MakeScalar((int64_t)3000);

  ASSERT_EQ(result_table->num_rows(), 1);
  if (result_table->num_rows() == 1) {
    for (const auto& field : result_table->schema()->fields()) {
      auto column = result_table->GetColumnByName(field->name());
      ASSERT_NE(column, nullptr);
      auto scalar_result =
          column->GetScalar(0);  // Get value from the first (only) row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();

      ASSERT_TRUE(expected_cells.count(field->name()))
          << "Unexpected column in result: " << field->name();
      std::shared_ptr<arrow::Scalar> expected_scalar =
          expected_cells.at(field->name());
      // std::cout << "actual_scalar=" << actual_scalar->type->ToString() <<
      // std::endl; std::cout << "expected_scalar=" <<
      // expected_scalar->type->ToString() << std::endl;
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in column \'" << field->name() << "\': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }
}

TEST(JoinTest, JoinFromSameNode) {
  auto db = setup_test_db();
  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff

  Query query = Query::from("u:users")
                    .traverse("u", "friend", "f:users", TraverseType::Inner)
                    .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging if needed
  std::cout << "JoinTest Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  ASSERT_EQ(result_table->num_rows(), 2);

  // Verify the first row
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
        expected_row1;
    expected_row1["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row1["u.name"] = arrow::MakeScalar("alex");
    expected_row1["u.age"] = arrow::MakeScalar((int64_t)25);
    expected_row1["f.id"] = arrow::MakeScalar((int64_t)1);
    expected_row1["f.name"] = arrow::MakeScalar("bob");
    expected_row1["f.age"] = arrow::MakeScalar((int64_t)31);

    for (const auto& [field_name, expected_scalar] : expected_row1) {
      auto column = result_table->GetColumnByName(field_name);
      ASSERT_NE(column, nullptr);
      auto scalar_result = column->GetScalar(0);  // First row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in row 1, column '" << field_name << "': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }

  // Verify the second row
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
        expected_row2;
    expected_row2["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row2["u.name"] = arrow::MakeScalar("alex");
    expected_row2["u.age"] = arrow::MakeScalar((int64_t)25);
    expected_row2["f.id"] = arrow::MakeScalar((int64_t)2);
    expected_row2["f.name"] = arrow::MakeScalar("jeff");
    expected_row2["f.age"] = arrow::MakeScalar((int64_t)33);

    for (const auto& [field_name, expected_scalar] : expected_row2) {
      auto column = result_table->GetColumnByName(field_name);
      ASSERT_NE(column, nullptr);
      auto scalar_result = column->GetScalar(1);  // Second row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in row 2, column '" << field_name << "': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }
}

TEST(JoinTest, InnerJoinFromSameNodeMultiTarget) {
  auto db = setup_test_db();
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(0, "works-at", 1).ValueOrDie();  // alex -> google

  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("u", "works-at", "c:companies", TraverseType::Inner)
          .build();
  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "InnerJoinFromSameNodeMultiTarget Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Verify result has exactly 2 rows (cartesian product of 2 friends × 1
  // company)
  ASSERT_EQ(result_table->num_rows(), 2);

  // Verify the first row: alex -> bob + alex -> google
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
        expected_row1;
    // Alex (source user)
    expected_row1["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row1["u.name"] = arrow::MakeScalar("alex");
    expected_row1["u.age"] = arrow::MakeScalar((int64_t)25);
    // Bob (friend)
    expected_row1["f.id"] = arrow::MakeScalar((int64_t)1);
    expected_row1["f.name"] = arrow::MakeScalar("bob");
    expected_row1["f.age"] = arrow::MakeScalar((int64_t)31);
    // Google (company)
    expected_row1["c.id"] = arrow::MakeScalar((int64_t)1);
    expected_row1["c.name"] = arrow::MakeScalar("google");
    expected_row1["c.size"] = arrow::MakeScalar((int64_t)3000);

    for (const auto& [field_name, expected_scalar] : expected_row1) {
      auto column = result_table->GetColumnByName(field_name);
      ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
      auto scalar_result = column->GetScalar(0);  // First row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in row 1, column '" << field_name << "': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }

  // Verify the second row: alex -> jeff + alex -> google
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
        expected_row2;
    // Alex (source user)
    expected_row2["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row2["u.name"] = arrow::MakeScalar("alex");
    expected_row2["u.age"] = arrow::MakeScalar((int64_t)25);
    // Jeff (friend)
    expected_row2["f.id"] = arrow::MakeScalar((int64_t)2);
    expected_row2["f.name"] = arrow::MakeScalar("jeff");
    expected_row2["f.age"] = arrow::MakeScalar((int64_t)33);
    // Google (company)
    expected_row2["c.id"] = arrow::MakeScalar((int64_t)1);
    expected_row2["c.name"] = arrow::MakeScalar("google");
    expected_row2["c.size"] = arrow::MakeScalar((int64_t)3000);

    for (const auto& [field_name, expected_scalar] : expected_row2) {
      auto column = result_table->GetColumnByName(field_name);
      ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
      auto scalar_result = column->GetScalar(1);  // Second row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in row 2, column '" << field_name << "': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }
}

TEST(JoinTest, InnerJoinFromSameNodeAndEndConnections) {
  auto db = setup_test_db();
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(0, "works-at", 0).ValueOrDie();  // alex -> ibm
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  db->connect(2, "works-at", 2).ValueOrDie();  // jeff -> aws

  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("u", "works-at", "c:companies", TraverseType::Inner)
          .build();
  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "InnerJoinFromSameNodeAndEndConnections Result Table:"
            << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Verify result has exactly 2 rows (cartesian product of 2 friends × 1
  // company)
  ASSERT_EQ(result_table->num_rows(), 2);

  // Verify the first row: alex -> bob + alex -> ibm
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
        expected_row1;
    // Alex (source user)
    expected_row1["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row1["u.name"] = arrow::MakeScalar("alex");
    expected_row1["u.age"] = arrow::MakeScalar((int64_t)25);
    // Bob (friend)
    expected_row1["f.id"] = arrow::MakeScalar((int64_t)1);
    expected_row1["f.name"] = arrow::MakeScalar("bob");
    expected_row1["f.age"] = arrow::MakeScalar((int64_t)31);
    // IBM (company)
    expected_row1["c.id"] = arrow::MakeScalar((int64_t)0);
    expected_row1["c.name"] = arrow::MakeScalar("ibm");
    expected_row1["c.size"] = arrow::MakeScalar((int64_t)1000);

    for (const auto& [field_name, expected_scalar] : expected_row1) {
      auto column = result_table->GetColumnByName(field_name);
      ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
      auto scalar_result = column->GetScalar(0);  // First row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in row 1, column '" << field_name << "': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }

  // Verify the second row: alex -> jeff + alex -> ibm
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
        expected_row2;
    // Alex (source user)
    expected_row2["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row2["u.name"] = arrow::MakeScalar("alex");
    expected_row2["u.age"] = arrow::MakeScalar((int64_t)25);
    // Jeff (friend)
    expected_row2["f.id"] = arrow::MakeScalar((int64_t)2);
    expected_row2["f.name"] = arrow::MakeScalar("jeff");
    expected_row2["f.age"] = arrow::MakeScalar((int64_t)33);
    // IBM (company)
    expected_row2["c.id"] = arrow::MakeScalar((int64_t)0);
    expected_row2["c.name"] = arrow::MakeScalar("ibm");
    expected_row2["c.size"] = arrow::MakeScalar((int64_t)1000);

    for (const auto& [field_name, expected_scalar] : expected_row2) {
      auto column = result_table->GetColumnByName(field_name);
      ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
      auto scalar_result = column->GetScalar(1);  // Second row
      ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
      auto actual_scalar = scalar_result.ValueOrDie();
      ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
          << "Mismatch in row 2, column '" << field_name << "': Expected "
          << expected_scalar->ToString() << " but got "
          << actual_scalar->ToString();
    }
  }
}

TEST(JoinTest, EmptyResultFromInnerJoin) {
  auto db = setup_test_db();
  // Create relationships that will result in empty results due to inner join
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob (friend)
  db->connect(1, "friend", 2).ValueOrDie();    // bob -> jeff (friend)
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  // But no connections for jeff to any company

  // Query that will return no results because jeff doesn't work anywhere
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f1:users", TraverseType::Inner)
          .traverse("f1", "friend", "f2:users", TraverseType::Inner)
          .traverse("f2", "works-at", "c:companies", TraverseType::Inner)
          .build();

  auto query_result = db->query(query);
  if (query_result.ok()) {
    std::cout << query_result.status().ToString() << std::endl;
  }
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "EmptyResultFromInnerJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should have 0 rows due to inner join failing at the last hop
  ASSERT_EQ(result_table->num_rows(), 0);
}

TEST(JoinTest, MultiPathToSameTarget) {
  auto db = setup_test_db();
  // Create multiple paths to the same target
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(0, "works-at", 0).ValueOrDie();  // alex -> ibm
  db->connect(1, "works-at", 0)
      .ValueOrDie();  // bob -> ibm (same company as alex)
  db->connect(2, "works-at", 1).ValueOrDie();  // jeff -> google

  // Query: Find all friends of alex who work at the same company as alex
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("u", "works-at", "c1:companies", TraverseType::Inner)
          .traverse("f", "works-at", "c2:companies", TraverseType::Inner)
          .where("c1.id", CompareOp::Eq,
                 Value((int64_t)0))  // Filter for alex's company (IBM ID 0)
          .where("c2.id", CompareOp::Eq,
                 Value((int64_t)0))  // Filter for friend's company (also IBM ID 0)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "MultiPathToSameTarget Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should have 1 row - bob works at IBM just like alex does
  ASSERT_EQ(result_table->num_rows(), 1);

  // Verify the expected row
  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> expected_row;
  // u1 is alex
  expected_row["u.id"] = arrow::MakeScalar((int64_t)0);
  expected_row["u.name"] = arrow::MakeScalar("alex");
  expected_row["u.age"] = arrow::MakeScalar((int64_t)25);
  // u2 is bob (friend)
  expected_row["f.id"] = arrow::MakeScalar((int64_t)1);
  expected_row["f.name"] = arrow::MakeScalar("bob");
  expected_row["f.age"] = arrow::MakeScalar((int64_t)31);
  // c1 is IBM (alex's company)
  expected_row["c1.id"] = arrow::MakeScalar((int64_t)0);
  expected_row["c1.name"] = arrow::MakeScalar("ibm");
  expected_row["c1.size"] = arrow::MakeScalar((int64_t)1000);
  // c2 is also IBM (bob's company)
  expected_row["c2.id"] = arrow::MakeScalar((int64_t)0);
  expected_row["c2.name"] = arrow::MakeScalar("ibm");
  expected_row["c2.size"] = arrow::MakeScalar((int64_t)1000);

  for (const auto& [field_name, expected_scalar] : expected_row) {
    auto column = result_table->GetColumnByName(field_name);
    ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
    auto scalar_result = column->GetScalar(0);
    ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
    auto actual_scalar = scalar_result.ValueOrDie();
    ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
        << "Mismatch in column '" << field_name << "': Expected "
        << expected_scalar->ToString() << " but got "
        << actual_scalar->ToString();
  }
}

TEST(JoinTest, CartesianProductExplosion) {
  auto db = setup_test_db();
  // Create a pattern with many-to-many relationships
  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff
  db->connect(0, "friend", 3).ValueOrDie();  // alex -> sam

  db->connect(1, "works-at", 0).ValueOrDie();  // bob -> ibm
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google

  db->connect(2, "works-at", 1).ValueOrDie();  // jeff -> google
  db->connect(2, "works-at", 2).ValueOrDie();  // jeff -> aws

  db->connect(3, "works-at", 0).ValueOrDie();  // sam -> ibm
  db->connect(3, "works-at", 2).ValueOrDie();  // sam -> aws

  // Query: Friends of alex and where they work
  // Results in 3 friends × ~2 companies each = ~6 rows total
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Inner)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "CartesianProductExplosion Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should have 6 rows: Bob(IBM,Google) + Jeff(Google,AWS) + Sam(IBM,AWS)
  ASSERT_EQ(result_table->num_rows(), 6);

  // Verify some rows contain the expected companies
  std::set<std::string> found_companies;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto company_col = result_table->GetColumnByName("c.name");
    auto scalar_result = company_col->GetScalar(i);
    ASSERT_TRUE(scalar_result.ok());
    auto company_scalar = std::static_pointer_cast<arrow::StringScalar>(
        scalar_result.ValueOrDie());
    found_companies.insert(company_scalar->ToString());
  }

  // Should have all three companies in results
  ASSERT_TRUE(found_companies.find("ibm") != found_companies.end())
      << "IBM missing from results";
  ASSERT_TRUE(found_companies.find("google") != found_companies.end())
      << "Google missing from results";
  ASSERT_TRUE(found_companies.find("aws") != found_companies.end())
      << "AWS missing from results";
}

TEST(JoinTest, LeftJoin) {
  auto db = setup_test_db();
  // Create relationships where some nodes don't have target matches
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  // jeff has no company (will produce NULL in the results with LEFT JOIN)

  // LEFT JOIN: Keep all users even if they don't work at any company
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Left)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "LeftJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should have 2 rows - one for bob (with company) and one for jeff (without
  // company)
  ASSERT_EQ(result_table->num_rows(), 2);

  // Define the two expected rows
  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> bob_row;
  bob_row["u.id"] = arrow::MakeScalar((int64_t)0);
  bob_row["u.name"] = arrow::MakeScalar("alex");
  bob_row["u.age"] = arrow::MakeScalar((int64_t)25);
  bob_row["f.id"] = arrow::MakeScalar((int64_t)1);
  bob_row["f.name"] = arrow::MakeScalar("bob");
  bob_row["f.age"] = arrow::MakeScalar((int64_t)31);
  bob_row["c.id"] = arrow::MakeScalar((int64_t)1);
  bob_row["c.name"] = arrow::MakeScalar("google");
  bob_row["c.size"] = arrow::MakeScalar((int64_t)3000);

  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> jeff_row;
  jeff_row["u.id"] = arrow::MakeScalar((int64_t)0);
  jeff_row["u.name"] = arrow::MakeScalar("alex");
  jeff_row["u.age"] = arrow::MakeScalar((int64_t)25);
  jeff_row["f.id"] = arrow::MakeScalar((int64_t)2);
  jeff_row["f.name"] = arrow::MakeScalar("jeff");
  jeff_row["f.age"] = arrow::MakeScalar((int64_t)33);
  // Jeff's company fields are NULL

  // Find which row is which by checking for the friend name
  int bob_index = -1;
  int jeff_index = -1;

  auto friend_name_col = result_table->GetColumnByName("f.name");
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto scalar_result = friend_name_col->GetScalar(i);
    ASSERT_TRUE(scalar_result.ok());
    auto name_scalar = std::static_pointer_cast<arrow::StringScalar>(
        scalar_result.ValueOrDie());

    if (name_scalar->view() == "bob") {
      bob_index = i;
    } else if (name_scalar->view() == "jeff") {
      jeff_index = i;
    }
  }

  ASSERT_NE(bob_index, -1) << "Could not find row with bob as friend";
  ASSERT_NE(jeff_index, -1) << "Could not find row with jeff as friend";

  // Verify bob's row (with company)
  for (const auto& [field_name, expected_scalar] : bob_row) {
    auto column = result_table->GetColumnByName(field_name);
    ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
    auto scalar_result = column->GetScalar(bob_index);
    ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
    auto actual_scalar = scalar_result.ValueOrDie();
    ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
        << "Mismatch in bob's row, column '" << field_name << "': Expected "
        << expected_scalar->ToString() << " but got "
        << actual_scalar->ToString();
  }

  // Verify jeff's row (without company)
  for (const auto& [field_name, expected_scalar] : jeff_row) {
    auto column = result_table->GetColumnByName(field_name);
    ASSERT_NE(column, nullptr) << "Column " << field_name << " not found";
    auto scalar_result = column->GetScalar(jeff_index);
    ASSERT_TRUE(scalar_result.ok()) << scalar_result.status().ToString();
    auto actual_scalar = scalar_result.ValueOrDie();
    ASSERT_TRUE(actual_scalar->Equals(*expected_scalar))
        << "Mismatch in jeff's row, column '" << field_name << "': Expected "
        << expected_scalar->ToString() << " but got "
        << actual_scalar->ToString();
  }

  // Verify company fields are NULL for jeff
  auto c_id_col = result_table->GetColumnByName("c.id");
  auto c_name_col = result_table->GetColumnByName("c.name");
  auto c_size_col = result_table->GetColumnByName("c.size");

  ASSERT_TRUE(c_id_col->chunk(0)->IsNull(jeff_index))
      << "Expected NULL for c.id in jeff's row";
  ASSERT_TRUE(c_name_col->chunk(0)->IsNull(jeff_index))
      << "Expected NULL for c.name in jeff's row";
  ASSERT_TRUE(c_size_col->chunk(0)->IsNull(jeff_index))
      << "Expected NULL for c.size in jeff's row";
}

TEST(JoinTest, RightJoin) {
  auto db = setup_test_db();
  // Create relationships where some targets don't have matching sources
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  db->connect(2, "works-at", 2).ValueOrDie();  // jeff -> aws
  // Sam (id=3) has no friends but works at ibm
  db->connect(3, "works-at", 0).ValueOrDie();  // sam -> ibm

  // RIGHT JOIN: Keep all companies even if no users work there
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Right)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "RightJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should show IBM in the results even though no friend of alex works there
  // But sam (who isn't alex's friend) works at IBM

  // Find expected companies in the results
  std::set<std::string> found_companies;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto company_col = result_table->GetColumnByName("c.name");
    auto scalar_result = company_col->GetScalar(i);
    if (scalar_result.ok()) {
      auto company_scalar = std::static_pointer_cast<arrow::StringScalar>(
          scalar_result.ValueOrDie());
      found_companies.insert(company_scalar->ToString());
    }
  }

  // Should have all companies in results, including IBM (where no friend works)
  ASSERT_TRUE(found_companies.find("google") != found_companies.end())
      << "Google missing from results";
  ASSERT_TRUE(found_companies.find("aws") != found_companies.end())
      << "AWS missing from results";
  ASSERT_TRUE(found_companies.find("ibm") != found_companies.end())
      << "IBM missing from results (RIGHT JOIN should include it)";
}

TEST(JoinTest, CombinedJoinTypes) {
  auto db = setup_test_db();
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  // jeff has no company

  // Create a row for matt who has no friends
  db->connect(4, "works-at", 0).ValueOrDie();  // matt -> ibm

  // Query that combines INNER, LEFT and RIGHT joins
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Left)
          .traverse("f", "works-at", "c:companies", TraverseType::Right)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "CombinedJoinTypes Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Expected result should include:
  // 1. alex->bob->google (regular match)
  // 2. alex->jeff->NULL (LEFT JOIN: jeff has no company)
  // 3. NULL->NULL->ibm (RIGHT JOIN: ibm included but no user works there)

  // Check for the presence of specific scenarios
  bool has_alex_bob_google = false;  // Normal match
  bool has_alex_jeff_null = false;   // LEFT JOIN effect (jeff has no company)
  bool has_null_null_aws = false;    // RIGHT JOIN effect (aws has no employee)

  // Print each row for debugging
  std::cout << "Checking rows for specific patterns:" << std::endl;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto u_id_col = result_table->GetColumnByName("u.id");
    auto f_id_col = result_table->GetColumnByName("f.id");
    auto c_id_col = result_table->GetColumnByName("c.id");

    std::cout << "Row " << i << ": ";

    // Print user ID
    if (u_id_col->chunk(0)->IsNull(i)) {
      std::cout << "u.id=NULL ";
    } else {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      std::cout << "u.id=" << u_id << " ";
    }

    // Print friend ID
    if (f_id_col->chunk(0)->IsNull(i)) {
      std::cout << "f.id=NULL ";
    } else {
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      std::cout << "f.id=" << f_id << " ";
    }

    // Print company ID
    if (c_id_col->chunk(0)->IsNull(i)) {
      std::cout << "c.id=NULL";
    } else {
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      std::cout << "c.id=" << c_id;
    }
    std::cout << std::endl;

    // Check for alex(0)->bob(1)->google(6) using IDs instead of names for more
    // reliability
    if (!u_id_col->chunk(0)->IsNull(i) && !f_id_col->chunk(0)->IsNull(i) &&
        !c_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (u_id == 0 && f_id == 1 && c_id == 1) {
        has_alex_bob_google = true;
        std::cout << "  ✓ Found alex->bob->google pattern" << std::endl;
      }
    }

    // Check for alex(0)->jeff(2)->NULL company using IDs
    if (!u_id_col->chunk(0)->IsNull(i) && !f_id_col->chunk(0)->IsNull(i) &&
        c_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (u_id == 0 && f_id == 2) {
        has_alex_jeff_null = true;
        std::cout << "  ✓ Found alex->jeff->NULL pattern" << std::endl;
      }
    }

    // Check for NULL->NULL->aws(7) using ID
    if ((u_id_col->chunk(0)->IsNull(i) || f_id_col->chunk(0)->IsNull(i)) &&
        !c_id_col->chunk(0)->IsNull(i)) {
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (c_id == 2) {
        has_null_null_aws = true;
        std::cout << "  ✓ Found NULL->NULL->aws pattern" << std::endl;
      }
    }
  }

  ASSERT_TRUE(has_alex_bob_google) << "Should include regular matches";
  ASSERT_TRUE(has_alex_jeff_null)
      << "Should include left-join effect (jeff with NULL company)";
  ASSERT_TRUE(has_null_null_aws)
      << "Should include right-join effect (AWS with no connected users)";
}

TEST(JoinTest, MultiLevelLeftJoin) {
  auto db = setup_test_db();
  // Create a multi-level relationship
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(0, "friend", 3).ValueOrDie();    // alex -> sam
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  db->connect(2, "likes", 0).ValueOrDie();     // jeff -> ibm (Company ID 0)
  // sam has no company and no likes

  // Multi-level LEFT JOINs: Keep all users at each level
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Left)
          .traverse("f", "works-at", "c:companies", TraverseType::Left)
          .traverse("f", "likes", "l:companies", TraverseType::Left)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "MultiLevelLeftJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should have 7 rows - 3 for alex's friends and 4 for each user as a starting
  // point
  ASSERT_EQ(result_table->num_rows(), 7);

  // Get all user IDs in the result
  std::set<int64_t> user_ids;
  auto user_id_col = result_table->GetColumnByName("u.id");

  for (int i = 0; i < result_table->num_rows(); i++) {
    if (!user_id_col->chunk(0)->IsNull(i)) {
      auto user_id = std::static_pointer_cast<arrow::Int64Scalar>(
                         user_id_col->GetScalar(i).ValueOrDie())
                         ->value;
      user_ids.insert(user_id);
    }
  }

  // All 5 users should be in the result
  ASSERT_EQ(user_ids.size(), 5) << "All 5 users should be included";
  for (int64_t i = 0; i < 5; i++) {
    ASSERT_TRUE(user_ids.find(i) != user_ids.end())
        << "User ID " << i << " missing";
  }

  // Find the Alex-Bob-Google row
  bool found_alex_bob_google = false;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto u_id_col = result_table->GetColumnByName("u.id");
    auto f_id_col = result_table->GetColumnByName("f.id");
    auto c_id_col = result_table->GetColumnByName("c.id");

    if (!u_id_col->chunk(0)->IsNull(i) && !f_id_col->chunk(0)->IsNull(i) &&
        !c_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (u_id == 0 && f_id == 1 && c_id == 1) {
        found_alex_bob_google = true;
        break;
      }
    }
  }

  ASSERT_TRUE(found_alex_bob_google) << "Alex->Bob->Google path not found";

  // Find the Alex-Jeff-IBM row
  bool found_alex_jeff_ibm = false;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto u_id_col = result_table->GetColumnByName("u.id");
    auto f_id_col = result_table->GetColumnByName("f.id");
    auto l_id_col = result_table->GetColumnByName("l.id");

    if (!u_id_col->chunk(0)->IsNull(i) && !f_id_col->chunk(0)->IsNull(i) &&
        !l_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto l_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      l_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (u_id == 0 && f_id == 2 && l_id == 0) {  // l_id=0 (IBM per-schema)
        found_alex_jeff_ibm = true;
        break;
      }
    }
  }

  ASSERT_TRUE(found_alex_jeff_ibm) << "Alex->Jeff->IBM path not found";

  // Test for all users to be included in the results
  std::set<int64_t> all_users;
  auto u_id_col = result_table->GetColumnByName("u.id");
  for (int i = 0; i < result_table->num_rows(); i++) {
    if (!u_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      all_users.insert(u_id);
    }
  }

  // Verify all 5 users are included in the results
  ASSERT_EQ(all_users.size(), 5) << "Expected all 5 users in the result";
  for (int64_t i = 0; i < 5; i++) {
    ASSERT_TRUE(all_users.find(i) != all_users.end())
        << "User ID " << i << " missing";
  }

  // The following tests are incorrect because our query doesn't look for direct
  // connections from users to companies. It only looks for connections through
  // friends. Removed:
  // - Test for Bob's direct connection to Google when Bob is the starting point
  // - Test for Jeff's direct connection to IBM when Jeff is the starting point
}

TEST(JoinTest, SelfJoinWithLeftJoin) {
  auto db = setup_test_db();
  // Create relationships for a self-join scenario with missing relationships
  db->connect(0, "manages", 1).ValueOrDie();  // alex manages bob
  db->connect(1, "manages", 2).ValueOrDie();  // bob manages jeff
  db->connect(1, "manages", 3).ValueOrDie();  // bob manages sam
  // matt (id=4) is not managed by anyone and doesn't manage anyone

  // LEFT JOIN with self: Find all management chains, including users with no
  // manager or subordinates
  Query query =
      Query::from("manager:users")
          .traverse("manager", "manages", "employee:users", TraverseType::Left)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "SelfJoinWithLeftJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // For detailed debugging, print each row with IDs
  std::cout << "Managers and employees by ID:" << std::endl;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto manager_id_col = result_table->GetColumnByName("manager.id");
    auto employee_id_col = result_table->GetColumnByName("employee.id");

    auto manager_id =
        manager_id_col->chunk(0)->IsNull(i)
            ? "NULL"
            : std::to_string(std::static_pointer_cast<arrow::Int64Scalar>(
                                 manager_id_col->GetScalar(i).ValueOrDie())
                                 ->value);

    auto employee_id =
        employee_id_col->chunk(0)->IsNull(i)
            ? "NULL"
            : std::to_string(std::static_pointer_cast<arrow::Int64Scalar>(
                                 employee_id_col->GetScalar(i).ValueOrDie())
                                 ->value);

    std::cout << "Row " << i << ": manager.id=" << manager_id
              << ", employee.id=" << employee_id << std::endl;
  }

  // Get all unique manager IDs from the result table
  std::set<int64_t> manager_ids;
  auto manager_id_col = result_table->GetColumnByName("manager.id");

  for (int i = 0; i < result_table->num_rows(); i++) {
    if (!manager_id_col->chunk(0)->IsNull(i)) {
      auto scalar_result = manager_id_col->GetScalar(i);
      ASSERT_TRUE(scalar_result.ok());
      auto id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
          scalar_result.ValueOrDie());
      manager_ids.insert(id_scalar->value);
    }
  }

  // Check that all 5 users are included as managers (all users in the database)
  std::cout << "Found " << manager_ids.size() << " unique managers: ";
  for (auto id : manager_ids) {
    std::cout << id << " ";
  }
  std::cout << std::endl;

  // We expect manager IDs 0, 1, 2, 3, 4 (all users)
  ASSERT_EQ(manager_ids.size(), 5) << "Expected 5 unique managers (all users)";
  ASSERT_TRUE(manager_ids.find(0) != manager_ids.end())
      << "Manager ID 0 (alex) missing";
  ASSERT_TRUE(manager_ids.find(1) != manager_ids.end())
      << "Manager ID 1 (bob) missing";
  ASSERT_TRUE(manager_ids.find(2) != manager_ids.end())
      << "Manager ID 2 (jeff) missing";
  ASSERT_TRUE(manager_ids.find(3) != manager_ids.end())
      << "Manager ID 3 (sam) missing";
  ASSERT_TRUE(manager_ids.find(4) != manager_ids.end())
      << "Manager ID 4 (matt) missing";

  // Check management relationships
  bool alex_manages_bob = false;
  bool bob_manages_jeff = false;
  bool bob_manages_sam = false;

  for (int i = 0; i < result_table->num_rows(); i++) {
    auto manager_id_col = result_table->GetColumnByName("manager.id");
    auto employee_id_col = result_table->GetColumnByName("employee.id");

    if (employee_id_col->chunk(0)->IsNull(i)) {
      continue;  // Skip NULL employee rows
    }

    auto manager_id = std::static_pointer_cast<arrow::Int64Scalar>(
                          manager_id_col->GetScalar(i).ValueOrDie())
                          ->value;
    auto employee_id = std::static_pointer_cast<arrow::Int64Scalar>(
                           employee_id_col->GetScalar(i).ValueOrDie())
                           ->value;

    if (manager_id == 0 && employee_id == 1) {
      alex_manages_bob = true;
    } else if (manager_id == 1 && employee_id == 2) {
      bob_manages_jeff = true;
    } else if (manager_id == 1 && employee_id == 3) {
      bob_manages_sam = true;
    }
  }

  ASSERT_TRUE(alex_manages_bob) << "Alex should manage Bob";
  ASSERT_TRUE(bob_manages_jeff) << "Bob should manage Jeff";
  ASSERT_TRUE(bob_manages_sam) << "Bob should manage Sam";
}

TEST(JoinTest, FullOuterJoin) {
  auto db = setup_test_db();

  // Create relationships for a FULL OUTER JOIN scenario
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google
  // jeff has no company

  // matt (id=4) has no friends but works at ibm
  db->connect(4, "works-at", 0).ValueOrDie();  // matt -> ibm

  // AWS (id=7) has no employee connected directly

  // FULL OUTER JOIN: Keep all records from both sides
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Full)
          .traverse("f", "works-at", "c:companies", TraverseType::Full)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "FullOuterJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Collect all company names present in the result
  std::set<std::string> company_names;
  auto company_col = result_table->GetColumnByName("c.name");

  for (int i = 0; i < result_table->num_rows(); i++) {
    auto scalar_result = company_col->GetScalar(i);
    if (!scalar_result.ok() || company_col->chunk(0)->IsNull(i)) {
      continue;  // Skip NULL company rows
    }
    auto company_scalar = std::static_pointer_cast<arrow::StringScalar>(
        scalar_result.ValueOrDie());
    company_names.insert(company_scalar->ToString());
  }

  // FULL OUTER JOIN should include all companies, even those not connected to
  // friends
  ASSERT_TRUE(company_names.find("google") != company_names.end())
      << "Google missing from results";
  ASSERT_TRUE(company_names.find("ibm") != company_names.end())
      << "IBM missing from results (FULL JOIN should include it)";
  ASSERT_TRUE(company_names.find("aws") != company_names.end())
      << "AWS missing from results (FULL JOIN should include it)";

  // Check for the presence of specific scenarios
  bool has_alex_bob_google = false;  // Normal match
  bool has_alex_jeff_null = false;   // LEFT JOIN effect (jeff has no company)
  bool has_null_null_aws = false;    // RIGHT JOIN effect (aws has no employee)

  // Print each row for debugging
  std::cout << "Checking rows for specific patterns:" << std::endl;
  for (int i = 0; i < result_table->num_rows(); i++) {
    auto u_id_col = result_table->GetColumnByName("u.id");
    auto f_id_col = result_table->GetColumnByName("f.id");
    auto c_id_col = result_table->GetColumnByName("c.id");

    std::cout << "Row " << i << ": ";

    // Print user ID
    if (u_id_col->chunk(0)->IsNull(i)) {
      std::cout << "u.id=NULL ";
    } else {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      std::cout << "u.id=" << u_id << " ";
    }

    // Print friend ID
    if (f_id_col->chunk(0)->IsNull(i)) {
      std::cout << "f.id=NULL ";
    } else {
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      std::cout << "f.id=" << f_id << " ";
    }

    // Print company ID
    if (c_id_col->chunk(0)->IsNull(i)) {
      std::cout << "c.id=NULL";
    } else {
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      std::cout << "c.id=" << c_id;
    }
    std::cout << std::endl;

    // Check for alex(0)->bob(1)->google(6) using IDs instead of names for more
    // reliability
    if (!u_id_col->chunk(0)->IsNull(i) && !f_id_col->chunk(0)->IsNull(i) &&
        !c_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (u_id == 0 && f_id == 1 && c_id == 1) {
        has_alex_bob_google = true;
        std::cout << "  ✓ Found alex->bob->google pattern" << std::endl;
      }
    }

    // Check for alex(0)->jeff(2)->NULL company using IDs
    if (!u_id_col->chunk(0)->IsNull(i) && !f_id_col->chunk(0)->IsNull(i) &&
        c_id_col->chunk(0)->IsNull(i)) {
      auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      u_id_col->GetScalar(i).ValueOrDie())
                      ->value;
      auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      f_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (u_id == 0 && f_id == 2) {
        has_alex_jeff_null = true;
        std::cout << "  ✓ Found alex->jeff->NULL pattern" << std::endl;
      }
    }

    // Check for NULL->NULL->aws(7) using ID
    if ((u_id_col->chunk(0)->IsNull(i) || f_id_col->chunk(0)->IsNull(i)) &&
        !c_id_col->chunk(0)->IsNull(i)) {
      auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                      c_id_col->GetScalar(i).ValueOrDie())
                      ->value;

      if (c_id == 2) {
        has_null_null_aws = true;
        std::cout << "  ✓ Found NULL->NULL->aws pattern" << std::endl;
      }
    }
  }

  ASSERT_TRUE(has_alex_bob_google) << "Should include regular matches";
  ASSERT_TRUE(has_alex_jeff_null)
      << "Should include left-join effect (jeff with NULL company)";
  ASSERT_TRUE(has_null_null_aws)
      << "Should include right-join effect (AWS with no connected users)";
}

TEST(JoinTest, SelectClauseFiltering) {
  auto db = setup_test_db();

  // Create some connections for our test
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(1, "works-at", 1).ValueOrDie();  // bob -> google

  // Query with SELECT - only get user (u) and friend (f) columns
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Inner)
          .select({"u", "f"})  // Only select u.* and f.* columns
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "SelectClauseFiltering Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Verify that only columns with u.* and f.* prefixes are in the result
  for (const auto& field : result_table->schema()->fields()) {
    std::string field_name = field->name();
    ASSERT_TRUE(field_name.find("u.") == 0 || field_name.find("f.") == 0)
        << "Found unexpected column: " << field_name;
  }

  // Verify no company (c.*) columns are present
  bool has_company_column = false;
  for (const auto& field : result_table->schema()->fields()) {
    if (field->name().find("c.") == 0) {
      has_company_column = true;
      break;
    }
  }
  ASSERT_FALSE(has_company_column) << "Company columns should be filtered out";

  // Verify we have the expected number of rows (same as without SELECT)
  ASSERT_EQ(result_table->num_rows(), 1)
      << "Should have 1 row for alex->bob->google";

  // Verify we have expected data in the columns
  auto u_id_col = result_table->GetColumnByName("u.id");
  auto u_name_col = result_table->GetColumnByName("u.name");
  auto f_id_col = result_table->GetColumnByName("f.id");
  auto f_name_col = result_table->GetColumnByName("f.name");

  ASSERT_NE(u_id_col, nullptr);
  ASSERT_NE(u_name_col, nullptr);
  ASSERT_NE(f_id_col, nullptr);
  ASSERT_NE(f_name_col, nullptr);

  // Check u.id (should be 0 - alex)
  auto u_id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      u_id_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(u_id_scalar->value, 0);

  // Check u.name (should be "alex")
  auto u_name_scalar = std::static_pointer_cast<arrow::StringScalar>(
      u_name_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(u_name_scalar->view(), "alex");

  // Check f.id (should be 1 - bob)
  auto f_id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      f_id_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(f_id_scalar->value, 1);

  // Check f.name (should be "bob")
  auto f_name_scalar = std::static_pointer_cast<arrow::StringScalar>(
      f_name_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(f_name_scalar->view(), "bob");
}

// Test for specific column selection
TEST(JoinTest, SelectSpecificColumns) {
  auto db = setup_test_db();

  // Create some connections for our test
  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff

  // Query with SELECT for specific columns
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .select({"u.name", "f.age"})  // Only select specific columns
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "SelectSpecificColumns Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Verify we have exactly 2 columns
  ASSERT_EQ(result_table->schema()->num_fields(), 2);

  // Verify the column names are exactly what we requested
  ASSERT_EQ(result_table->schema()->field(0)->name(), "u.name");
  ASSERT_EQ(result_table->schema()->field(1)->name(), "f.age");

  // Verify we have 2 rows (alex->bob and alex->jeff)
  ASSERT_EQ(result_table->num_rows(), 2);

  // Find row with f.age = 31 (bob)
  int bob_row = -1;
  int jeff_row = -1;

  auto f_age_col = result_table->GetColumnByName("f.age");
  for (int i = 0; i < result_table->num_rows(); ++i) {
    auto age_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
        f_age_col->GetScalar(i).ValueOrDie());
    if (age_scalar->value == 31) {
      bob_row = i;  // Found bob's row
    } else if (age_scalar->value == 33) {
      jeff_row = i;  // Found jeff's row
    }
  }

  ASSERT_NE(bob_row, -1) << "Should have a row with bob (age 31)";
  ASSERT_NE(jeff_row, -1) << "Should have a row with jeff (age 33)";

  // Verify alex's name appears in both rows
  auto u_name_col = result_table->GetColumnByName("u.name");
  auto bob_u_name = std::static_pointer_cast<arrow::StringScalar>(
      u_name_col->GetScalar(bob_row).ValueOrDie());
  auto jeff_u_name = std::static_pointer_cast<arrow::StringScalar>(
      u_name_col->GetScalar(jeff_row).ValueOrDie());

  ASSERT_EQ(bob_u_name->view(), "alex");
  ASSERT_EQ(jeff_u_name->view(), "alex");
}

TEST(JoinTest, MultiPatternPathThroughFriends) {
  auto db = setup_test_db();

  // Create the test data as described
  // Users: Alex(0), Bob(1), Jeff(2), Sam(3)
  // Companies: Google(6), IBM(5), AWS(7)

  // Reset database and create our specific schema and nodes
  auto db_path = "test_db_" + std::to_string(now_millis());
  auto config = make_config()
                    .with_db_path(db_path)
                    .with_shard_capacity(1000)
                    .with_chunk_size(1000)
                    .build();

  auto db_custom = std::make_shared<Database>(config);

  // Create User schema
  {
    auto name_field = arrow::field("name", arrow::utf8());
    auto age_field = arrow::field("age", arrow::int64());
    auto user_schema = arrow::schema({name_field, age_field});
    db_custom->get_schema_registry()->create("User", user_schema).ValueOrDie();
  }

  // Create Company schema
  {
    auto name_field = arrow::field("name", arrow::utf8());
    auto size_field = arrow::field("size", arrow::int64());
    auto company_schema = arrow::schema({name_field, size_field});
    db_custom->get_schema_registry()
        ->create("Company", company_schema)
        .ValueOrDie();
  }

  // Create User nodes
  std::vector<std::shared_ptr<Node>> user_nodes;
  {
    // Alex - ID 0
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Alex"}}, {"age", Value{int64_t(25)}}};
      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }

    // Bob - ID 1
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Bob"}}, {"age", Value{int64_t(30)}}};
      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }

    // Charlie - ID 2
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Charlie"}}, {"age", Value{int64_t(35)}}};
      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }

    // Sam - ID 3
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Sam"}}, {"age", Value{int64_t(21)}}};
      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }
  }

  // Create Company nodes
  std::vector<std::shared_ptr<Node>> company_nodes;
  {
    // Google - ID 4
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Google"}}, {"size", Value{int64_t(3000)}}};
      auto node = db_custom->create_node("Company", data).ValueOrDie();
      company_nodes.push_back(node);
    }

    // IBM - ID 5
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"IBM"}}, {"size", Value{int64_t(1000)}}};
      auto node = db_custom->create_node("Company", data).ValueOrDie();
      company_nodes.push_back(node);
    }

    // AWS - ID 6
    {
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"AWS"}}, {"size", Value{int64_t(2000)}}};
      auto node = db_custom->create_node("Company", data).ValueOrDie();
      company_nodes.push_back(node);
    }
  }

  // Create relationships
  db_custom->connect(0, "FRIEND", 1).ValueOrDie();    // Alex -> Bob
  db_custom->connect(1, "FRIEND", 0).ValueOrDie();    // Bob -> Alex
  db_custom->connect(0, "WORKS_AT", 0).ValueOrDie();  // Alex -> Google (Company ID 0)
  db_custom->connect(1, "WORKS_AT", 1).ValueOrDie();  // Bob -> IBM (Company ID 1)

  // Run the query: MATCH (u:User)-[:FRIEND INNER]->(f:User), (f)-[:WORKS_AT
  // INNER]->(c:Company)
  Query query_custom =
      Query::from("u:User")
          .traverse("u", "FRIEND", "f:User", TraverseType::Inner)
          .traverse("f", "WORKS_AT", "c:Company", TraverseType::Inner)
          .build();

  auto query_result_custom = db_custom->query(query_custom);
  ASSERT_TRUE(query_result_custom.ok());

  auto result_table_custom = query_result_custom.ValueOrDie()->table();
  ASSERT_NE(result_table_custom, nullptr);

  // Print the results
  std::cout << "MultiPatternPathThroughFriends Result Table:" << std::endl;
  print_table(result_table_custom);
  static_cast<void>(arrow::PrettyPrint(*result_table_custom, {}, &std::cout));

  // We should have exactly 2 rows:
  // 1. Alex -> Bob -> IBM
  // 2. Bob -> Alex -> Google
  ASSERT_EQ(result_table_custom->num_rows(), 2);

  // Find and verify the Alex -> Bob -> IBM row
  bool found_alex_bob_ibm = false;
  bool found_bob_alex_google = false;

  std::cout << "\nChecking rows for expected patterns:" << std::endl;
  for (int i = 0; i < result_table_custom->num_rows(); i++) {
    auto u_name_col = result_table_custom->GetColumnByName("u.name");
    auto f_name_col = result_table_custom->GetColumnByName("f.name");
    auto c_name_col = result_table_custom->GetColumnByName("c.name");

    auto u_name = std::static_pointer_cast<arrow::StringScalar>(
                      u_name_col->GetScalar(i).ValueOrDie())
                      ->view();
    auto f_name = std::static_pointer_cast<arrow::StringScalar>(
                      f_name_col->GetScalar(i).ValueOrDie())
                      ->view();
    auto c_name = std::static_pointer_cast<arrow::StringScalar>(
                      c_name_col->GetScalar(i).ValueOrDie())
                      ->view();

    std::cout << "Row " << i << ": User=" << u_name << ", Friend=" << f_name
              << ", Company=" << c_name << std::endl;

    // Use case-insensitive comparison
    std::string u_name_str(u_name);
    std::string f_name_str(f_name);
    std::string c_name_str(c_name);

    // Convert to lowercase for case-insensitive comparison
    std::transform(u_name_str.begin(), u_name_str.end(), u_name_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    std::transform(f_name_str.begin(), f_name_str.end(), f_name_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    std::transform(c_name_str.begin(), c_name_str.end(), c_name_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if ((u_name_str == "alex" && f_name_str == "bob" && c_name_str == "ibm") ||
        (u_name == "Alex" && f_name == "Bob" && c_name == "IBM")) {
      found_alex_bob_ibm = true;
      std::cout << "  ✓ Found Alex->Bob->IBM pattern" << std::endl;
    }

    if ((u_name_str == "bob" && f_name_str == "alex" &&
         c_name_str == "google") ||
        (u_name == "Bob" && f_name == "Alex" && c_name == "Google")) {
      found_bob_alex_google = true;
      std::cout << "  ✓ Found Bob->Alex->Google pattern" << std::endl;
    }
  }

  // Let's also try ID-based comparison
  std::cout << "\nTrying ID-based pattern matching:" << std::endl;
  for (int i = 0; i < result_table_custom->num_rows(); i++) {
    auto u_id_col = result_table_custom->GetColumnByName("u.id");
    auto f_id_col = result_table_custom->GetColumnByName("f.id");
    auto c_id_col = result_table_custom->GetColumnByName("c.id");

    auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                    u_id_col->GetScalar(i).ValueOrDie())
                    ->value;
    auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                    f_id_col->GetScalar(i).ValueOrDie())
                    ->value;
    auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                    c_id_col->GetScalar(i).ValueOrDie())
                    ->value;

    std::cout << "Row " << i << ": u.id=" << u_id << ", f.id=" << f_id
              << ", c.id=" << c_id << std::endl;

    if (u_id == 0 && f_id == 1 && c_id == 1) {
      found_alex_bob_ibm = true;
      std::cout << "  ✓ Found Alex(ID=0)->Bob(ID=1)->IBM(ID=1) pattern by ID"
                << std::endl;
    }

    if (u_id == 1 && f_id == 0 && c_id == 0) {
      found_bob_alex_google = true;
      std::cout << "  ✓ Found Bob(ID=1)->Alex(ID=0)->Google(ID=0) pattern by ID"
                << std::endl;
    }
  }

  ASSERT_TRUE(found_alex_bob_ibm)
      << "Missing expected result: Alex -> Bob -> IBM";
  ASSERT_TRUE(found_bob_alex_google)
      << "Missing expected result: Bob -> Alex -> Google";

  // Skip detailed field validation since we've already verified the patterns
}

TEST(JoinTest, MultiPatternWithSharedVars) {
  auto db = setup_test_db();
  db->connect(0, "FRIEND", 1).ValueOrDie();    // Alex -> Bob
  db->connect(0, "FRIEND", 2).ValueOrDie();    // Alex -> Jeff
  db->connect(0, "WORKS_AT", 1).ValueOrDie();  // Alex -> Google (Company ID 1)
  db->connect(2, "WORKS_AT", 1).ValueOrDie();  // Jeff -> Google (Company ID 1)
  db->connect(1, "WORKS_AT", 0).ValueOrDie();  // Bob -> IBM (Company ID 0)

  Query query = Query::from("u:users")
                    .traverse("u", "FRIEND", "f:users")
                    .traverse("f", "WORKS_AT", "c:companies")
                    .traverse("u", "WORKS_AT", "c")
                    .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "SelfJoinWithLeftJoin Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));
  // 0	alex	25	2	jeff	33	6	google	3000
  ASSERT_EQ(result_table->num_rows(), 1);

  auto u_id_col = result_table->GetColumnByName("u.id");
  auto u_name_col = result_table->GetColumnByName("u.name");
  auto u_age_col = result_table->GetColumnByName("u.age");
  auto f_id_col = result_table->GetColumnByName("f.id");
  auto f_age_col = result_table->GetColumnByName("f.age");
  auto f_name_col = result_table->GetColumnByName("f.name");
  auto c_id_col = result_table->GetColumnByName("c.id");
  auto c_name_col = result_table->GetColumnByName("c.name");

  ASSERT_NE(u_id_col, nullptr);
  ASSERT_NE(u_age_col, nullptr);
  ASSERT_NE(u_name_col, nullptr);
  ASSERT_NE(f_id_col, nullptr);
  ASSERT_NE(f_name_col, nullptr);
  ASSERT_NE(f_age_col, nullptr);

  // Check u.id (should be 0 - alex)
  auto u_id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      u_id_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(u_id_scalar->value, 0);

  // Check u.age (should be 25)
  auto u_age_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      u_age_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(u_age_scalar->value, 25);

  // Check u.name (should be "alex")
  auto u_name_scalar = std::static_pointer_cast<arrow::StringScalar>(
      u_name_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(u_name_scalar->view(), "alex");

  // Check f.id (should be 2 - jeff)
  auto f_id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      f_id_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(f_id_scalar->value, 2);

  // Check f.name (should be "jeff")
  auto f_name_scalar = std::static_pointer_cast<arrow::StringScalar>(
      f_name_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(f_name_scalar->view(), "jeff");

  // Check c.id (should be 1 - google)
  auto c_id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      c_id_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(c_id_scalar->value, 1);

  auto c_name_scalar = std::static_pointer_cast<arrow::StringScalar>(
      c_name_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(c_name_scalar->view(), "google");
}

TEST(JoinTest, FullJoinFriendRelationship) {
  Logger::get_instance().set_level(LogLevel::DEBUG);
  auto db = setup_test_db();

  // Create friend relationships
  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff

  Query query = Query::from("u:users")
                    .traverse("u", "friend", "f:users", TraverseType::Full)
                    .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "FullJoinFriendRelationship Result Table:" << std::endl;
  print_table(result_table);
  static_cast<void>(arrow::PrettyPrint(*result_table, {}, &std::cout));

  // Should have 8 rows total:
  // 2 matched relationships (alex->bob, alex->jeff)
  // 4 unmatched left records (bob, jeff, sam, matt with no outgoing friend
  // edges) 2 unmatched right records (sam, matt with no incoming friend edges)
  ASSERT_EQ(result_table->num_rows(), 8);

  // Helper function to check if a row matches expected values
  auto check_row = [&result_table](int row_idx,
                                   const std::optional<int64_t>& u_id,
                                   const std::optional<std::string>& u_name,
                                   const std::optional<int64_t>& u_age,
                                   const std::optional<int64_t>& f_id,
                                   const std::optional<std::string>& f_name,
                                   const std::optional<int64_t>& f_age) {
    if (u_id.has_value()) {
      auto u_id_col = result_table->GetColumnByName("u.id");
      auto scalar_result = u_id_col->GetScalar(row_idx);
      ASSERT_TRUE(scalar_result.ok());
      ASSERT_TRUE(
          scalar_result.ValueOrDie()->Equals(*arrow::MakeScalar(*u_id)));

      auto u_name_col = result_table->GetColumnByName("u.name");
      scalar_result = u_name_col->GetScalar(row_idx);
      ASSERT_TRUE(scalar_result.ok());
      ASSERT_TRUE(
          scalar_result.ValueOrDie()->Equals(*arrow::MakeScalar(*u_name)));

      auto u_age_col = result_table->GetColumnByName("u.age");
      scalar_result = u_age_col->GetScalar(row_idx);
      ASSERT_TRUE(scalar_result.ok());
      ASSERT_TRUE(
          scalar_result.ValueOrDie()->Equals(*arrow::MakeScalar(*u_age)));
    } else {
      auto u_id_col = result_table->GetColumnByName("u.id");
      ASSERT_TRUE(u_id_col->chunk(0)->IsNull(row_idx));
      auto u_name_col = result_table->GetColumnByName("u.name");
      ASSERT_TRUE(u_name_col->chunk(0)->IsNull(row_idx));
      auto u_age_col = result_table->GetColumnByName("u.age");
      ASSERT_TRUE(u_age_col->chunk(0)->IsNull(row_idx));
    }

    if (f_id.has_value()) {
      auto f_id_col = result_table->GetColumnByName("f.id");
      auto scalar_result = f_id_col->GetScalar(row_idx);
      ASSERT_TRUE(scalar_result.ok());
      ASSERT_TRUE(
          scalar_result.ValueOrDie()->Equals(*arrow::MakeScalar(*f_id)));

      auto f_name_col = result_table->GetColumnByName("f.name");
      scalar_result = f_name_col->GetScalar(row_idx);
      ASSERT_TRUE(scalar_result.ok());
      ASSERT_TRUE(
          scalar_result.ValueOrDie()->Equals(*arrow::MakeScalar(*f_name)));

      auto f_age_col = result_table->GetColumnByName("f.age");
      scalar_result = f_age_col->GetScalar(row_idx);
      ASSERT_TRUE(scalar_result.ok());
      ASSERT_TRUE(
          scalar_result.ValueOrDie()->Equals(*arrow::MakeScalar(*f_age)));
    } else {
      auto f_id_col = result_table->GetColumnByName("f.id");
      ASSERT_TRUE(f_id_col->chunk(0)->IsNull(row_idx));
      auto f_name_col = result_table->GetColumnByName("f.name");
      ASSERT_TRUE(f_name_col->chunk(0)->IsNull(row_idx));
      auto f_age_col = result_table->GetColumnByName("f.age");
      ASSERT_TRUE(f_age_col->chunk(0)->IsNull(row_idx));
    }
  };

  // Check matched relationships
  check_row(0, 0, "alex", 25, 1, "bob", 31);   // alex -> bob
  check_row(1, 0, "alex", 25, 2, "jeff", 33);  // alex -> jeff

  // Check unmatched left records (users with no outgoing friend edges)
  check_row(2, 1, "bob", 31, std::nullopt, std::nullopt, std::nullopt);  // bob
  check_row(3, 2, "jeff", 33, std::nullopt, std::nullopt,
            std::nullopt);                                               // jeff
  check_row(4, 3, "sam", 21, std::nullopt, std::nullopt, std::nullopt);  // sam
  check_row(5, 4, "matt", 40, std::nullopt, std::nullopt,
            std::nullopt);  // matt

  // Check unmatched right records (users with no incoming friend edges)
  check_row(6, std::nullopt, std::nullopt, std::nullopt, 3, "sam", 21);  // sam
  check_row(7, std::nullopt, std::nullopt, std::nullopt, 4, "matt",
            40);  // matt
}

}  // namespace tundradb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Logger::get_instance().set_level(LogLevel::DEBUG);
  return RUN_ALL_TESTS();
}