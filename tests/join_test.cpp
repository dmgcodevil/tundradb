#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "../include/core.hpp"
#include "../include/logger.hpp"
#include "../include/metadata.hpp"
#include "../include/query.hpp"

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
    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    name_builder.Append(user.name);
    name_builder.Finish(&name_array);

    // Create age array
    arrow::Int64Builder age_builder;
    std::shared_ptr<arrow::Array> age_array;
    age_builder.Append(user.age);
    age_builder.Finish(&age_array);

    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
        {"name", name_array}, {"age", age_array}};

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
    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    name_builder.Append(company.name);
    name_builder.Finish(&name_array);

    // Create age array
    arrow::Int64Builder size_builder;
    std::shared_ptr<arrow::Array> size_array;
    size_builder.Append(company.size);
    size_builder.Finish(&size_array);

    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
        {"name", name_array}, {"size", size_array}};

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);
  ASSERT_EQ(result_table->num_rows(), 5);
}

TEST(JoinTest, UserFriendCompanyInnerJoin) {
  auto db = setup_test_db();

  db->connect(0, "friend", 1).ValueOrDie();
  db->connect(1, "works-at", 6).ValueOrDie();

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

  ASSERT_EQ(result_table->num_rows(), 1);

  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>>
      expected_cells;

  expected_cells["u.id"] = arrow::MakeScalar((int64_t)0);
  expected_cells["u.name"] = arrow::MakeScalar("alex");
  expected_cells["u.age"] = arrow::MakeScalar((int64_t)25);

  expected_cells["f.id"] = arrow::MakeScalar((int64_t)1);
  expected_cells["f.name"] = arrow::MakeScalar("bob");
  expected_cells["f.age"] = arrow::MakeScalar((int64_t)31);

  expected_cells["c.id"] = arrow::MakeScalar((int64_t)6);
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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  db->connect(0, "works-at", 6).ValueOrDie();  // alex -> google

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
    expected_row1["c.id"] = arrow::MakeScalar((int64_t)6);
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
    expected_row2["c.id"] = arrow::MakeScalar((int64_t)6);
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
  db->connect(0, "works-at", 5).ValueOrDie();  // alex -> ibm
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
  db->connect(2, "works-at", 7).ValueOrDie();  // jeff -> aws

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
    expected_row1["c.id"] = arrow::MakeScalar((int64_t)5);
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
    expected_row2["c.id"] = arrow::MakeScalar((int64_t)5);
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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

  // Should have 0 rows due to inner join failing at the last hop
  ASSERT_EQ(result_table->num_rows(), 0);
}

TEST(JoinTest, MultiPathToSameTarget) {
  auto db = setup_test_db();
  // Create multiple paths to the same target
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(0, "works-at", 5).ValueOrDie();  // alex -> ibm
  db->connect(1, "works-at", 5)
      .ValueOrDie();  // bob -> ibm (same company as alex)
  db->connect(2, "works-at", 6).ValueOrDie();  // jeff -> google

  // Query: Find all friends of alex who work at the same company as alex
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("u", "works-at", "c1:companies", TraverseType::Inner)
          .traverse("f", "works-at", "c2:companies", TraverseType::Inner)
          .where("c1.id", CompareOp::Eq,
                 Value((int64_t)5))  // Filter for alex's company (IBM)
          .where("c2.id", CompareOp::Eq,
                 Value((int64_t)5))  // Filter for friend's company (also IBM)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "MultiPathToSameTarget Result Table:" << std::endl;
  print_table(result_table);
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  expected_row["c1.id"] = arrow::MakeScalar((int64_t)5);
  expected_row["c1.name"] = arrow::MakeScalar("ibm");
  expected_row["c1.size"] = arrow::MakeScalar((int64_t)1000);
  // c2 is also IBM (bob's company)
  expected_row["c2.id"] = arrow::MakeScalar((int64_t)5);
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

  db->connect(1, "works-at", 5).ValueOrDie();  // bob -> ibm
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google

  db->connect(2, "works-at", 6).ValueOrDie();  // jeff -> google
  db->connect(2, "works-at", 7).ValueOrDie();  // jeff -> aws

  db->connect(3, "works-at", 5).ValueOrDie();  // sam -> ibm
  db->connect(3, "works-at", 7).ValueOrDie();  // sam -> aws

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  bob_row["c.id"] = arrow::MakeScalar((int64_t)6);
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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
  db->connect(2, "works-at", 7).ValueOrDie();  // jeff -> aws
  // Sam (id=3) has no friends but works at ibm
  db->connect(3, "works-at", 5).ValueOrDie();  // sam -> ibm

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
  // jeff has no company

  // Create a row for matt who has no friends
  db->connect(4, "works-at", 5).ValueOrDie();  // matt -> ibm

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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

      if (u_id == 0 && f_id == 1 && c_id == 6) {
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

      if (c_id == 7) {
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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
  db->connect(2, "likes", 5).ValueOrDie();     // jeff -> ibm
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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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

      if (u_id == 0 && f_id == 1 && c_id == 6) {
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

      if (u_id == 0 && f_id == 2 && l_id == 5) {
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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google
  // jeff has no company

  // matt (id=4) has no friends but works at ibm
  db->connect(4, "works-at", 5).ValueOrDie();  // matt -> ibm

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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

      if (u_id == 0 && f_id == 1 && c_id == 6) {
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

      if (c_id == 7) {
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
  db->connect(1, "works-at", 6).ValueOrDie();  // bob -> google

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;
      std::shared_ptr<arrow::Array> name_array, age_array;

      name_builder.Append("Alex");
      name_builder.Finish(&name_array);

      age_builder.Append(25);
      age_builder.Finish(&age_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"age", age_array}};

      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }

    // Bob - ID 1
    {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;
      std::shared_ptr<arrow::Array> name_array, age_array;

      name_builder.Append("Bob");
      name_builder.Finish(&name_array);

      age_builder.Append(31);
      age_builder.Finish(&age_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"age", age_array}};

      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }

    // Jeff - ID 2
    {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;
      std::shared_ptr<arrow::Array> name_array, age_array;

      name_builder.Append("Jeff");
      name_builder.Finish(&name_array);

      age_builder.Append(33);
      age_builder.Finish(&age_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"age", age_array}};

      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }

    // Sam - ID 3
    {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;
      std::shared_ptr<arrow::Array> name_array, age_array;

      name_builder.Append("Sam");
      name_builder.Finish(&name_array);

      age_builder.Append(21);
      age_builder.Finish(&age_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"age", age_array}};

      auto node = db_custom->create_node("User", data).ValueOrDie();
      user_nodes.push_back(node);
    }
  }

  // Create Company nodes
  std::vector<std::shared_ptr<Node>> company_nodes;
  {
    // Google - ID 4
    {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder size_builder;
      std::shared_ptr<arrow::Array> name_array, size_array;

      name_builder.Append("Google");
      name_builder.Finish(&name_array);

      size_builder.Append(3000);
      size_builder.Finish(&size_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"size", size_array}};

      auto node = db_custom->create_node("Company", data).ValueOrDie();
      company_nodes.push_back(node);
    }

    // IBM - ID 5
    {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder size_builder;
      std::shared_ptr<arrow::Array> name_array, size_array;

      name_builder.Append("IBM");
      name_builder.Finish(&name_array);

      size_builder.Append(1000);
      size_builder.Finish(&size_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"size", size_array}};

      auto node = db_custom->create_node("Company", data).ValueOrDie();
      company_nodes.push_back(node);
    }

    // AWS - ID 6
    {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder size_builder;
      std::shared_ptr<arrow::Array> name_array, size_array;

      name_builder.Append("AWS");
      name_builder.Finish(&name_array);

      size_builder.Append(2000);
      size_builder.Finish(&size_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"size", size_array}};

      auto node = db_custom->create_node("Company", data).ValueOrDie();
      company_nodes.push_back(node);
    }
  }

  // Create relationships
  db_custom->connect(0, "FRIEND", 1).ValueOrDie();    // Alex -> Bob
  db_custom->connect(1, "FRIEND", 0).ValueOrDie();    // Bob -> Alex
  db_custom->connect(0, "WORKS_AT", 4).ValueOrDie();  // Alex -> Google
  db_custom->connect(1, "WORKS_AT", 5).ValueOrDie();  // Bob -> IBM

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
  arrow::PrettyPrint(*result_table_custom, {}, &std::cout);

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

    if (u_id == 0 && f_id == 1 && c_id == 5) {
      found_alex_bob_ibm = true;
      std::cout << "  ✓ Found Alex(ID=0)->Bob(ID=1)->IBM(ID=5) pattern by ID"
                << std::endl;
    }

    if (u_id == 1 && f_id == 0 && c_id == 4) {
      found_bob_alex_google = true;
      std::cout << "  ✓ Found Bob(ID=1)->Alex(ID=0)->Google(ID=4) pattern by ID"
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
  db->connect(0, "WORKS_AT", 6).ValueOrDie();  // Alex -> Google
  db->connect(2, "WORKS_AT", 6).ValueOrDie();  // Jeff -> Google
  db->connect(1, "WORKS_AT", 5).ValueOrDie();

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
  arrow::PrettyPrint(*result_table, {}, &std::cout);
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

  // Check c.id (should be 6 - google)
  auto c_id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(
      c_id_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(c_id_scalar->value, 6);

  auto c_name_scalar = std::static_pointer_cast<arrow::StringScalar>(
      c_name_col->GetScalar(0).ValueOrDie());
  ASSERT_EQ(c_name_scalar->view(), "google");
}

TEST(JoinTest, FullJoinFriendRelationship) {
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
  arrow::PrettyPrint(*result_table, {}, &std::cout);

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

TEST(JoinTest, FullJoinMultiSchemaCornerCase) {
  auto db = setup_test_db();

  // Create a complex scenario with multiple schemas and FULL JOINs
  // This test is designed to expose potential issues with the FULL JOIN
  // implementation

  // Friend relationships (users -> users)
  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(1, "friend", 2).ValueOrDie();  // bob -> jeff

  // Work relationships (users -> companies)
  db->connect(0, "works-at", 5).ValueOrDie();  // alex -> ibm
  db->connect(2, "works-at", 6).ValueOrDie();  // jeff -> google
  // Note: bob (id=1) doesn't work anywhere
  // Note: sam (id=3) and matt (id=4) don't have friends or work
  // Note: aws (id=7) has no employees

  // Query with chained FULL JOINs: users -> friends -> companies
  // This should test the multi-schema FULL JOIN logic
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
  std::cout << "FullJoinMultiSchemaCornerCase Result Table:" << std::endl;
  print_table(result_table);
  arrow::PrettyPrint(*result_table, {}, &std::cout);

  // Expected results for FULL JOIN chain:
  // 1. Matched paths: alex->bob->NULL, bob->jeff->google
  // 2. Left-side unmatched from first join: jeff, sam, matt (with NULL friends)
  // 3. Right-side unmatched from first join: sam, matt (as unmatched friends)
  // 4. Left-side unmatched from second join: users/friends with no companies
  // 5. Right-side unmatched from second join: companies with no employees (ibm,
  // aws)

  // This is a complex scenario that should test edge cases in the FULL JOIN
  // logic

  // Let's verify some key patterns exist:
  bool found_alex_bob_null = false;  // alex -> bob -> NULL (bob has no company)
  bool found_bob_jeff_google = false;  // bob -> jeff -> google
  bool found_unmatched_companies =
      false;  // NULL -> NULL -> company (unmatched companies)
  bool found_unmatched_users = false;  // user -> NULL -> NULL (unmatched users)

  std::cout << "Analyzing result patterns:" << std::endl;

  for (int i = 0; i < result_table->num_rows(); i++) {
    auto u_id_col = result_table->GetColumnByName("u.id");
    auto f_id_col = result_table->GetColumnByName("f.id");
    auto c_id_col = result_table->GetColumnByName("c.id");

    // Get values (handling NULLs)
    std::optional<int64_t> u_id, f_id, c_id;

    if (!u_id_col->chunk(0)->IsNull(i)) {
      u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                 u_id_col->GetScalar(i).ValueOrDie())
                 ->value;
    }

    if (!f_id_col->chunk(0)->IsNull(i)) {
      f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                 f_id_col->GetScalar(i).ValueOrDie())
                 ->value;
    }

    if (!c_id_col->chunk(0)->IsNull(i)) {
      c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                 c_id_col->GetScalar(i).ValueOrDie())
                 ->value;
    }

    std::cout << "Row " << i
              << ": u=" << (u_id ? std::to_string(*u_id) : "NULL")
              << " f=" << (f_id ? std::to_string(*f_id) : "NULL")
              << " c=" << (c_id ? std::to_string(*c_id) : "NULL") << std::endl;

    // Check for specific patterns
    if (u_id == 0 && f_id == 1 && !c_id.has_value()) {
      found_alex_bob_null = true;
      std::cout << "  ✓ Found alex->bob->NULL pattern" << std::endl;
    }

    if (u_id == 1 && f_id == 2 && c_id == 6) {
      found_bob_jeff_google = true;
      std::cout << "  ✓ Found bob->jeff->google pattern" << std::endl;
    }

    if (!u_id.has_value() && !f_id.has_value() && c_id.has_value()) {
      found_unmatched_companies = true;
      std::cout << "  ✓ Found unmatched company pattern (NULL->NULL->company)"
                << std::endl;
    }

    if (u_id.has_value() && !f_id.has_value() && !c_id.has_value()) {
      found_unmatched_users = true;
      std::cout << "  ✓ Found unmatched user pattern (user->NULL->NULL)"
                << std::endl;
    }
  }

  // Verify that FULL JOIN includes all expected patterns
  ASSERT_TRUE(found_alex_bob_null)
      << "Should include alex->bob->NULL (bob has no company)";
  ASSERT_TRUE(found_bob_jeff_google)
      << "Should include bob->jeff->google (complete path)";
  ASSERT_TRUE(found_unmatched_companies)
      << "Should include unmatched companies (NULL->NULL->company)";
  ASSERT_TRUE(found_unmatched_users)
      << "Should include unmatched users (user->NULL->NULL)";

  // The exact number of rows depends on the complex FULL JOIN logic,
  // but we should have at least these key patterns
  ASSERT_GE(result_table->num_rows(), 4)
      << "Should have at least the key patterns we're testing for";
}

TEST(JoinTest, FullJoinAfterInnerJoinTricky) {
  auto db = setup_test_db();

  // Create a scenario where INNER JOIN filters heavily, then FULL JOIN should
  // expand This tests if FULL JOIN correctly handles a reduced intermediate
  // result set

  // Only alex has friends
  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff

  // Only some friends work at companies
  db->connect(1, "works-at", 5).ValueOrDie();  // bob -> ibm
  // jeff doesn't work anywhere

  // Companies that no one works at
  // ibm (id=5) has bob, google (id=6) and aws (id=7) have no employees

  // Query: Start with users, INNER join to friends (filters to only alex),
  // then FULL join to companies (should include all companies, even unmatched
  // ones)
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users",
                    TraverseType::Inner)  // Only alex has friends
          .traverse("f", "works-at", "c:companies",
                    TraverseType::Full)  // Should include all companies
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  std::cout << "FullJoinAfterInnerJoinTricky Result Table:" << std::endl;
  print_table(result_table);

  // Expected results:
  // 1. alex -> bob -> ibm (matched path)
  // 2. alex -> jeff -> NULL (jeff has no company)
  // 3. NULL -> NULL -> google (unmatched company)
  // 4. NULL -> NULL -> aws (unmatched company)

  // Verify we have all companies represented
  std::set<std::string> found_companies;
  auto c_name_col = result_table->GetColumnByName("c.name");

  for (int i = 0; i < result_table->num_rows(); i++) {
    if (!c_name_col->chunk(0)->IsNull(i)) {
      auto company_name = std::static_pointer_cast<arrow::StringScalar>(
                              c_name_col->GetScalar(i).ValueOrDie())
                              ->ToString();
      found_companies.insert(company_name);
    }
  }

  ASSERT_TRUE(found_companies.find("ibm") != found_companies.end())
      << "Should include IBM (matched)";
  ASSERT_TRUE(found_companies.find("google") != found_companies.end())
      << "Should include Google (unmatched in FULL JOIN)";
  ASSERT_TRUE(found_companies.find("aws") != found_companies.end())
      << "Should include AWS (unmatched in FULL JOIN)";

  // Should have at least 4 rows (the patterns mentioned above)
  ASSERT_GE(result_table->num_rows(), 4);
}

TEST(JoinTest, InnerJoinAfterFullJoinTricky) {
  auto db = setup_test_db();

  // Create a scenario where FULL JOIN expands the result set,
  // then INNER JOIN should filter it down significantly

  db->connect(0, "friend", 1).ValueOrDie();  // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();  // alex -> jeff

  // Only bob works at a company that has a "partner" relationship
  db->connect(1, "works-at", 5).ValueOrDie();  // bob -> ibm
  db->connect(5, "partner", 6)
      .ValueOrDie();  // ibm -> google (partner relationship)

  // jeff works at google but google has no partners
  db->connect(2, "works-at", 6).ValueOrDie();  // jeff -> google

  // Query: FULL join to companies (includes all), then INNER join to partners
  // (filters heavily)
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Inner)
          .traverse("f", "works-at", "c:companies", TraverseType::Full)
          .traverse("c", "partner", "p:companies",
                    TraverseType::Inner)  // Only companies with partners
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  std::cout << "InnerJoinAfterFullJoinTricky Result Table:" << std::endl;
  print_table(result_table);

  // Expected: Only alex -> bob -> ibm -> google should remain
  // The FULL JOIN would have included unmatched companies, but the subsequent
  // INNER JOIN should filter out everything except paths that lead to companies
  // with partners

  ASSERT_EQ(result_table->num_rows(), 1)
      << "Should have exactly 1 row after INNER JOIN filtering";

  // Verify it's the alex -> bob -> ibm -> google path
  auto u_id_col = result_table->GetColumnByName("u.id");
  auto f_id_col = result_table->GetColumnByName("f.id");
  auto c_id_col = result_table->GetColumnByName("c.id");
  auto p_id_col = result_table->GetColumnByName("p.id");

  auto u_id = std::static_pointer_cast<arrow::Int64Scalar>(
                  u_id_col->GetScalar(0).ValueOrDie())
                  ->value;
  auto f_id = std::static_pointer_cast<arrow::Int64Scalar>(
                  f_id_col->GetScalar(0).ValueOrDie())
                  ->value;
  auto c_id = std::static_pointer_cast<arrow::Int64Scalar>(
                  c_id_col->GetScalar(0).ValueOrDie())
                  ->value;
  auto p_id = std::static_pointer_cast<arrow::Int64Scalar>(
                  p_id_col->GetScalar(0).ValueOrDie())
                  ->value;

  ASSERT_EQ(u_id, 0) << "Should be alex";
  ASSERT_EQ(f_id, 1) << "Should be bob";
  ASSERT_EQ(c_id, 5) << "Should be ibm";
  ASSERT_EQ(p_id, 6) << "Should be google";
}

TEST(JoinTest, FullJoinWithSelfReferenceTricky) {
  auto db = setup_test_db();

  // Create a tricky scenario with self-referencing relationships and FULL JOIN
  // This tests if the FULL JOIN logic correctly handles when source and target
  // schemas are the same

  // Create a management hierarchy
  db->connect(0, "manages", 1).ValueOrDie();  // alex manages bob
  db->connect(1, "manages", 2).ValueOrDie();  // bob manages jeff
  db->connect(0, "manages", 3).ValueOrDie();  // alex also manages sam directly

  // matt (id=4) is not in any management relationship

  // FULL JOIN on self-referencing relationship
  // This should include:
  // 1. All actual management relationships
  // 2. All users who don't manage anyone (as left-side unmatched)
  // 3. All users who aren't managed by anyone (as right-side unmatched)
  Query query =
      Query::from("manager:users")
          .traverse("manager", "manages", "employee:users", TraverseType::Full)
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  std::cout << "FullJoinWithSelfReferenceTricky Result Table:" << std::endl;
  print_table(result_table);

  // Expected patterns:
  // 1. alex -> bob (actual management)
  // 2. bob -> jeff (actual management)
  // 3. alex -> sam (actual management)
  // 4. jeff -> NULL (jeff doesn't manage anyone)
  // 5. sam -> NULL (sam doesn't manage anyone)
  // 6. matt -> NULL (matt doesn't manage anyone)
  // 7. NULL -> alex (alex isn't managed by anyone in our data)
  // 8. NULL -> matt (matt isn't managed by anyone)

  // Count different patterns
  int actual_management_count = 0;
  int unmatched_managers_count = 0;   // managers with no employees
  int unmatched_employees_count = 0;  // employees with no managers

  for (int i = 0; i < result_table->num_rows(); i++) {
    auto manager_id_col = result_table->GetColumnByName("manager.id");
    auto employee_id_col = result_table->GetColumnByName("employee.id");

    bool manager_null = manager_id_col->chunk(0)->IsNull(i);
    bool employee_null = employee_id_col->chunk(0)->IsNull(i);

    if (!manager_null && !employee_null) {
      actual_management_count++;
    } else if (!manager_null && employee_null) {
      unmatched_managers_count++;
    } else if (manager_null && !employee_null) {
      unmatched_employees_count++;
    }
  }

  ASSERT_EQ(actual_management_count, 3)
      << "Should have 3 actual management relationships";
  ASSERT_GE(unmatched_managers_count, 3)
      << "Should have at least 3 managers with no employees (jeff, sam, matt)";
  ASSERT_GE(unmatched_employees_count, 2)
      << "Should have at least 2 employees with no managers (alex, matt)";
}

TEST(JoinTest, ChainedFullJoinsTricky) {
  auto db = setup_test_db();

  // Create a scenario with multiple chained FULL JOINs
  // This is the most complex case and most likely to expose bugs

  // Create sparse relationships across three schemas
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(1, "works-at", 5).ValueOrDie();  // bob -> ibm
  db->connect(5, "located-in", 0)
      .ValueOrDie();  // ibm -> alex (reusing user as location)

  // Leave many entities unconnected to test FULL JOIN behavior
  // jeff, sam, matt have no friends
  // google, aws have no employees
  // jeff, sam, matt are not locations for any company

  // Query with chained FULL JOINs across different schemas
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Full)
          .traverse("f", "works-at", "c:companies", TraverseType::Full)
          .traverse("c", "located-in", "l:users",
                    TraverseType::Full)  // Reuse users as locations
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  std::cout << "ChainedFullJoinsTricky Result Table:" << std::endl;
  print_table(result_table);

  // This should produce a complex result set with many NULL combinations
  // The key test is that it doesn't crash and includes representatives from all
  // schemas

  // Verify we have data from all schemas
  std::set<int64_t> user_ids, friend_ids, company_ids, location_ids;

  auto u_id_col = result_table->GetColumnByName("u.id");
  auto f_id_col = result_table->GetColumnByName("f.id");
  auto c_id_col = result_table->GetColumnByName("c.id");
  auto l_id_col = result_table->GetColumnByName("l.id");

  for (int i = 0; i < result_table->num_rows(); i++) {
    if (!u_id_col->chunk(0)->IsNull(i)) {
      user_ids.insert(std::static_pointer_cast<arrow::Int64Scalar>(
                          u_id_col->GetScalar(i).ValueOrDie())
                          ->value);
    }
    if (!f_id_col->chunk(0)->IsNull(i)) {
      friend_ids.insert(std::static_pointer_cast<arrow::Int64Scalar>(
                            f_id_col->GetScalar(i).ValueOrDie())
                            ->value);
    }
    if (!c_id_col->chunk(0)->IsNull(i)) {
      company_ids.insert(std::static_pointer_cast<arrow::Int64Scalar>(
                             c_id_col->GetScalar(i).ValueOrDie())
                             ->value);
    }
    if (!l_id_col->chunk(0)->IsNull(i)) {
      location_ids.insert(std::static_pointer_cast<arrow::Int64Scalar>(
                              l_id_col->GetScalar(i).ValueOrDie())
                              ->value);
    }
  }

  // FULL JOIN should ensure all entities appear somewhere
  ASSERT_EQ(user_ids.size(), 5) << "All 5 users should appear as users";
  ASSERT_EQ(friend_ids.size(), 5) << "All 5 users should appear as friends";
  ASSERT_EQ(company_ids.size(), 3) << "All 3 companies should appear";
  ASSERT_EQ(location_ids.size(), 5) << "All 5 users should appear as locations";

  // Should have a substantial number of rows due to FULL JOINs
  ASSERT_GE(result_table->num_rows(), 10)
      << "Should have many rows due to FULL JOINs";
}

TEST(JoinTest, FullJoinWithFilteringTricky) {
  auto db = setup_test_db();

  // Test FULL JOIN combined with WHERE clauses
  // This tests if filtering interacts correctly with FULL JOIN NULL handling

  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff
  db->connect(1, "works-at", 5).ValueOrDie();  // bob -> ibm

  // Query with FULL JOIN and filtering
  // The WHERE clause should not eliminate the NULL rows from FULL JOIN
  Query query =
      Query::from("u:users")
          .traverse("u", "friend", "f:users", TraverseType::Full)
          .traverse("f", "works-at", "c:companies", TraverseType::Full)
          .where("u.age", CompareOp::Gt,
                 Value((int64_t)20))  // Filter users by age
          .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());

  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  std::cout << "FullJoinWithFilteringTricky Result Table:" << std::endl;
  print_table(result_table);

  // The tricky part: WHERE clauses should not eliminate NULL rows from FULL
  // JOIN We should still see unmatched companies (NULL -> NULL -> company) even
  // though the user age filter is applied

  bool found_unmatched_company = false;
  auto u_id_col = result_table->GetColumnByName("u.id");
  auto c_id_col = result_table->GetColumnByName("c.id");

  for (int i = 0; i < result_table->num_rows(); i++) {
    if (u_id_col->chunk(0)->IsNull(i) && !c_id_col->chunk(0)->IsNull(i)) {
      found_unmatched_company = true;
      break;
    }
  }

  ASSERT_TRUE(found_unmatched_company)
      << "FULL JOIN should still include unmatched companies despite WHERE "
         "clause";
}

}  // namespace tundradb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Logger::getInstance().setLevel(LogLevel::DEBUG);
  return RUN_ALL_TESTS();
}