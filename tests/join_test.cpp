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
  db->get_schema_registry()->add("users", users_schema).ValueOrDie();
  db->get_schema_registry()->add("companies", companies_schema).ValueOrDie();
  const auto users =
      std::vector({User{"alex", 25}, User{"bob", 31}, User{"jeff", 33},
                   User{"sam", 21}, User{"matt", 40}});

  const auto companies = std::vector{
      {Company{"ibm", 1000}, Company{"google", 3000}, Company{"aws", 5000}}};
  create_users(db, users);
  create_companies(db, companies);
  return db;
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

TEST(JoinTest, BidirectionalRelationships) {
  auto db = setup_test_db();
  // Create bidirectional "knows" relationships
  db->connect(0, "knows", 1).ValueOrDie();    // alex knows bob
  db->connect(1, "knows", 0).ValueOrDie();    // bob knows alex
  db->connect(0, "knows", 2).ValueOrDie();    // alex knows jeff
  db->connect(1, "knows", 3).ValueOrDie();    // bob knows sam

  // Find mutual connections (people who know each other)
  Query query = Query::from("u1:users")
                    .traverse("u1", "knows", "u2:users", TraverseType::Inner)
                    .traverse("u2", "knows", "u3:users", TraverseType::Inner)
                    .where("u3.id", CompareOp::Eq, Value((int64_t)0))  // u3 must be alex (ID 0)
                    .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "BidirectionalRelationships Result Table:" << std::endl;
  print_table(result_table);
  arrow::PrettyPrint(*result_table, {}, &std::cout);

  // There should be only one result: bob knows alex and alex knows bob
  ASSERT_EQ(result_table->num_rows(), 1);

  // Verify the mutual relationship: bob -> alex -> bob
  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> expected_row;
  // u1 is bob
  expected_row["u1.id"] = arrow::MakeScalar((int64_t)1);
  expected_row["u1.name"] = arrow::MakeScalar("bob");
  expected_row["u1.age"] = arrow::MakeScalar((int64_t)31);
  // u2 is alex
  expected_row["u2.id"] = arrow::MakeScalar((int64_t)0);
  expected_row["u2.name"] = arrow::MakeScalar("alex");
  expected_row["u2.age"] = arrow::MakeScalar((int64_t)25);
  // u3 is alex (again, filtered by the where clause)
  expected_row["u3.id"] = arrow::MakeScalar((int64_t)0);
  expected_row["u3.name"] = arrow::MakeScalar("alex");
  expected_row["u3.age"] = arrow::MakeScalar((int64_t)25);

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

TEST(JoinTest, MultiLevelJoinChain) {
  auto db = setup_test_db();
  // Create multi-level relationships
  db->connect(0, "friend", 1).ValueOrDie();     // alex -> bob (friend)
  db->connect(1, "works-at", 6).ValueOrDie();   // bob -> google
  db->connect(6, "partner", 5).ValueOrDie();    // google -> ibm (partner)
  db->connect(6, "partner", 7).ValueOrDie();    // google -> aws (partner)

  // Find partners of companies where friends work
  Query query = Query::from("u:users")
                    .traverse("u", "friend", "f:users", TraverseType::Inner)
                    .traverse("f", "works-at", "c1:companies", TraverseType::Inner)
                    .traverse("c1", "partner", "c2:companies", TraverseType::Inner)
                    .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "MultiLevelJoinChain Result Table:" << std::endl;
  print_table(result_table);
  arrow::PrettyPrint(*result_table, {}, &std::cout);

  // Should have 2 rows: alex -> bob -> google -> [ibm, aws]
  ASSERT_EQ(result_table->num_rows(), 2);

  // First row: alex -> bob -> google -> ibm
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> expected_row1;
    expected_row1["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row1["u.name"] = arrow::MakeScalar("alex");
    expected_row1["u.age"] = arrow::MakeScalar((int64_t)25);
    expected_row1["f.id"] = arrow::MakeScalar((int64_t)1);
    expected_row1["f.name"] = arrow::MakeScalar("bob");
    expected_row1["f.age"] = arrow::MakeScalar((int64_t)31);
    expected_row1["c1.id"] = arrow::MakeScalar((int64_t)6);
    expected_row1["c1.name"] = arrow::MakeScalar("google");
    expected_row1["c1.size"] = arrow::MakeScalar((int64_t)3000);
    expected_row1["c2.id"] = arrow::MakeScalar((int64_t)5);
    expected_row1["c2.name"] = arrow::MakeScalar("ibm");
    expected_row1["c2.size"] = arrow::MakeScalar((int64_t)1000);

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

  // Second row: alex -> bob -> google -> aws
  {
    std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> expected_row2;
    expected_row2["u.id"] = arrow::MakeScalar((int64_t)0);
    expected_row2["u.name"] = arrow::MakeScalar("alex");
    expected_row2["u.age"] = arrow::MakeScalar((int64_t)25);
    expected_row2["f.id"] = arrow::MakeScalar((int64_t)1);
    expected_row2["f.name"] = arrow::MakeScalar("bob");
    expected_row2["f.age"] = arrow::MakeScalar((int64_t)31);
    expected_row2["c1.id"] = arrow::MakeScalar((int64_t)6);
    expected_row2["c1.name"] = arrow::MakeScalar("google");
    expected_row2["c1.size"] = arrow::MakeScalar((int64_t)3000);
    expected_row2["c2.id"] = arrow::MakeScalar((int64_t)7);
    expected_row2["c2.name"] = arrow::MakeScalar("aws");
    expected_row2["c2.size"] = arrow::MakeScalar((int64_t)5000);

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

TEST(JoinTest, MultipleRelationshipTypes) {
  auto db = setup_test_db();
  // Create different types of relationships between the same entities
  db->connect(0, "friend", 1).ValueOrDie();    // alex -> bob (friend)
  db->connect(0, "colleague", 1).ValueOrDie(); // alex -> bob (colleague)
  db->connect(0, "friend", 2).ValueOrDie();    // alex -> jeff (friend)
  db->connect(0, "colleague", 3).ValueOrDie(); // alex -> sam (colleague)

  // Find users who are both friends and colleagues
  Query query = Query::from("u1:users")
                    .traverse("u1", "friend", "u2:users", TraverseType::Inner)
                    .traverse("u1", "colleague", "u3:users", TraverseType::Inner)
                    .where("u2.id", CompareOp::Eq, "u3.id")  // Same person
                    .build();

  auto query_result = db->query(query);
  ASSERT_TRUE(query_result.ok());
  auto result_table = query_result.ValueOrDie()->table();
  ASSERT_NE(result_table, nullptr);

  // Pretty print for debugging
  std::cout << "MultipleRelationshipTypes Result Table:" << std::endl;
  print_table(result_table);
  arrow::PrettyPrint(*result_table, {}, &std::cout);

  // Verify that the result has a row for bob who is both friend and colleague
  ASSERT_EQ(result_table->num_rows(), 1);

  std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> expected_row;
  // u1 is alex
  expected_row["u1.id"] = arrow::MakeScalar((int64_t)0);
  expected_row["u1.name"] = arrow::MakeScalar("alex");
  expected_row["u1.age"] = arrow::MakeScalar((int64_t)25);
  // u2 is bob (friend)
  expected_row["u2.id"] = arrow::MakeScalar((int64_t)1);
  expected_row["u2.name"] = arrow::MakeScalar("bob");
  expected_row["u2.age"] = arrow::MakeScalar((int64_t)31);
  // u3 is bob (colleague)
  expected_row["u3.id"] = arrow::MakeScalar((int64_t)1);
  expected_row["u3.name"] = arrow::MakeScalar("bob");
  expected_row["u3.age"] = arrow::MakeScalar((int64_t)31);

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

}  // namespace tundradb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Logger::getInstance().setLevel(LogLevel::DEBUG);
  return RUN_ALL_TESTS();
}