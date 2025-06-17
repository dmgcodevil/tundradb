#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../include/core.hpp"
#include "../include/logger.hpp"
#include "../include/metadata.hpp"
#include "../include/query.hpp"
#include "../include/utils.hpp"

// Helper macro for Arrow operations
#define ASSERT_OK(expr) ASSERT_TRUE((expr).ok())

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {
class WhereExpressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create User schema
    auto user_name_field = arrow::field("name", arrow::utf8());
    auto user_age_field = arrow::field("age", arrow::int64());
    auto user_city_field = arrow::field("city", arrow::utf8());
    auto user_salary_field = arrow::field("salary", arrow::int64());
    user_schema_ = arrow::schema(
        {user_name_field, user_age_field, user_city_field, user_salary_field});

    // Create Company schema
    auto company_name_field = arrow::field("name", arrow::utf8());
    auto company_size_field = arrow::field("size", arrow::int64());
    auto company_city_field = arrow::field("city", arrow::utf8());
    company_schema_ = arrow::schema(
        {company_name_field, company_size_field, company_city_field});

    // Setup database
    auto db_path = "where_expr_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()->create("User", user_schema_).ValueOrDie();
    db_->get_schema_registry()->create("Company", company_schema_).ValueOrDie();

    create_test_data();
  }

  void create_test_data() {
    // Create users with diverse data for testing
    struct UserData {
      std::string name;
      int64_t age;
      std::string city;
      int64_t salary;
    };

    std::vector<UserData> users = {
        {"Alice", 25, "NYC", 80000},    // 0: Young, NYC, high salary
        {"Bob", 35, "NYC", 120000},     // 1: Mid-age, NYC, high salary
        {"Charlie", 45, "SF", 150000},  // 2: Older, SF, high salary
        {"Diana", 30, "LA", 60000},     // 3: Mid-age, LA, low salary
        {"Eve", 55, "NYC", 200000},     // 4: Senior, NYC, very high salary
        {"Frank", 22, "SF", 50000},     // 5: Young, SF, low salary
        {"Grace", 38, "LA", 90000},     // 6: Mid-age, LA, high salary
        {"Henry", 60, "NYC", 180000},   // 7: Senior, NYC, very high salary
        {"Ivy", 28, "SF", 75000},       // 8: Young, SF, medium salary
        {"Jack", 42, "LA", 110000}      // 9: Mid-age, LA, high salary
    };

    for (const auto& user : users) {
      // Create Arrow arrays
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;
      arrow::StringBuilder city_builder;
      arrow::Int64Builder salary_builder;

      static_cast<void>(name_builder.Append(user.name));
      static_cast<void>(age_builder.Append(user.age));
      static_cast<void>(city_builder.Append(user.city));
      static_cast<void>(salary_builder.Append(user.salary));

      std::shared_ptr<arrow::Array> name_array, age_array, city_array,
          salary_array;
      ASSERT_OK(name_builder.Finish(&name_array));
      ASSERT_OK(age_builder.Finish(&age_array));
      ASSERT_OK(city_builder.Finish(&city_array));
      ASSERT_OK(salary_builder.Finish(&salary_array));

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array},
          {"age", age_array},
          {"city", city_array},
          {"salary", salary_array}};

      auto node = db_->create_node("User", data).ValueOrDie();
      user_nodes_.push_back(node);
    }

    // Create companies
    struct CompanyData {
      std::string name;
      int64_t size;
      std::string city;
    };

    std::vector<CompanyData> companies = {{"TechCorp", 5000, "NYC"},
                                          {"StartupInc", 50, "SF"},
                                          {"BigCorp", 10000, "LA"}};

    for (const auto& company : companies) {
      arrow::StringBuilder name_builder;
      arrow::Int64Builder size_builder;
      arrow::StringBuilder city_builder;

      static_cast<void>(name_builder.Append(company.name));
      static_cast<void>(size_builder.Append(company.size));
      static_cast<void>(city_builder.Append(company.city));

      std::shared_ptr<arrow::Array> name_array, size_array, city_array;
      ASSERT_OK(name_builder.Finish(&name_array));
      ASSERT_OK(size_builder.Finish(&size_array));
      ASSERT_OK(city_builder.Finish(&city_array));

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"size", size_array}, {"city", city_array}};

      auto node = db_->create_node("Company", data).ValueOrDie();
      company_nodes_.push_back(node);
    }

    // Create some friendships for traversal tests
    // Alice (0) -> Bob (1), Charlie (2)
    db_->connect(0, "FRIEND", 1).ValueOrDie();
    db_->connect(0, "FRIEND", 2).ValueOrDie();

    // Bob (1) -> Charlie (2), Diana (3)
    db_->connect(1, "FRIEND", 2).ValueOrDie();
    db_->connect(1, "FRIEND", 3).ValueOrDie();

    // Charlie (2) -> Eve (4)
    db_->connect(2, "FRIEND", 4).ValueOrDie();

    // Create work relationships
    db_->connect(0, "WORKS_AT", 10).ValueOrDie();  // Alice -> TechCorp
    db_->connect(1, "WORKS_AT", 10).ValueOrDie();  // Bob -> TechCorp
    db_->connect(2, "WORKS_AT", 11).ValueOrDie();  // Charlie -> StartupInc
  }

  std::shared_ptr<arrow::Schema> user_schema_;
  std::shared_ptr<arrow::Schema> company_schema_;
  std::shared_ptr<Database> db_;
  std::vector<std::shared_ptr<Node>> user_nodes_;
  std::vector<std::shared_ptr<Node>> company_nodes_;
};

// Test simple WHERE expressions
TEST_F(WhereExpressionTest, SimpleWhereCondition) {
  // Test basic WHERE clause
  Query query = Query::from("u:User").where("u.age", CompareOp::Gt, 40).build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 4);  // Charlie(45), Eve(55), Henry(60), Jack(42)

  // Verify all results have age > 40
  auto ages = get_column_values<int64_t>(table, "u.age").ValueOrDie();
  for (auto age : ages) {
    EXPECT_GT(age, 40);
  }
}

// Test compound WHERE with AND - fluent API
TEST_F(WhereExpressionTest, CompoundWhereAndFluent) {
  // Test: age > 30 AND city = "NYC"
  Query query = Query::from("u:User")
                    .where("u.age", CompareOp::Gt, 30)
                    .and_where("u.city", CompareOp::Eq, "NYC")
                    .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 3);  // Bob(35,NYC), Eve(55,NYC), Henry(60,NYC)

  // Verify conditions
  auto ages = get_column_values<int64_t>(table, "u.age").ValueOrDie();
  auto cities = get_column_values<std::string>(table, "u.city").ValueOrDie();

  for (size_t i = 0; i < ages.size(); ++i) {
    EXPECT_GT(ages[i], 30);
    EXPECT_EQ(cities[i], "NYC");
  }
}

// Test compound WHERE with OR - fluent API
TEST_F(WhereExpressionTest, CompoundWhereOrFluent) {
  Logger::get_instance().set_level(LogLevel::DEBUG);
  // Test: city = "SF" OR salary > 150000
  Query query = Query::from("u:User")
                    .where("u.city", CompareOp::Eq, "SF")
                    .or_where("u.salary", CompareOp::Gt, 150000)
                    .build();

  std::cout << query.clauses().size() << std::endl;
  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(),
            5);  // Charlie(SF,150k), Frank(SF), Ivy(SF), Eve(200k), Henry(180k)

  // Verify conditions (each row should satisfy at least one condition)
  auto cities = get_column_values<std::string>(table, "u.city").ValueOrDie();
  auto salaries = get_column_values<int64_t>(table, "u.salary").ValueOrDie();

  for (size_t i = 0; i < cities.size(); ++i) {
    EXPECT_TRUE(cities[i] == "SF" || salaries[i] > 150000)
        << "Row " << i << " doesn't satisfy OR condition: city=" << cities[i]
        << ", salary=" << salaries[i];
  }
}

// Test complex expressions with precedence using explicit tree API
TEST_F(WhereExpressionTest, ComplexExpressionWithPrecedence) {
  // Test: age > 30 AND (city = "NYC" OR salary > 150000)
  // This should match: Bob(35,NYC), Eve(55,NYC,200k), Henry(60,NYC,180k)

  auto age_condition =
      std::make_shared<ComparisonExpr>("u.age", CompareOp::Gt, Value(30));
  auto city_condition =
      std::make_shared<ComparisonExpr>("u.city", CompareOp::Eq, Value("NYC"));
  auto salary_condition = std::make_shared<ComparisonExpr>(
      "u.salary", CompareOp::Gt, Value(150000));

  // (city = "NYC" OR salary > 150000)
  auto or_expr = LogicalExpr::or_expr(city_condition, salary_condition);

  // age > 30 AND (city = "NYC" OR salary > 150000)
  auto final_expr = LogicalExpr::and_expr(age_condition, or_expr);

  Query query = Query::from("u:User").where_logical_expr(final_expr).build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 3);  // Bob, Eve, Henry

  // Verify precedence is correct
  auto ages = get_column_values<int64_t>(table, "u.age").ValueOrDie();
  auto cities = get_column_values<std::string>(table, "u.city").ValueOrDie();
  auto salaries = get_column_values<int64_t>(table, "u.salary").ValueOrDie();

  for (size_t i = 0; i < ages.size(); ++i) {
    EXPECT_GT(ages[i], 30);  // age > 30 (always true due to AND)
    EXPECT_TRUE(cities[i] == "NYC" ||
                salaries[i] > 150000)  // (city = NYC OR salary > 150k)
        << "Row " << i << " doesn't satisfy OR part: city=" << cities[i]
        << ", salary=" << salaries[i];
  }
}

// Test inline WHERE with simple condition
TEST_F(WhereExpressionTest, InlineWhereSimple) {
  // Test inline optimization with simple WHERE
  Query query = Query::from("u:User")
                    .traverse("u", "FRIEND", "f:User")
                    .where("f.age", CompareOp::Gt, 40)
                    .inline_where()
                    .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  // Should find: Alice->Charlie(45), Bob->Charlie(45), Charlie->Eve(55)
  ASSERT_EQ(table->num_rows(), 3);

  // Verify all friends have age > 40
  auto friend_ages = get_column_values<int64_t>(table, "f.age").ValueOrDie();
  for (auto age : friend_ages) {
    EXPECT_GT(age, 40);
  }
}

// Test inline WHERE with compound condition
TEST_F(WhereExpressionTest, InlineWhereCompound) {
  // Test inline optimization with compound WHERE: f.age > 25 AND f.city = "NYC"
  Query query = Query::from("u:User")
                    .traverse("u", "FRIEND", "f:User")
                    .where("f.age", CompareOp::Gt, 25)
                    .and_where("f.city", CompareOp::Eq, "NYC")
                    .inline_where()
                    .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  // Should find: Alice->Bob(35,NYC), Bob->Nothing (Charlie is SF),
  // Charlie->Eve(55,NYC)
  ASSERT_EQ(table->num_rows(), 2);

  // Verify all friends satisfy both conditions
  auto friend_ages = get_column_values<int64_t>(table, "f.age").ValueOrDie();
  auto friend_cities =
      get_column_values<std::string>(table, "f.city").ValueOrDie();

  for (size_t i = 0; i < friend_ages.size(); ++i) {
    EXPECT_GT(friend_ages[i], 25);
    EXPECT_EQ(friend_cities[i], "NYC");
  }
}

// Test multiple WHERE clauses with different precedence
TEST_F(WhereExpressionTest, MultipleDifferentPrecedence) {
  // Compare left-to-right vs explicit precedence with conditions that actually
  // differ

  // Left-to-right: (age > 40 AND city = "LA") OR salary > 100000
  // This will match: Bob(salary), Charlie(salary), Eve(salary), Henry(salary),
  // Jack(both) = 5 users
  Query query_left = Query::from("u:User")
                         .where("u.age", CompareOp::Gt, 40)
                         .and_where("u.city", CompareOp::Eq, "LA")
                         .or_where("u.salary", CompareOp::Gt, 100000)
                         .build();

  auto result_left = db_->query(query_left);
  ASSERT_OK(result_left);
  auto table_left = result_left.ValueOrDie()->table();

  // Explicit precedence: age > 40 AND (city = "LA" OR salary > 100000)
  // This will match: Charlie(age+salary), Eve(age+salary), Henry(age+salary),
  // Jack(age+both) = 4 users
  auto age_cond =
      std::make_shared<ComparisonExpr>("u.age", CompareOp::Gt, Value(40));
  auto city_cond =
      std::make_shared<ComparisonExpr>("u.city", CompareOp::Eq, Value("LA"));
  auto salary_cond = std::make_shared<ComparisonExpr>("u.salary", CompareOp::Gt,
                                                      Value(100000));
  auto or_part = LogicalExpr::or_expr(city_cond, salary_cond);
  auto final_expr = LogicalExpr::and_expr(age_cond, or_part);

  Query query_explicit =
      Query::from("u:User").where_logical_expr(final_expr).build();

  auto result_explicit = db_->query(query_explicit);
  ASSERT_OK(result_explicit);
  auto table_explicit = result_explicit.ValueOrDie()->table();

  // Results should be different due to precedence:
  // Left-to-right should have 5 rows (includes Bob who doesn't meet age
  // requirement) Explicit should have 4 rows (excludes Bob due to age
  // requirement)
  EXPECT_NE(table_left->num_rows(), table_explicit->num_rows())
      << "Different precedence should yield different results";
  EXPECT_EQ(table_left->num_rows(), 5) << "Left-to-right should match 5 users";
  EXPECT_EQ(table_explicit->num_rows(), 4)
      << "Explicit precedence should match 4 users";

  std::cout << "Left-to-right result: " << table_left->num_rows() << " rows"
            << std::endl;
  std::cout << "Explicit precedence result: " << table_explicit->num_rows()
            << " rows" << std::endl;
}

// Test WhereExpression toString functionality
TEST_F(WhereExpressionTest, ExpressionToString) {
  auto age_cond =
      std::make_shared<ComparisonExpr>("u.age", CompareOp::Gt, Value(30));
  auto city_cond =
      std::make_shared<ComparisonExpr>("u.city", CompareOp::Eq, Value("NYC"));
  auto compound = LogicalExpr::and_expr(age_cond, city_cond);

  std::string expr_str = compound->toString();

  // Should contain both conditions and AND operator
  EXPECT_NE(expr_str.find("u.age > 30"), std::string::npos);
  EXPECT_NE(expr_str.find("u.city = 'NYC'"), std::string::npos);
  EXPECT_NE(expr_str.find("AND"), std::string::npos);

  std::cout << "Expression string: " << expr_str << std::endl;
}

// Test error handling
TEST_F(WhereExpressionTest, ErrorHandling) {
  // Test invalid field name
  Query query = Query::from("u:User")
                    .where("u.nonexistent", CompareOp::Eq, "value")
                    .build();

  auto result = db_->query(query);
  EXPECT_FALSE(result.ok()) << "Should fail with nonexistent field";
}

// Test that expressions with multiple variables are NOT inlined
TEST_F(WhereExpressionTest, MultipleVariablesNotInlined) {
  // Create expression: (u.age > 40 AND (u.city = "NYC" OR c.size > 1000))
  // This should NOT be inlined for variable "u" because it contains variable
  // "c"

  auto age_condition =
      std::make_shared<ComparisonExpr>("u.age", CompareOp::Gt, Value(40));
  auto city_condition =
      std::make_shared<ComparisonExpr>("u.city", CompareOp::Eq, Value("NYC"));
  auto company_condition =
      std::make_shared<ComparisonExpr>("c.size", CompareOp::Gt, Value(1000));

  // Build: (u.city = "NYC" OR c.size > 1000)
  auto mixed_or_expr = LogicalExpr::or_expr(city_condition, company_condition);

  // Build: u.age > 40 AND (u.city = "NYC" OR c.size > 1000)
  auto final_expr = LogicalExpr::and_expr(age_condition, mixed_or_expr);

  // Test get_conditions_for_variable for "u" - should return empty because
  // expression contains "c"
  auto conditions_for_u = final_expr->get_conditions_for_variable("u");
  EXPECT_TRUE(conditions_for_u.empty())
      << "Expression with multiple variables should not return conditions for "
         "single variable";

  // Test get_conditions_for_variable for "c" - should return empty because
  // expression contains "u"
  auto conditions_for_c = final_expr->get_conditions_for_variable("c");
  EXPECT_TRUE(conditions_for_c.empty())
      << "Expression with multiple variables should not return conditions for "
         "single variable";

  // Test get_all_variables - should return both "u" and "c"
  auto all_variables = final_expr->get_all_variables();
  EXPECT_EQ(all_variables.size(), 2);
  EXPECT_TRUE(all_variables.count("u") > 0);
  EXPECT_TRUE(all_variables.count("c") > 0);

  // Test that a single-variable expression still works
  auto single_var_expr = LogicalExpr::and_expr(age_condition, city_condition);
  auto single_var_conditions =
      single_var_expr->get_conditions_for_variable("u");
  EXPECT_EQ(single_var_conditions.size(), 2)
      << "Single-variable expression should return all conditions";

  // Verify the single-variable expression only has "u"
  auto single_var_variables = single_var_expr->get_all_variables();
  EXPECT_EQ(single_var_variables.size(), 1);
  EXPECT_TRUE(single_var_variables.count("u") > 0);
}

// Test performance comparison test
TEST_F(WhereExpressionTest, PerformanceComparison) {
  // Create more test data for performance testing
  for (int i = 0; i < 1000; ++i) {
    arrow::StringBuilder name_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder city_builder;
    arrow::Int64Builder salary_builder;

    static_cast<void>(name_builder.Append("User" + std::to_string(i)));
    static_cast<void>(age_builder.Append(20 + (i % 50)));
    static_cast<void>(city_builder.Append(i % 2 == 0 ? "NYC" : "SF"));
    static_cast<void>(salary_builder.Append(50000 + (i * 100)));

    std::shared_ptr<arrow::Array> name_array, age_array, city_array,
        salary_array;
    ASSERT_OK(name_builder.Finish(&name_array));
    ASSERT_OK(age_builder.Finish(&age_array));
    ASSERT_OK(city_builder.Finish(&city_array));
    ASSERT_OK(salary_builder.Finish(&salary_array));

    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
        {"name", name_array},
        {"age", age_array},
        {"city", city_array},
        {"salary", salary_array}};

    db_->create_node("User", data).ValueOrDie();
  }

  // Test simple WHERE performance
  auto start = std::chrono::high_resolution_clock::now();

  Query query = Query::from("u:User")
                    .where("u.age", CompareOp::Gt, 40)
                    .and_where("u.city", CompareOp::Eq, "NYC")
                    .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::cout << "Compound WHERE query took: " << duration.count() << " ms"
            << std::endl;
  std::cout << "Result rows: " << result.ValueOrDie()->table()->num_rows()
            << std::endl;
}

// Test that conditions in OR expressions with multiple variables are not
// inlined
TEST_F(WhereExpressionTest, OrWithMultipleVariablesNotInlined) {
  Logger::get_instance().set_level(LogLevel::DEBUG);
  // Create test data with specific values to test the condition
  arrow::StringBuilder name_builder;
  arrow::Int64Builder age_builder;
  arrow::StringBuilder city_builder;
  arrow::Int64Builder salary_builder;

  // Create a user that will match the first part but not the second part of the
  // OR
  static_cast<void>(name_builder.Append("TestUser"));
  static_cast<void>(age_builder.Append(30));  // This will match a.age == 30
  static_cast<void>(
      city_builder.Append("LA"));  // This will NOT match a.city == "NYC"
  static_cast<void>(salary_builder.Append(50000));

  std::shared_ptr<arrow::Array> name_array, age_array, city_array, salary_array;
  ASSERT_OK(name_builder.Finish(&name_array));
  ASSERT_OK(age_builder.Finish(&age_array));
  ASSERT_OK(city_builder.Finish(&city_array));
  ASSERT_OK(salary_builder.Finish(&salary_array));

  std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
      {"name", name_array},
      {"age", age_array},
      {"city", city_array},
      {"salary", salary_array}};

  auto user_node = db_->create_node("User", data).ValueOrDie();

  // Create a company that will match the second part of the OR
  arrow::StringBuilder company_name_builder;
  arrow::Int64Builder company_size_builder;
  arrow::StringBuilder company_city_builder;

  static_cast<void>(company_name_builder.Append("TestCompany"));
  static_cast<void>(
      company_size_builder.Append(2000));  // This will match c.size > 1000
  static_cast<void>(company_city_builder.Append("NYC"));

  std::shared_ptr<arrow::Array> company_name_array, company_size_array,
      company_city_array;
  ASSERT_OK(company_name_builder.Finish(&company_name_array));
  ASSERT_OK(company_size_builder.Finish(&company_size_array));
  ASSERT_OK(company_city_builder.Finish(&company_city_array));

  std::unordered_map<std::string, std::shared_ptr<arrow::Array>> company_data =
      {{"name", company_name_array},
       {"size", company_size_array},
       {"city", company_city_array}};

  auto company_node = db_->create_node("Company", company_data).ValueOrDie();

  // Connect user to company
  db_->connect(user_node->id, "WORKS_AT", company_node->id).ValueOrDie();

  // Create the complex where expression: a.age == 30 AND (a.city == "NYC" OR
  // c.size > 1000)
  auto age_condition =
      std::make_shared<ComparisonExpr>("a.age", CompareOp::Eq, Value(30));
  auto city_condition =
      std::make_shared<ComparisonExpr>("a.city", CompareOp::Eq, Value("NYC"));
  auto size_condition =
      std::make_shared<ComparisonExpr>("c.size", CompareOp::Gt, Value(1000));

  // Build: (a.city == "NYC" OR c.size > 1000)
  auto or_expr = LogicalExpr::or_expr(city_condition, size_condition);

  // Build: a.age == 30 AND (a.city == "NYC" OR c.size > 1000)
  auto final_expr = LogicalExpr::and_expr(age_condition, or_expr);

  // Create query that should match our test data
  Query query = Query::from("a:User")
                    .traverse("a", "WORKS_AT", "c:Company")
                    .select({"a.age", "a.city",
                             "c.size"})  // Explicitly select the fields we need
                    .where_logical_expr(final_expr)
                    .inline_where()
                    .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();

  // The query should match because:
  // 1. a.age == 30 is true
  // 2. a.city == "NYC" is false BUT c.size > 1000 is true
  // So the OR condition is satisfied
  ASSERT_EQ(table->num_rows(), 1) << "Query should match one row";

  // Verify the matched row has the correct values
  auto ages = get_column_values<int64_t>(table, "a.age").ValueOrDie();
  auto cities = get_column_values<std::string>(table, "a.city").ValueOrDie();
  auto sizes = get_column_values<int64_t>(table, "c.size").ValueOrDie();

  EXPECT_EQ(ages[0], 30);
  EXPECT_EQ(cities[0], "LA");
  EXPECT_EQ(sizes[0], 2000);
}

// Test different combinations of traversals and where clauses
TEST_F(WhereExpressionTest, TraversalWhereCombinations) {
  Logger::get_instance().set_level(LogLevel::DEBUG);

  // Test Case 1: Single variable where clause (should be inlined)
  {
    Query query = Query::from("u:User")
                      .where("u.age", CompareOp::Gt, 35)
                      .traverse("u", "WORKS_AT", "c:Company")

                      .select({"u.age", "u.city", "c.size"})
                      .inline_where()
                      .build();

    auto result = db_->query(query);
    ASSERT_OK(result);

    auto table = result.ValueOrDie()->table();
    ASSERT_EQ(table->num_rows(), 1);  // Charlie (45) have companies

    const auto& stats = result.ValueOrDie()->execution_stats();
    EXPECT_EQ(stats.num_where_clauses_inlined, 1);
    EXPECT_EQ(stats.num_where_clauses_post_processed, 0);
  }
}

TEST_F(WhereExpressionTest, TraversalWhereCombinations2) {
  Query query = Query::from("u:User")
                    .traverse("u", "WORKS_AT", "c:Company")
                    .where("u.age", CompareOp::Gte, 35)
                    .and_where("c.size", CompareOp::Gt, 1000)
                    .select({"u.age", "u.city", "c.size"})
                    .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(),
            1);  // Only Bob (35) -> TechCorp (5000) matches

  const auto& stats = result.ValueOrDie()->execution_stats();
  EXPECT_EQ(stats.num_where_clauses_inlined, 0);
  EXPECT_EQ(stats.num_where_clauses_post_processed, 1);
}

TEST_F(WhereExpressionTest, TraversalWhereCombinations3) {
  Query query =
      Query::from("u:User")
          .where("u.age", CompareOp::Gte, 35)  // Should be inlined
          .traverse("u", "WORKS_AT", "c:Company")
          .where("c.size", CompareOp::Gt, 1000)  // Should be inlined
          .traverse("c", "EMPLOYS", "u2:User")
          .where("u2.city", CompareOp::Eq, "NYC")  // Should be inlined
          .and_where("u.city", CompareOp::Eq,
                     "LA")  // Should be post-processed (references u)
          .inline_where()
          .build();

  auto result = db_->query(query);
  ASSERT_OK(result);

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 0);

  const auto& stats = result.ValueOrDie()->execution_stats();
  EXPECT_EQ(stats.num_where_clauses_inlined, 2);
  EXPECT_EQ(stats.num_where_clauses_post_processed, 1);
}

}  // namespace tundradb