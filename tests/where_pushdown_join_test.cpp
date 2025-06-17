#include <arrow/pretty_print.h>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <random>
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

class WherePushdownJoinTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto user_name_field = arrow::field("name", arrow::utf8());
    auto user_age_field = arrow::field("age", arrow::int64());
    user_schema_ = arrow::schema({user_name_field, user_age_field});

    auto company_name_field = arrow::field("name", arrow::utf8());
    auto company_size_field = arrow::field("size", arrow::int64());
    company_schema_ = arrow::schema({company_name_field, company_size_field});

    auto db_path_ = "benchmark_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path_)
                      .with_shard_capacity(10000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()->create("User", user_schema_).ValueOrDie();
    db_->get_schema_registry()->create("Company", company_schema_).ValueOrDie();
  }

  void create_users(int64_t count) {
    for (int i = 0; i < count; ++i) {
      // Generate user data
      std::string name = "user-" + std::to_string(i);
      int64_t age = 18 + (rng_() % 62);  // Ages 18-80

      // Create Arrow arrays
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;

      ASSERT_OK(name_builder.Append(name));
      ASSERT_OK(age_builder.Append(age));

      std::shared_ptr<arrow::Array> name_array, age_array;
      ASSERT_OK(name_builder.Finish(&name_array));
      ASSERT_OK(age_builder.Finish(&age_array));

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"age", age_array}};

      db_->create_node("User", data).ValueOrDie();
    }
  }

  void create_companies(std::vector<std::string> names) {
    for (const auto& name : names) {
      int64_t size = 10 + (rng_() % 9990);  // Size 10-10000

      arrow::StringBuilder name_builder;
      arrow::Int64Builder size_builder;

      ASSERT_OK(name_builder.Append(name));
      ASSERT_OK(size_builder.Append(size));

      std::shared_ptr<arrow::Array> name_array, size_array;
      ASSERT_OK(name_builder.Finish(&name_array));
      ASSERT_OK(size_builder.Finish(&size_array));

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"size", size_array}};

      db_->create_node("Company", data).ValueOrDie();
    }
  }

  std::shared_ptr<arrow::Schema> user_schema_;
  std::shared_ptr<arrow::Schema> company_schema_;
  std::shared_ptr<Database> db_;
  mutable std::mt19937 rng_;
};

TEST_F(WherePushdownJoinTest, WhereInJoin) {
  auto total_users = 100000;
  auto start_time = std::chrono::high_resolution_clock::now();
  create_users(total_users);
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
  create_companies({"google", "ibm", "piedpiper"});
  Query query = Query::from("u:User").build();
  auto result = db_->query(query);

  std::cout << result.ValueOrDie()->table()->num_rows()
            << " users created within " << duration.count() << " ms"
            << std::endl;

  // half users have friends
  auto half = total_users / 2;
  for (auto i = 0; i < half; ++i) {
    db_->connect(i, "FRIEND", i + half).ValueOrDie();
  }

  query = Query::from("u:User").traverse("u", "FRIEND", "f:User").build();

  result = db_->query(query);

  auto all_friends = result.ValueOrDie()->table();
  auto friends_age =
      get_column_values<int64_t>(all_friends, "f.age").ValueOrDie();

  std::cout << "users with friends: " << all_friends->num_rows() << std::endl;

  int64_t older_than_50 = 0;

  for (const auto& age : friends_age) {
    if (age > 50) {
      older_than_50++;
    }
  }
  std::cout << "older_than_50=" << older_than_50 << " "
            << ((static_cast<double>(older_than_50) / half) * 100.0) << "%"
            << std::endl;

  start_time = std::chrono::high_resolution_clock::now();
  result = db_->query(Query::from("u:User")
                          .traverse("u", "FRIEND", "f:User")
                          .where("f.age", CompareOp::Gt, 50)
                          .build());
  end_time = std::chrono::high_resolution_clock::now();
  const auto duration_unoptimized =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                            start_time);

  const auto unoptimized_size = result.ValueOrDie()->table()->num_rows();
  std::cout << "duration_unoptimized=" << duration_unoptimized.count() << " ms"
            << std::endl;
  std::cout << "unoptimized size=" << unoptimized_size << std::endl;

  start_time = std::chrono::high_resolution_clock::now();
  result = db_->query(Query::from("u:User")
                          .traverse("u", "FRIEND", "f:User")
                          .where("f.age", CompareOp::Gt, 50)
                          .inline_where()
                          .build());
  end_time = std::chrono::high_resolution_clock::now();
  const auto duration_optimized =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                            start_time);
  const auto optimized_size = result.ValueOrDie()->table()->num_rows();
  std::cout << "duration_optimized=" << duration_optimized.count() << " ms"
            << std::endl;
  std::cout << "optimized size=" << optimized_size << std::endl;

  std::cout << "query time reduced by "
            << duration_unoptimized.count() - duration_optimized.count()
            << " ms" << std::endl;
  EXPECT_EQ(optimized_size, unoptimized_size);
}

}  // namespace tundradb
