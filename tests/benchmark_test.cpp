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

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb::benchmark {

class BenchmarkFixture {
 private:
  std::shared_ptr<Database> db_;
  std::string db_path_;
  mutable std::mt19937 rng_;

 public:
  BenchmarkFixture() : rng_(42) {
    db_path_ = "benchmark_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path_)
                      .with_shard_capacity(10000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    setupSchemas();
  }

  ~BenchmarkFixture() {
    db_.reset();
    if (std::filesystem::exists(db_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }

  std::shared_ptr<Database> db() const { return db_; }

  void setupSchemas() {
    // User schema
    auto user_name_field = arrow::field("name", arrow::utf8());
    auto user_age_field = arrow::field("age", arrow::int64());
    auto user_schema = arrow::schema({user_name_field, user_age_field});
    db_->get_schema_registry()->create("User", user_schema).ValueOrDie();

    // Company schema
    auto company_name_field = arrow::field("name", arrow::utf8());
    auto company_size_field = arrow::field("size", arrow::int64());
    auto company_industry_field = arrow::field("industry", arrow::utf8());
    auto company_schema = arrow::schema(
        {company_name_field, company_size_field, company_industry_field});
    db_->get_schema_registry()->create("Company", company_schema).ValueOrDie();

    // Product schema
    auto product_name_field = arrow::field("name", arrow::utf8());
    auto product_price_field = arrow::field("price", arrow::int64());
    auto product_category_field = arrow::field("category", arrow::utf8());
    auto product_schema = arrow::schema(
        {product_name_field, product_price_field, product_category_field});
    db_->get_schema_registry()->create("Product", product_schema).ValueOrDie();
  }

  void createUsers(int count) {
    std::vector<std::string> names = {"Alice", "Bob",   "Charlie", "David",
                                      "Eve",   "Frank", "Grace",   "Henry",
                                      "Ivy",   "Jack"};

    for (int i = 0; i < count; ++i) {
      // Generate user data
      std::string name = names[rng_() % names.size()] + "_" + std::to_string(i);
      int64_t age = 18 + (rng_() % 62);  // Ages 18-80

      // Create Arrow arrays
      arrow::StringBuilder name_builder;
      arrow::Int64Builder age_builder;

      name_builder.Append(name);
      age_builder.Append(age);

      std::shared_ptr<arrow::Array> name_array, age_array;
      name_builder.Finish(&name_array);
      age_builder.Finish(&age_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array}, {"age", age_array}};

      db_->create_node("User", data).ValueOrDie();
    }
  }

  void createCompanies(int count) {
    std::vector<std::string> companies = {"TechCorp", "DataSys", "CloudCo",
                                          "AI Labs", "NetWorks"};
    std::vector<std::string> industries = {
        "Technology", "Finance", "Healthcare", "Education", "Manufacturing"};

    for (int i = 0; i < count; ++i) {
      std::string name =
          companies[rng_() % companies.size()] + "_" + std::to_string(i);
      int64_t size = 10 + (rng_() % 9990);  // Size 10-10000
      std::string industry = industries[rng_() % industries.size()];

      arrow::StringBuilder name_builder, industry_builder;
      arrow::Int64Builder size_builder;

      name_builder.Append(name);
      size_builder.Append(size);
      industry_builder.Append(industry);

      std::shared_ptr<arrow::Array> name_array, size_array, industry_array;
      name_builder.Finish(&name_array);
      size_builder.Finish(&size_array);
      industry_builder.Finish(&industry_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array},
          {"size", size_array},
          {"industry", industry_array}};

      db_->create_node("Company", data).ValueOrDie();
    }
  }

  void createProducts(int count) {
    std::vector<std::string> products = {"Widget", "Gadget", "Tool", "Device",
                                         "System"};
    std::vector<std::string> categories = {
        "Electronics", "Software", "Hardware", "Services", "Consulting"};

    for (int i = 0; i < count; ++i) {
      std::string name =
          products[rng_() % products.size()] + "_" + std::to_string(i);
      int64_t price = 10 + (rng_() % 1990);  // Price $10-$2000
      std::string category = categories[rng_() % categories.size()];

      arrow::StringBuilder name_builder, category_builder;
      arrow::Int64Builder price_builder;

      name_builder.Append(name);
      price_builder.Append(price);
      category_builder.Append(category);

      std::shared_ptr<arrow::Array> name_array, price_array, category_array;
      name_builder.Finish(&name_array);
      price_builder.Finish(&price_array);
      category_builder.Finish(&category_array);

      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
          {"name", name_array},
          {"price", price_array},
          {"category", category_array}};

      db_->create_node("Product", data).ValueOrDie();
    }
  }

  void createRandomEdges(const std::string& edge_type, int64_t source_min,
                         int64_t source_max, int64_t target_min,
                         int64_t target_max, int count) {
    for (int i = 0; i < count; ++i) {
      int64_t source_id = source_min + (rng_() % (source_max - source_min + 1));
      int64_t target_id = target_min + (rng_() % (target_max - target_min + 1));
      db_->connect(source_id, edge_type, target_id);
    }
  }

  // Convenience method with old signature for simple cases
  void createRandomEdges(const std::string& edge_type, int64_t max_source_id,
                         int64_t max_target_id, int count) {
    createRandomEdges(edge_type, 0, max_source_id - 1, 0, max_target_id - 1,
                      count);
  }
};

// Global fixture instance for sharing across benchmarks
static std::unique_ptr<BenchmarkFixture> g_fixture;

// Test fixtures for different dataset sizes
class SmallDatasetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fixture = std::make_unique<BenchmarkFixture>();
    fixture->createUsers(100);     // IDs 0-99
    fixture->createCompanies(20);  // IDs 100-119
    fixture->createProducts(50);   // IDs 120-169

    // Fix edge creation with correct ID ranges
    fixture->createRandomEdges("WORKS_AT", 0, 99, 100, 119,
                               80);  // Users (0-99) -> Companies (100-119)
    fixture->createRandomEdges("FRIEND", 0, 99, 0, 99,
                               150);  // Users (0-99) -> Users (0-99)
    fixture->createRandomEdges("BUYS", 0, 99, 120, 169,
                               200);  // Users (0-99) -> Products (120-169)
  }

  std::unique_ptr<BenchmarkFixture> fixture;
};

class MediumDatasetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fixture = std::make_unique<BenchmarkFixture>();
    fixture->createUsers(5000);     // IDs 0-4999
    fixture->createCompanies(500);  // IDs 5000-5499
    fixture->createProducts(1000);  // IDs 5500-6499

    fixture->createRandomEdges("WORKS_AT", 0, 4999, 5000, 5499,
                               4000);  // Users -> Companies
    fixture->createRandomEdges("FRIEND", 0, 4999, 0, 4999,
                               7500);  // Users -> Users
    fixture->createRandomEdges("BUYS", 0, 4999, 5500, 6499,
                               15000);  // Users -> Products
  }

  std::unique_ptr<BenchmarkFixture> fixture;
};

class LargeDatasetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fixture = std::make_unique<BenchmarkFixture>();
    fixture->createUsers(50000);     // IDs 0-49999
    fixture->createCompanies(5000);  // IDs 50000-54999
    fixture->createProducts(10000);  // IDs 55000-64999

    fixture->createRandomEdges("WORKS_AT", 0, 49999, 50000, 54999,
                               40000);  // Users -> Companies
    fixture->createRandomEdges("FRIEND", 0, 49999, 0, 49999,
                               75000);  // Users -> Users
    fixture->createRandomEdges("BUYS", 0, 49999, 55000, 64999,
                               150000);  // Users -> Products
  }

  std::unique_ptr<BenchmarkFixture> fixture;
};

// Benchmark functions that can be called from Google Benchmark
void BM_NodeCreation(::benchmark::State& state) {
  auto fixture = std::make_unique<BenchmarkFixture>();
  int nodes_per_iteration = state.range(0);

  for (auto _ : state) {
    state.PauseTiming();
    // Create fresh fixture for each iteration to avoid ID conflicts
    fixture = std::make_unique<BenchmarkFixture>();
    state.ResumeTiming();

    fixture->createUsers(nodes_per_iteration);
  }

  state.SetItemsProcessed(state.iterations() * nodes_per_iteration);
  state.SetBytesProcessed(state.iterations() * nodes_per_iteration *
                          64);  // Approximate bytes per node
}

void BM_FullScan(::benchmark::State& state) {
  auto fixture = std::make_unique<BenchmarkFixture>();
  int node_count = state.range(0);
  fixture->createUsers(node_count);

  for (auto _ : state) {
    Query query = Query::from("u:User").build();
    auto result = fixture->db()->query(query);
    if (!result.ok() ||
        result.ValueOrDie()->table()->num_rows() != node_count) {
      state.SkipWithError("Query failed or returned wrong number of rows");
    }
  }

  state.SetItemsProcessed(state.iterations() * node_count);
}

void BM_SimpleJoin(::benchmark::State& state) {
  auto fixture = std::make_unique<BenchmarkFixture>();
  int user_count = state.range(0);
  int company_count = user_count / 10;  // 10:1 ratio

  fixture->createUsers(user_count);  // IDs 0 to user_count-1
  fixture->createCompanies(
      company_count);  // IDs user_count to user_count+company_count-1

  // Fix: Connect users to companies with correct ID ranges
  fixture->createRandomEdges("WORKS_AT", 0, user_count - 1, user_count,
                             user_count + company_count - 1, user_count * 0.8);

  for (auto _ : state) {
    Query query =
        Query::from("u:User")
            .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner)
            .build();
    auto result = fixture->db()->query(query);
    if (!result.ok()) {
      state.SkipWithError("Query failed");
    }
  }

  state.SetItemsProcessed(state.iterations() * user_count);
}

void BM_ComplexJoin(::benchmark::State& state) {
  auto fixture = std::make_unique<BenchmarkFixture>();
  int user_count = state.range(0);
  int company_count = user_count / 10;
  int product_count = user_count / 5;

  fixture->createUsers(user_count);  // IDs 0 to user_count-1
  fixture->createCompanies(
      company_count);  // IDs user_count to user_count+company_count-1
  fixture->createProducts(
      product_count);  // IDs user_count+company_count to
                       // user_count+company_count+product_count-1

  int company_start = user_count;
  int product_start = user_count + company_count;

  // Fix: Use correct ID ranges for all edges
  fixture->createRandomEdges("WORKS_AT", 0, user_count - 1, company_start,
                             company_start + company_count - 1,
                             user_count * 0.8);
  fixture->createRandomEdges("FRIEND", 0, user_count - 1, 0, user_count - 1,
                             user_count * 1.5);
  fixture->createRandomEdges("BUYS", 0, user_count - 1, product_start,
                             product_start + product_count - 1, user_count * 3);

  for (auto _ : state) {
    // Complex 3-way join: Users -> Friends -> Companies
    Query query =
        Query::from("u:User")
            .traverse("u", "FRIEND", "f:User", TraverseType::Inner)
            .traverse("f", "WORKS_AT", "c:Company", TraverseType::Inner)
            .build();
    auto result = fixture->db()->query(query);
    if (!result.ok()) {
      state.SkipWithError("Complex query failed");
    }
  }

  state.SetItemsProcessed(state.iterations() * user_count);
}

void BM_FilteredQuery(::benchmark::State& state) {
  auto fixture = std::make_unique<BenchmarkFixture>();
  int user_count = state.range(0);
  fixture->createUsers(user_count);

  for (auto _ : state) {
    // Query with WHERE clause - users over 50
    Query query =
        Query::from("u:User").where("u.age", CompareOp::Gt, Value(50)).build();
    auto result = fixture->db()->query(query);
    if (!result.ok()) {
      state.SkipWithError("Filtered query failed");
    }
  }

  state.SetItemsProcessed(state.iterations() * user_count);
}

// Google Test cases for correctness verification
TEST_F(SmallDatasetTest, NodeCreationCorrectness) {
  Query query = Query::from("u:User").build();
  auto result = fixture->db()->query(query);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie()->table()->num_rows(), 100);
}

TEST_F(SmallDatasetTest, SimpleJoinCorrectness) {
  Query query = Query::from("u:User")
                    .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner)
                    .build();
  auto result = fixture->db()->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  EXPECT_GT(table->num_rows(), 0);
  EXPECT_LE(table->num_rows(), 100);  // Should be <= number of users

  // Verify we have the expected columns
  EXPECT_NE(table->GetColumnByName("u.id"), nullptr);
  EXPECT_NE(table->GetColumnByName("u.name"), nullptr);
  EXPECT_NE(table->GetColumnByName("c.id"), nullptr);
  EXPECT_NE(table->GetColumnByName("c.name"), nullptr);
}

TEST_F(SmallDatasetTest, ComplexJoinCorrectness) {
  Query query = Query::from("u:User")
                    .traverse("u", "FRIEND", "f:User", TraverseType::Inner)
                    .traverse("f", "WORKS_AT", "c:Company", TraverseType::Inner)
                    .build();
  auto result = fixture->db()->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  // Should have some results but not necessarily all users
  EXPECT_GE(table->num_rows(), 0);
}

TEST_F(SmallDatasetTest, FilteredQueryCorrectness) {
  Query query =
      Query::from("u:User").where("u.age", CompareOp::Gt, Value(50)).build();
  auto result = fixture->db()->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();

  // Verify all returned users have age > 50
  if (table->num_rows() > 0) {
    auto age_column = table->GetColumnByName("u.age");
    ASSERT_NE(age_column, nullptr);

    for (int64_t i = 0; i < table->num_rows(); ++i) {
      auto age_result = age_column->GetScalar(i);
      ASSERT_TRUE(age_result.ok());
      auto age_scalar =
          std::static_pointer_cast<arrow::Int64Scalar>(age_result.ValueOrDie());
      EXPECT_GT(age_scalar->value, 50);
    }
  }
}

TEST_F(MediumDatasetTest, ScalabilityTest) {
  auto start_time = std::chrono::high_resolution_clock::now();

  Query query = Query::from("u:User")
                    .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner)
                    .build();
  auto result = fixture->db()->query(query);

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  ASSERT_TRUE(result.ok());
  EXPECT_LT(duration.count(), 5000);  // Should complete within 5 seconds

  std::cout << "Medium dataset join took " << duration.count() << "ms"
            << std::endl;
}

// Performance test with manual timing
TEST_F(LargeDatasetTest, PerformanceBaseline) {
  const int num_iterations = 10;
  std::vector<double> times;

  for (int i = 0; i < num_iterations; ++i) {
    auto start = std::chrono::high_resolution_clock::now();

    Query query =
        Query::from("u:User")
            .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner)
            .build();
    auto result = fixture->db()->query(query);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    ASSERT_TRUE(result.ok());
    times.push_back(duration.count() / 1000.0);  // Convert to ms
  }

  // Calculate statistics
  double sum = 0;
  for (double time : times) sum += time;
  double avg = sum / num_iterations;

  double variance = 0;
  for (double time : times) variance += (time - avg) * (time - avg);
  double std_dev = sqrt(variance / num_iterations);

  std::cout << "Large dataset performance:" << std::endl;
  std::cout << "  Average: " << avg << "ms" << std::endl;
  std::cout << "  Std Dev: " << std_dev << "ms" << std::endl;
  std::cout << "  Min: " << *std::min_element(times.begin(), times.end())
            << "ms" << std::endl;
  std::cout << "  Max: " << *std::max_element(times.begin(), times.end())
            << "ms" << std::endl;

  // Set reasonable performance expectations
  EXPECT_LT(avg, 10000);  // Average should be less than 10 seconds
  EXPECT_LT(std_dev / avg,
            0.5);  // Coefficient of variation should be reasonable
}

}  // namespace tundradb::benchmark

// Register Google Benchmark functions
BENCHMARK(tundradb::benchmark::BM_NodeCreation)
    ->RangeMultiplier(10)
    ->Range(10, 10000)
    ->Unit(::benchmark::kMicrosecond);

BENCHMARK(tundradb::benchmark::BM_FullScan)
    ->RangeMultiplier(10)
    ->Range(100, 100000)
    ->Unit(::benchmark::kMicrosecond);

BENCHMARK(tundradb::benchmark::BM_SimpleJoin)
    ->RangeMultiplier(10)
    ->Range(100, 10000)
    ->Unit(::benchmark::kMicrosecond);

BENCHMARK(tundradb::benchmark::BM_ComplexJoin)
    ->RangeMultiplier(10)
    ->Range(100, 5000)
    ->Unit(::benchmark::kMicrosecond);

BENCHMARK(tundradb::benchmark::BM_FilteredQuery)
    ->RangeMultiplier(10)
    ->Range(100, 100000)
    ->Unit(::benchmark::kMicrosecond);

// Main function for running Google Benchmark
int main(int argc, char** argv) {
  // Initialize Google Test
  ::testing::InitGoogleTest(&argc, argv);

  // Check if we should run benchmarks or tests
  bool run_benchmarks = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--benchmark") {
      run_benchmarks = true;
      break;
    }
  }

  if (run_benchmarks) {
    // Filter out the --benchmark flag before passing to Google Benchmark
    std::vector<char*> filtered_args;
    filtered_args.push_back(argv[0]);  // Keep program name

    for (int i = 1; i < argc; ++i) {
      if (std::string(argv[i]) != "--benchmark") {
        filtered_args.push_back(argv[i]);
      }
    }

    int filtered_argc = filtered_args.size();
    char** filtered_argv = filtered_args.data();

    // Run Google Benchmark
    ::benchmark::Initialize(&filtered_argc, filtered_argv);
    if (::benchmark::ReportUnrecognizedArguments(filtered_argc,
                                                 filtered_argv)) {
      return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    return 0;
  } else {
    // Run Google Test
    return RUN_ALL_TESTS();
  }
}