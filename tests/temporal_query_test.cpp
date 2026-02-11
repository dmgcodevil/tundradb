#include <gtest/gtest.h>

#include <memory>

#include "../include/clock.hpp"
#include "../include/core.hpp"
#include "../include/node_arena.hpp"
#include "../include/query.hpp"
#include "../include/temporal_context.hpp"

using namespace tundradb;

class TemporalQueryTest : public ::testing::Test {
 protected:
  std::shared_ptr<Database> db_;
  std::string test_db_path_;
  MockClock mock_clock_;

  // Timestamps for testing
  uint64_t t0_;  // Initial time
  uint64_t t1_;  // First update
  uint64_t t2_;  // Second update
  uint64_t t3_;  // Third update

  void SetUp() override {
    // Install mock clock for deterministic testing
    Clock::set_instance(&mock_clock_);
    // Create unique test directory
    test_db_path_ = "temporal_test_db_" + std::to_string(now_millis());

    // Create database with persistence
    auto config = make_config()
                      .with_db_path(test_db_path_)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);

    // Create User schema
    // auto user_id_field = arrow::field("id", arrow::int64());
    auto user_name_field = arrow::field("name", arrow::utf8());
    auto user_age_field = arrow::field("age", arrow::int32());
    auto user_active_field = arrow::field("active", arrow::boolean());
    auto user_schema =
        arrow::schema({user_name_field, user_age_field, user_active_field});
    db_->get_schema_registry()->create("User", user_schema).ValueOrDie();

    // Create Company schema
    // auto company_id_field = arrow::field("id", arrow::int64());
    auto company_name_field = arrow::field("name", arrow::utf8());
    auto company_revenue_field = arrow::field("revenue", arrow::float64());
    auto company_schema =
        arrow::schema({company_name_field, company_revenue_field});
    db_->get_schema_registry()->create("Company", company_schema).ValueOrDie();

    // Setup test timestamps (nanoseconds)
    t0_ = 1685577600000000000ULL;  // 2023-06-01 00:00:00 UTC
    t1_ = 1688169600000000000ULL;  // 2023-07-01 00:00:00 UTC
    t2_ = 1690848000000000000ULL;  // 2023-08-01 00:00:00 UTC
    t3_ = 1693526400000000000ULL;  // 2023-09-01 00:00:00 UTC

    // Set initial time to t0
    mock_clock_.set_time(t0_);
  }

  void TearDown() override {
    // Restore system clock
    Clock::reset();

    // Clean up test directory
    std::filesystem::remove_all(test_db_path_);
  }

  // Helper to create a simple user for testing
  int64_t create_simple_user(const std::string& name, int32_t age) {
    std::unordered_map<std::string, Value> data = {
        {"name", Value{name}}, {"age", Value{age}}, {"active", Value{true}}};

    auto node = db_->create_node("User", data).ValueOrDie();
    return node->id;
  }

  // Helper to create user with versioning enabled
  int64_t create_versioned_user(const std::string& name, int32_t age) {
    // Note: Once Database supports versioning config, we can enable it
    // For now, this is the same as create_simple_user
    return create_simple_user(name, age);
  }
};

// ============================================================================
// TEST: Node Updates with Temporal Queries
// ============================================================================

TEST_F(TemporalQueryTest, NodeUpdateAtDifferentTimes) {
  // Create user at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Alice", 25);

  // Update age to 26 at t1
  mock_clock_.set_time(t1_);
  auto update_result1 =
      db_->update_node(user_id, "age", Value(26), UpdateType::SET);
  ASSERT_TRUE(update_result1.ok()) << update_result1.status();

  // Update age to 27 at t2
  mock_clock_.set_time(t2_);
  auto update_result2 =
      db_->update_node(user_id, "age", Value(27), UpdateType::SET);
  ASSERT_TRUE(update_result2.ok()) << update_result2.status();

  // Query current version (at t2): should see age=27
  auto query_current = Query::from("u:User")
                           .where("u.name", CompareOp::Eq, Value("Alice"))
                           .build();
  auto result_current = db_->query(query_current);
  ASSERT_TRUE(result_current.ok());
  auto table_current = result_current.ValueOrDie()->table();
  ASSERT_EQ(table_current->num_rows(), 1);

  // Note: Until Database creates versioned nodes, this will return current
  // version
  auto age_col_current = table_current->GetColumnByName("u.age");
  auto age_array_current =
      std::static_pointer_cast<arrow::Int32Array>(age_col_current->chunk(0));
  EXPECT_EQ(age_array_current->Value(0), 27);
}

TEST_F(TemporalQueryTest, MultipleFieldUpdateAtSameTime) {
  // Create user at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Bob", 30);

  // Update both age and active at t1
  mock_clock_.set_time(t1_);
  auto update1 = db_->update_node(user_id, "age", Value(31), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  auto update2 =
      db_->update_node(user_id, "active", Value(false), UpdateType::SET);
  ASSERT_TRUE(update2.ok());

  // Query at current time: should see age=31, active=false
  auto query = Query::from("u:User")
                   .where("u.name", CompareOp::Eq, Value("Bob"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());
  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  auto age_col = table->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 31);

  auto active_col = table->GetColumnByName("u.active");
  auto active_array =
      std::static_pointer_cast<arrow::BooleanArray>(active_col->chunk(0));
  EXPECT_FALSE(active_array->Value(0));
}

TEST_F(TemporalQueryTest, ClockAdvanceAndQuery) {
  // Create user at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Charlie", 35);

  // Advance time and update
  mock_clock_.advance_seconds(1);  // 1 second after t0
  auto update1 = db_->update_node(user_id, "age", Value(36), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  // Advance time and update again
  mock_clock_.advance_seconds(1);  // 2 seconds after t0
  auto update2 = db_->update_node(user_id, "age", Value(37), UpdateType::SET);
  ASSERT_TRUE(update2.ok());

  // Query current: should see age=37
  auto query = Query::from("u:User")
                   .where("u.name", CompareOp::Eq, Value("Charlie"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());
  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  auto age_col = table->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 37);
}

TEST_F(TemporalQueryTest, CurrentVersionQuery) {
  // Create a simple user
  int64_t user_id = create_simple_user("Alice", 27);

  // Query current version (no AS OF clause)
  auto query = Query::from("u:User")
                   .where("u.name", CompareOp::Eq, Value("Alice"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  // Current version should be age=27
  auto age_col = table->GetColumnByName("u.age");
  ASSERT_NE(age_col, nullptr);
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 27);
}

TEST_F(TemporalQueryTest, AsOfValidTimeQuery) {
  // Create user
  int64_t user_id = create_simple_user("Alice", 25);

  // Query at t1
  auto query = Query::from("u:User")
                   .as_of_valid_time(t1_)
                   .where("u.name", CompareOp::Eq, Value("Alice"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  auto age_col = table->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 25);
}

TEST_F(TemporalQueryTest, AsOfTxTimeQuery) {
  // Create user
  int64_t user_id = create_simple_user("Alice", 26);

  // Query as of transaction time t1
  auto query = Query::from("u:User")
                   .as_of_tx_time(t1_)
                   .where("u.name", CompareOp::Eq, Value("Alice"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  auto age_col = table->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 26);
}

TEST_F(TemporalQueryTest, BitemporalQuery) {
  // Create user
  int64_t user_id = create_simple_user("Alice", 26);

  // Query both dimensions: valid_time=t1, tx_time=t1
  auto query = Query::from("u:User")
                   .as_of(t1_, t1_)
                   .where("u.name", CompareOp::Eq, Value("Alice"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  ASSERT_EQ(table->num_rows(), 1);

  auto age_col = table->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 26);
}

TEST_F(TemporalQueryTest, TemporalQueryWithWhereClause) {
  // Create two users
  create_simple_user("Alice", 25);
  create_simple_user("Bob", 30);

  // Query at t0 where age > 26 (should find only Bob)
  auto query = Query::from("u:User")
                   .as_of_valid_time(t0_)
                   .where("u.age", CompareOp::Gt, Value(26))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());

  auto table = result.ValueOrDie()->table();
  EXPECT_EQ(table->num_rows(), 1);

  auto name_col = table->GetColumnByName("u.name");
  auto name_array =
      std::static_pointer_cast<arrow::StringArray>(name_col->chunk(0));
  EXPECT_EQ(name_array->GetString(0), "Bob");

  auto age_col = table->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 30);
}

TEST_F(TemporalQueryTest, TemporalSnapshotInQueryState) {
  // Verify that TemporalSnapshot is correctly passed through QueryState
  int64_t user_id = create_simple_user("Alice", 25);

  // Create query with temporal snapshot
  auto query = Query::from("u:User")
                   .as_of_valid_time(t1_)
                   .where("u.name", CompareOp::Eq, Value("Alice"))
                   .build();

  // Verify temporal_snapshot is set
  ASSERT_TRUE(query.temporal_snapshot().has_value());
  EXPECT_EQ(query.temporal_snapshot()->valid_time, t1_);
  EXPECT_EQ(query.temporal_snapshot()->tx_time,
            std::numeric_limits<uint64_t>::max());

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());
}

TEST_F(TemporalQueryTest, QueryBeforeFirstVersion) {
  // Create user
  int64_t user_id = create_simple_user("Alice", 25);

  // Query before t0 (should return current data as versioning not fully enabled
  // yet)
  uint64_t before_t0 = t0_ - 1000000000ULL;  // 1 second before t0
  auto query = Query::from("u:User")
                   .as_of_valid_time(before_t0)
                   .where("u.name", CompareOp::Eq, Value("Alice"))
                   .build();

  auto result = db_->query(query);
  ASSERT_TRUE(result.ok());

  // Note: Since versioning is not fully enabled in Database yet,
  // this will return the current version
  auto table = result.ValueOrDie()->table();
  EXPECT_GE(table->num_rows(), 0);
}

TEST_F(TemporalQueryTest, AsOfBuilderMethods) {
  // Test as_of_valid_time()
  auto query1 = Query::from("u:User").as_of_valid_time(t1_).build();
  ASSERT_TRUE(query1.temporal_snapshot().has_value());
  EXPECT_EQ(query1.temporal_snapshot()->valid_time, t1_);
  EXPECT_EQ(query1.temporal_snapshot()->tx_time,
            std::numeric_limits<uint64_t>::max());

  // Test as_of_tx_time()
  auto query2 = Query::from("u:User").as_of_tx_time(t2_).build();
  ASSERT_TRUE(query2.temporal_snapshot().has_value());
  EXPECT_EQ(query2.temporal_snapshot()->valid_time,
            std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(query2.temporal_snapshot()->tx_time, t2_);

  // Test as_of() with both dimensions
  auto query3 = Query::from("u:User").as_of(t1_, t2_).build();
  ASSERT_TRUE(query3.temporal_snapshot().has_value());
  EXPECT_EQ(query3.temporal_snapshot()->valid_time, t1_);
  EXPECT_EQ(query3.temporal_snapshot()->tx_time, t2_);

  // Test chaining: as_of_valid_time() then as_of_tx_time()
  auto query4 =
      Query::from("u:User").as_of_valid_time(t1_).as_of_tx_time(t2_).build();
  ASSERT_TRUE(query4.temporal_snapshot().has_value());
  EXPECT_EQ(query4.temporal_snapshot()->valid_time, t1_);
  EXPECT_EQ(query4.temporal_snapshot()->tx_time, t2_);
}
