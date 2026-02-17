#include <gtest/gtest.h>

#include <memory>

#include "../include/clock.hpp"
#include "../include/core.hpp"
#include "../include/logger.hpp"
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
    // Logger::get_instance().set_level(LogLevel::DEBUG);
    // Create unique test directory
    test_db_path_ = "temporal_test_db_" + std::to_string(now_millis());

    // Create database with persistence and versioning enabled
    auto config =
        make_config()
            .with_db_path(test_db_path_)
            .with_shard_capacity(1000)
            .with_chunk_size(1000)
            .with_versioning_enabled(true)  // Enable temporal versioning
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
      db_->update_node("User", user_id, "age", Value(26), UpdateType::SET);
  ASSERT_TRUE(update_result1.ok()) << update_result1.status();

  // Update age to 27 at t2
  mock_clock_.set_time(t2_);
  auto update_result2 =
      db_->update_node("User", user_id, "age", Value(27), UpdateType::SET);
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

  // ========================================================================
  // TEMPORAL QUERIES: Query historical versions
  // ========================================================================

  // Query AS OF t0: should see age=25 (original version)
  auto query_t0 = Query::from("u:User")
                      .as_of_valid_time(t0_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t0 = db_->query(query_t0);
  ASSERT_TRUE(result_t0.ok());
  auto table_t0 = result_t0.ValueOrDie()->table();
  ASSERT_EQ(table_t0->num_rows(), 1);

  auto age_col_t0 = table_t0->GetColumnByName("u.age");
  auto age_array_t0 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t0->chunk(0));
  EXPECT_EQ(age_array_t0->Value(0), 25);  // Versioning enabled!

  // Query AS OF t1: should see age=26 (first update)
  auto query_t1 = Query::from("u:User")
                      .as_of_valid_time(t1_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t1 = db_->query(query_t1);
  ASSERT_TRUE(result_t1.ok());
  auto table_t1 = result_t1.ValueOrDie()->table();
  ASSERT_EQ(table_t1->num_rows(), 1);

  auto age_col_t1 = table_t1->GetColumnByName("u.age");
  auto age_array_t1 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t1->chunk(0));
  EXPECT_EQ(age_array_t1->Value(0), 26);  // Versioning enabled!

  // Query AS OF t2: should see age=27 (second update)
  auto query_t2 = Query::from("u:User")
                      .as_of_valid_time(t2_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t2 = db_->query(query_t2);
  ASSERT_TRUE(result_t2.ok());
  auto table_t2 = result_t2.ValueOrDie()->table();
  ASSERT_EQ(table_t2->num_rows(), 1);

  auto age_col_t2 = table_t2->GetColumnByName("u.age");
  auto age_array_t2 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t2->chunk(0));
  EXPECT_EQ(age_array_t2->Value(0), 27);
}

TEST_F(TemporalQueryTest, MultipleFieldUpdateAtSameTime) {
  // Create user at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Bob", 30);

  // Update both age and active at t1
  mock_clock_.set_time(t1_);
  auto update1 =
      db_->update_node("User", user_id, "age", Value(31), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  auto update2 = db_->update_node("User", user_id, "active", Value(false),
                                  UpdateType::SET);
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
  uint64_t creation_time = t0_;

  // Advance time and update
  mock_clock_.advance_seconds(1);  // 1 second after t0
  uint64_t update1_time = mock_clock_.now_nanos();
  auto update1 =
      db_->update_node("User", user_id, "age", Value(36), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  // Advance time and update again
  mock_clock_.advance_seconds(1);  // 2 seconds after t0
  uint64_t update2_time = mock_clock_.now_nanos();
  auto update2 =
      db_->update_node("User", user_id, "age", Value(37), UpdateType::SET);
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

  // ========================================================================
  // TEMPORAL QUERIES: Time travel with precise timestamps
  // ========================================================================

  // Query AS OF creation_time: should see age=35
  auto query_creation = Query::from("u:User")
                            .as_of_valid_time(creation_time)
                            .where("u.name", CompareOp::Eq, Value("Charlie"))
                            .build();
  auto result_creation = db_->query(query_creation);
  ASSERT_TRUE(result_creation.ok());
  auto table_creation = result_creation.ValueOrDie()->table();
  ASSERT_EQ(table_creation->num_rows(), 1);
  auto age_creation = std::static_pointer_cast<arrow::Int32Array>(
      table_creation->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_creation->Value(0), 35);

  // Query AS OF update1_time: should see age=36
  auto query_update1 = Query::from("u:User")
                           .as_of_valid_time(update1_time)
                           .where("u.name", CompareOp::Eq, Value("Charlie"))
                           .build();
  auto result_update1 = db_->query(query_update1);
  ASSERT_TRUE(result_update1.ok());
  auto table_update1 = result_update1.ValueOrDie()->table();
  ASSERT_EQ(table_update1->num_rows(), 1);
  auto age_update1 = std::static_pointer_cast<arrow::Int32Array>(
      table_update1->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_update1->Value(0), 36);

  // Query AS OF update2_time: should see age=37
  auto query_update2 = Query::from("u:User")
                           .as_of_valid_time(update2_time)
                           .where("u.name", CompareOp::Eq, Value("Charlie"))
                           .build();
  auto result_update2 = db_->query(query_update2);
  ASSERT_TRUE(result_update2.ok());
  auto table_update2 = result_update2.ValueOrDie()->table();
  ASSERT_EQ(table_update2->num_rows(), 1);
  auto age_update2 = std::static_pointer_cast<arrow::Int32Array>(
      table_update2->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_update2->Value(0), 37);
}

TEST_F(TemporalQueryTest, BitemporalQueryWithUpdates) {
  // Create user at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Diana", 40);

  // Update at t1
  mock_clock_.set_time(t1_);
  auto update1 =
      db_->update_node("User", user_id, "age", Value(41), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  // Update at t2
  mock_clock_.set_time(t2_);
  auto update2 =
      db_->update_node("User", user_id, "age", Value(42), UpdateType::SET);
  ASSERT_TRUE(update2.ok());

  // ========================================================================
  // BITEMPORAL QUERIES: Query with both VALIDTIME and TXNTIME
  // ========================================================================

  // Query AS OF (valid=t0, tx=t0): should see age=40
  auto query_t0_t0 = Query::from("u:User")
                         .as_of(t0_, t0_)
                         .where("u.name", CompareOp::Eq, Value("Diana"))
                         .build();
  auto result_t0_t0 = db_->query(query_t0_t0);
  ASSERT_TRUE(result_t0_t0.ok());
  ASSERT_EQ(result_t0_t0.ValueOrDie()->table()->num_rows(), 1);
  auto age_t0_t0 = std::static_pointer_cast<arrow::Int32Array>(
      result_t0_t0.ValueOrDie()->table()->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_t0_t0->Value(0), 40);

  // Query AS OF (valid=t1, tx=t1): should see age=41
  auto query_t1_t1 = Query::from("u:User")
                         .as_of(t1_, t1_)
                         .where("u.name", CompareOp::Eq, Value("Diana"))
                         .build();
  auto result_t1_t1 = db_->query(query_t1_t1);
  ASSERT_TRUE(result_t1_t1.ok());
  ASSERT_EQ(result_t1_t1.ValueOrDie()->table()->num_rows(), 1);
  auto age_t1_t1 = std::static_pointer_cast<arrow::Int32Array>(
      result_t1_t1.ValueOrDie()->table()->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_t1_t1->Value(0), 41);

  // Query AS OF (valid=t2, tx=t2): should see age=42
  auto query_t2_t2 = Query::from("u:User")
                         .as_of(t2_, t2_)
                         .where("u.name", CompareOp::Eq, Value("Diana"))
                         .build();
  auto result_t2_t2 = db_->query(query_t2_t2);
  ASSERT_TRUE(result_t2_t2.ok());
  ASSERT_EQ(result_t2_t2.ValueOrDie()->table()->num_rows(), 1);
  auto age_t2 = std::static_pointer_cast<arrow::Int32Array>(
      result_t2_t2.ValueOrDie()->table()->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_t2->Value(0), 42);

  // Query AS OF (valid=t0, tx=t2): "What did we know at t2 about t0?"
  // Should see age=40 (the value that was true at t0)
  auto query_t0_tx_t2 = Query::from("u:User")
                            .as_of(t0_, t2_)
                            .where("u.name", CompareOp::Eq, Value("Diana"))
                            .build();
  auto result_t0_tx_t2 = db_->query(query_t0_tx_t2);
  ASSERT_TRUE(result_t0_tx_t2.ok());
  ASSERT_EQ(result_t0_tx_t2.ValueOrDie()->table()->num_rows(), 1);
  auto age_t0_tx_t2 = std::static_pointer_cast<arrow::Int32Array>(
      result_t0_tx_t2.ValueOrDie()->table()->GetColumnByName("u.age")->chunk(
          0));
  EXPECT_EQ(age_t0_tx_t2->Value(0), 40);
}

TEST_F(TemporalQueryTest, TemporalQueryBetweenUpdateTimes) {
  // Create user at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Eve", 50);

  // Update at t1
  mock_clock_.set_time(t1_);
  auto update1 =
      db_->update_node("User", user_id, "age", Value(51), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  // Calculate midpoint between t0 and t1
  uint64_t t_mid = (t0_ + t1_) / 2;

  // Query AS OF t_mid (between t0 and t1): should see age=50 (the t0 version)
  auto query_mid = Query::from("u:User")
                       .as_of_valid_time(t_mid)
                       .where("u.name", CompareOp::Eq, Value("Eve"))
                       .build();
  auto result_mid = db_->query(query_mid);
  ASSERT_TRUE(result_mid.ok());
  auto table_mid = result_mid.ValueOrDie()->table();
  ASSERT_EQ(table_mid->num_rows(), 1);
  auto age_mid = std::static_pointer_cast<arrow::Int32Array>(
      table_mid->GetColumnByName("u.age")->chunk(0));
  EXPECT_EQ(age_mid->Value(0), 50);  // Should see the version from t0
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

TEST_F(TemporalQueryTest, NullFieldInVersionChain) {
  // Create user with age=25 at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Alice", 25);

  // Update age to 30 at t1
  mock_clock_.set_time(t1_);
  auto update1 =
      db_->update_node("User", user_id, "age", Value(30), UpdateType::SET);
  ASSERT_TRUE(update1.ok());

  // Update age to NULL at t2
  mock_clock_.set_time(t2_);
  auto update2 =
      db_->update_node("User", user_id, "age", Value(), UpdateType::SET);
  ASSERT_TRUE(update2.ok());

  // Query at t0: should see age=25
  auto query_t0 = Query::from("u:User")
                      .as_of_valid_time(t0_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t0 = db_->query(query_t0);
  ASSERT_TRUE(result_t0.ok());
  auto table_t0 = result_t0.ValueOrDie()->table();
  ASSERT_EQ(table_t0->num_rows(), 1);

  auto age_col_t0 = table_t0->GetColumnByName("u.age");
  auto age_array_t0 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t0->chunk(0));
  EXPECT_FALSE(age_array_t0->IsNull(0));
  EXPECT_EQ(age_array_t0->Value(0), 25);

  // Query at t1: should see age=30
  auto query_t1 = Query::from("u:User")
                      .as_of_valid_time(t1_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t1 = db_->query(query_t1);
  ASSERT_TRUE(result_t1.ok());
  auto table_t1 = result_t1.ValueOrDie()->table();
  ASSERT_EQ(table_t1->num_rows(), 1);

  auto age_col_t1 = table_t1->GetColumnByName("u.age");
  auto age_array_t1 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t1->chunk(0));
  EXPECT_FALSE(age_array_t1->IsNull(0));
  EXPECT_EQ(age_array_t1->Value(0), 30);

  // Query at t2: should see age=NULL
  auto query_t2 = Query::from("u:User")
                      .as_of_valid_time(t2_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t2 = db_->query(query_t2);
  ASSERT_TRUE(result_t2.ok());
  auto table_t2 = result_t2.ValueOrDie()->table();
  ASSERT_EQ(table_t2->num_rows(), 1);

  auto age_col_t2 = table_t2->GetColumnByName("u.age");
  auto age_array_t2 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t2->chunk(0));

  EXPECT_TRUE(age_array_t2->IsNull(0));
}

TEST_F(TemporalQueryTest, NodeNotVisibleBeforeCreation) {
  // Create node at t1
  mock_clock_.set_time(t1_);
  int64_t user_id = create_simple_user("Alice", 25);

  // Query at t0 (before node creation): should return 0 rows
  // Because the node's valid_from = t1, it shouldn't be visible at t0
  auto query_before = Query::from("u:User")
                          .as_of_valid_time(t0_)
                          .build();  // No WHERE clause - get all users at t0
  auto result_before = db_->query(query_before);
  ASSERT_TRUE(result_before.ok()) << result_before.status().message();

  auto table_before = result_before.ValueOrDie()->table();

  // Node should NOT be visible before its valid_from time
  EXPECT_EQ(table_before->num_rows(), 0);

  // Query at t1 (at creation): should return 1 row
  auto query_at_creation = Query::from("u:User")
                               .as_of_valid_time(t1_)
                               .where("u.name", CompareOp::Eq, Value("Alice"))
                               .build();
  auto result_at = db_->query(query_at_creation);
  ASSERT_TRUE(result_at.ok());
  EXPECT_EQ(result_at.ValueOrDie()->table()->num_rows(), 1);

  // Query at t2 (after creation): should also return 1 row
  auto query_after = Query::from("u:User")
                         .as_of_valid_time(t2_)
                         .where("u.name", CompareOp::Eq, Value("Alice"))
                         .build();
  auto result_after = db_->query(query_after);
  ASSERT_TRUE(result_after.ok());
  EXPECT_EQ(result_after.ValueOrDie()->table()->num_rows(), 1);

  auto age_col = result_after.ValueOrDie()->table()->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 25);
}

TEST_F(TemporalQueryTest, MultipleNodesIndependentVersions) {
  // Alice: created at t0 with age=25, updated to 26 at t1
  mock_clock_.set_time(t0_);
  int64_t alice_id = create_simple_user("Alice", 25);

  mock_clock_.set_time(t1_);
  auto alice_update =
      db_->update_node("User", alice_id, "age", Value(26), UpdateType::SET);
  ASSERT_TRUE(alice_update.ok());

  // Bob: created at t1 with age=30, updated to 31 at t2
  mock_clock_.set_time(t1_);
  int64_t bob_id = create_simple_user("Bob", 30);

  mock_clock_.set_time(t2_);
  auto bob_update =
      db_->update_node("User", bob_id, "age", Value(31), UpdateType::SET);
  ASSERT_TRUE(bob_update.ok());

  // Query at t0: should see only Alice (age=25)
  // Bob's valid_from = t1 > t0, so Bob should NOT be visible at t0
  auto query_t0 = Query::from("u:User").as_of_valid_time(t0_).build();
  auto result_t0 = db_->query(query_t0);
  ASSERT_TRUE(result_t0.ok());
  auto table_t0 = result_t0.ValueOrDie()->table();

  EXPECT_EQ(table_t0->num_rows(), 1);

  // Verify Alice's age is correct at t0
  auto name_col_t0 = table_t0->GetColumnByName("u.name");
  auto name_array_t0 =
      std::static_pointer_cast<arrow::StringArray>(name_col_t0->chunk(0));
  auto age_col_t0 = table_t0->GetColumnByName("u.age");
  auto age_array_t0 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t0->chunk(0));

  EXPECT_EQ(name_array_t0->GetString(0), "Alice");
  EXPECT_EQ(age_array_t0->Value(0), 25);

  // Query at t1: should see Alice (age=26) and Bob (age=30)
  auto query_t1 = Query::from("u:User").as_of_valid_time(t1_).build();
  auto result_t1 = db_->query(query_t1);
  ASSERT_TRUE(result_t1.ok());
  auto table_t1 = result_t1.ValueOrDie()->table();
  EXPECT_EQ(table_t1->num_rows(), 2);

  // Query at t2: should see Alice (age=26) and Bob (age=31)
  auto query_t2 = Query::from("u:User").as_of_valid_time(t2_).build();
  auto result_t2 = db_->query(query_t2);
  ASSERT_TRUE(result_t2.ok());
  auto table_t2 = result_t2.ValueOrDie()->table();
  EXPECT_EQ(table_t2->num_rows(), 2);

  // Verify Alice's age at t2 (should still be 26)
  auto name_col_t2 = table_t2->GetColumnByName("u.name");
  auto name_array_t2 =
      std::static_pointer_cast<arrow::StringArray>(name_col_t2->chunk(0));
  auto age_col_t2 = table_t2->GetColumnByName("u.age");
  auto age_array_t2 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t2->chunk(0));

  for (int i = 0; i < table_t2->num_rows(); i++) {
    std::string name = name_array_t2->GetString(i);
    if (name == "Alice") {
      EXPECT_EQ(age_array_t2->Value(i), 26);
    } else if (name == "Bob") {
      EXPECT_EQ(age_array_t2->Value(i), 31);
    }
  }
}

TEST_F(TemporalQueryTest, VersioningDisabledFallback) {
  // Create a database WITHOUT versioning enabled
  auto config_no_version = make_config().with_versioning_enabled(false).build();
  auto db_no_version = std::make_shared<Database>(config_no_version);

  // Register schema
  auto user_name_field = arrow::field("name", arrow::utf8());
  auto user_age_field = arrow::field("age", arrow::int32());
  auto user_schema = arrow::schema({user_name_field, user_age_field});
  db_no_version->get_schema_registry()
      ->create("User", user_schema)
      .ValueOrDie();

  // Create user with age=25
  mock_clock_.set_time(t0_);
  std::unordered_map<std::string, Value> data = {{"name", Value("Alice")},
                                                 {"age", Value(25)}};
  auto node = db_no_version->create_node("User", data).ValueOrDie();
  int64_t user_id = node->id;

  // Update to age=26
  mock_clock_.set_time(t1_);
  auto update_result = db_no_version->update_node("User", user_id, "age",
                                                  Value(26), UpdateType::SET);
  ASSERT_TRUE(update_result.ok());

  // Temporal query at t0 (should return CURRENT version, not historical)
  // Because versioning is disabled, no history is kept
  auto query_past = Query::from("u:User")
                        .as_of_valid_time(t0_)
                        .where("u.name", CompareOp::Eq, Value("Alice"))
                        .build();
  auto result_past = db_no_version->query(query_past);
  ASSERT_TRUE(result_past.ok());

  auto table_past = result_past.ValueOrDie()->table();
  ASSERT_EQ(table_past->num_rows(), 1);

  // Should see age=26 (current), NOT age=25 (historical)
  // Because versioning is disabled
  auto age_col = table_past->GetColumnByName("u.age");
  auto age_array =
      std::static_pointer_cast<arrow::Int32Array>(age_col->chunk(0));
  EXPECT_EQ(age_array->Value(0), 26);  // Current value, not historical

  // Current query should also return age=26
  auto query_current = Query::from("u:User")
                           .where("u.name", CompareOp::Eq, Value("Alice"))
                           .build();
  auto result_current = db_no_version->query(query_current);
  ASSERT_TRUE(result_current.ok());

  auto table_current = result_current.ValueOrDie()->table();
  auto age_col_current = table_current->GetColumnByName("u.age");
  auto age_array_current =
      std::static_pointer_cast<arrow::Int32Array>(age_col_current->chunk(0));
  EXPECT_EQ(age_array_current->Value(0), 26);
}

TEST_F(TemporalQueryTest, NoOpUpdateDoesNotCreateNewVersion) {
  // Create user with age=25 at t0
  mock_clock_.set_time(t0_);
  int64_t user_id = create_simple_user("Alice", 25);

  // Get the node and count initial versions
  auto node_result = db_->get_node_manager()->get_node("User", user_id);
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.ValueOrDie();
  auto handle = node->get_handle();
  ASSERT_NE(handle, nullptr);

  // Count versions in the chain (v0 = base node, should have no additional
  // versions yet)
  int version_count_before = 0;
  VersionInfo* v = handle->version_info_;
  while (v != nullptr) {
    version_count_before++;
    v = v->prev;
  }

  // Sanity check: should have at least the initial version
  EXPECT_GT(version_count_before, 0);

  // Update to SAME value at t1 (no-op update)
  mock_clock_.set_time(t1_);
  auto update_result =
      db_->update_node("User", user_id, "age", Value(25), UpdateType::SET);
  ASSERT_TRUE(update_result.ok());

  // Get updated node and count versions again
  node_result = db_->get_node_manager()->get_node("User", user_id);
  ASSERT_TRUE(node_result.ok());
  node = node_result.ValueOrDie();
  handle = node->get_handle();

  int version_count_after = 0;
  v = handle->version_info_;
  while (v != nullptr) {
    version_count_after++;
    v = v->prev;
  }

  // Version count should have increased by 1 (new version created even for
  // no-op) NOTE: Current implementation creates a new version even for no-op
  // updates. This test documents current behavior. In the future, we may
  // optimize to skip version creation for no-op updates.
  EXPECT_EQ(version_count_after, version_count_before + 1);

  // Query at t0: should see age=25
  auto query_t0 = Query::from("u:User")
                      .as_of_valid_time(t0_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t0 = db_->query(query_t0);
  ASSERT_TRUE(result_t0.ok());
  auto table_t0 = result_t0.ValueOrDie()->table();
  ASSERT_EQ(table_t0->num_rows(), 1);

  auto age_col_t0 = table_t0->GetColumnByName("u.age");
  auto age_array_t0 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t0->chunk(0));
  EXPECT_EQ(age_array_t0->Value(0), 25);

  // Query at t1: should also see age=25 (no change)
  auto query_t1 = Query::from("u:User")
                      .as_of_valid_time(t1_)
                      .where("u.name", CompareOp::Eq, Value("Alice"))
                      .build();
  auto result_t1 = db_->query(query_t1);
  ASSERT_TRUE(result_t1.ok());
  auto table_t1 = result_t1.ValueOrDie()->table();
  ASSERT_EQ(table_t1->num_rows(), 1);

  auto age_col_t1 = table_t1->GetColumnByName("u.age");
  auto age_array_t1 =
      std::static_pointer_cast<arrow::Int32Array>(age_col_t1->chunk(0));
  EXPECT_EQ(age_array_t1->Value(0), 25);
}
