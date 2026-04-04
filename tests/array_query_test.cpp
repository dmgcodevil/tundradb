#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/clock.hpp"
#include "main/database.hpp"
#include "query/query.hpp"
#include "common/utils.hpp"

#define ASSERT_OK(expr) ASSERT_TRUE((expr).ok())

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {

class ArrayQueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("tags", arrow::list(arrow::utf8())),
        arrow::field("scores", arrow::list(arrow::int32())),
    });

    auto db_path = "array_query_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(db_path)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .build();

    db_ = std::make_shared<Database>(config);
    db_->get_schema_registry()->create("Item", schema).ValueOrDie();

    create_test_data();
  }

  void create_test_data() {
    // Item 0: Alice with tags and scores
    {
      std::vector<Value> tags = {Value{"cpp"s}, Value{"rust"s}};
      std::vector<Value> scores = {Value{int32_t(90)}, Value{int32_t(85)}};
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Alice"s}},
          {"tags", Value{tags}},
          {"scores", Value{scores}},
      };
      db_->create_node("Item", data).ValueOrDie();
    }

    // Item 1: Bob with tags, no scores (empty array)
    {
      std::vector<Value> tags = {Value{"java"s}, Value{"go"s},
                                 Value{"python"s}};
      std::vector<Value> scores = {};
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Bob"s}},
          {"tags", Value{tags}},
          {"scores", Value{scores}},
      };
      db_->create_node("Item", data).ValueOrDie();
    }

    // Item 2: Charlie with no tags (empty), has scores
    {
      std::vector<Value> tags = {};
      std::vector<Value> scores = {Value{int32_t(100)}};
      std::unordered_map<std::string, Value> data = {
          {"name", Value{"Charlie"s}},
          {"tags", Value{tags}},
          {"scores", Value{scores}},
      };
      db_->create_node("Item", data).ValueOrDie();
    }
  }

  /// Query the "Item" table and return the full Arrow table.
  std::shared_ptr<arrow::Table> query_items() {
    auto query = Query::from("i:Item").build();
    auto result = db_->query(query).ValueOrDie();
    return result->table();
  }

  /// Extract list column values for a specific row index.
  template <typename T>
  std::vector<T> get_list_at_row(const std::shared_ptr<arrow::Table>& table,
                                 const std::string& col_name, int64_t row) {
    auto column = table->GetColumnByName(col_name);
    EXPECT_NE(column, nullptr) << "Column not found: " << col_name;
    if (!column) return {};

    int64_t offset = 0;
    for (int c = 0; c < column->num_chunks(); ++c) {
      auto chunk = column->chunk(c);
      if (row < offset + chunk->length()) {
        auto list_arr = std::static_pointer_cast<arrow::ListArray>(chunk);
        int64_t local = row - offset;
        if (list_arr->IsNull(local)) return {};
        auto values = list_arr->value_slice(local);
        std::vector<T> result;
        if constexpr (std::is_same_v<T, int32_t>) {
          auto typed = std::static_pointer_cast<arrow::Int32Array>(values);
          for (int64_t i = 0; i < typed->length(); ++i)
            result.push_back(typed->Value(i));
        } else if constexpr (std::is_same_v<T, std::string>) {
          auto typed = std::static_pointer_cast<arrow::StringArray>(values);
          for (int64_t i = 0; i < typed->length(); ++i)
            result.push_back(typed->GetString(i));
        }
        return result;
      }
      offset += chunk->length();
    }
    return {};
  }

  /// Find the row index for a given name in the query result.
  int64_t find_row_by_name(const std::shared_ptr<arrow::Table>& table,
                           const std::string& name) {
    auto names = get_column_values<std::string>(table, "i.name").ValueOrDie();
    for (size_t i = 0; i < names.size(); ++i) {
      if (names[i] == name) return static_cast<int64_t>(i);
    }
    return -1;
  }

  std::shared_ptr<Database> db_;
};

// =========================================================================
// Query tests — verify arrays come back in Arrow table
// =========================================================================

TEST_F(ArrayQueryTest, QueryReturnsTable) {
  auto table = query_items();
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 3);
}

TEST_F(ArrayQueryTest, QueryTableHasListColumns) {
  auto table = query_items();
  auto schema = table->schema();

  auto tags_field = schema->GetFieldByName("i.tags");
  ASSERT_NE(tags_field, nullptr);
  EXPECT_EQ(tags_field->type()->id(), arrow::Type::LIST);

  auto scores_field = schema->GetFieldByName("i.scores");
  ASSERT_NE(scores_field, nullptr);
  EXPECT_EQ(scores_field->type()->id(), arrow::Type::LIST);
}

TEST_F(ArrayQueryTest, QueryStringArrayValues) {
  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  ASSERT_EQ(tags.size(), 2);
  EXPECT_EQ(tags[0], "cpp");
  EXPECT_EQ(tags[1], "rust");
}

TEST_F(ArrayQueryTest, QueryInt32ArrayValues) {
  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto scores = get_list_at_row<int32_t>(table, "i.scores", alice_row);
  ASSERT_EQ(scores.size(), 2);
  EXPECT_EQ(scores[0], 90);
  EXPECT_EQ(scores[1], 85);
}

TEST_F(ArrayQueryTest, QueryMultiElementStringArray) {
  auto table = query_items();
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", bob_row);
  ASSERT_EQ(tags.size(), 3);
  EXPECT_EQ(tags[0], "java");
  EXPECT_EQ(tags[1], "go");
  EXPECT_EQ(tags[2], "python");
}

TEST_F(ArrayQueryTest, QueryEmptyArray) {
  auto table = query_items();
  int64_t charlie_row = find_row_by_name(table, "Charlie");
  ASSERT_GE(charlie_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", charlie_row);
  EXPECT_TRUE(tags.empty());
}

TEST_F(ArrayQueryTest, QueryEmptyInt32Array) {
  auto table = query_items();
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);

  auto scores = get_list_at_row<int32_t>(table, "i.scores", bob_row);
  EXPECT_TRUE(scores.empty());
}

TEST_F(ArrayQueryTest, QuerySingleElementArray) {
  auto table = query_items();
  int64_t charlie_row = find_row_by_name(table, "Charlie");
  ASSERT_GE(charlie_row, 0);

  auto scores = get_list_at_row<int32_t>(table, "i.scores", charlie_row);
  ASSERT_EQ(scores.size(), 1);
  EXPECT_EQ(scores[0], 100);
}

// =========================================================================
// Update tests — update array fields and verify via query
// =========================================================================

TEST_F(ArrayQueryTest, UpdateArrayByIdReplaceFull) {
  std::vector<Value> new_tags = {Value{"scala"s}, Value{"kotlin"s},
                                 Value{"haskell"s}};
  auto uq = UpdateQuery::on("Item", 0).set("tags", Value{new_tags}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  ASSERT_EQ(tags.size(), 3);
  EXPECT_EQ(tags[0], "scala");
  EXPECT_EQ(tags[1], "kotlin");
  EXPECT_EQ(tags[2], "haskell");
}

TEST_F(ArrayQueryTest, UpdateArrayByIdToEmpty) {
  std::vector<Value> empty_tags = {};
  auto uq = UpdateQuery::on("Item", 0).set("tags", Value{empty_tags}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  EXPECT_TRUE(tags.empty());
}

TEST_F(ArrayQueryTest, UpdateInt32ArrayById) {
  std::vector<Value> new_scores = {Value{int32_t(10)}, Value{int32_t(20)},
                                   Value{int32_t(30)}};
  auto uq = UpdateQuery::on("Item", 0).set("scores", Value{new_scores}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto scores = get_list_at_row<int32_t>(table, "i.scores", alice_row);
  ASSERT_EQ(scores.size(), 3);
  EXPECT_EQ(scores[0], 10);
  EXPECT_EQ(scores[1], 20);
  EXPECT_EQ(scores[2], 30);
}

TEST_F(ArrayQueryTest, UpdateArrayDoesNotAffectOtherNodes) {
  std::vector<Value> new_tags = {Value{"updated"s}};
  auto uq = UpdateQuery::on("Item", 0).set("tags", Value{new_tags}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto table = query_items();

  // Bob's tags unchanged
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);
  auto bob_tags = get_list_at_row<std::string>(table, "i.tags", bob_row);
  ASSERT_EQ(bob_tags.size(), 3);
  EXPECT_EQ(bob_tags[0], "java");
  EXPECT_EQ(bob_tags[1], "go");
  EXPECT_EQ(bob_tags[2], "python");
}

TEST_F(ArrayQueryTest, UpdateArrayDoesNotAffectOtherFields) {
  std::vector<Value> new_tags = {Value{"updated"s}};
  auto uq = UpdateQuery::on("Item", 0).set("tags", Value{new_tags}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  // Name unchanged
  auto names = get_column_values<std::string>(table, "i.name").ValueOrDie();
  EXPECT_EQ(names[alice_row], "Alice");

  // Scores unchanged
  auto scores = get_list_at_row<int32_t>(table, "i.scores", alice_row);
  ASSERT_EQ(scores.size(), 2);
  EXPECT_EQ(scores[0], 90);
  EXPECT_EQ(scores[1], 85);
}

TEST_F(ArrayQueryTest, SequentialArrayUpdatesAccumulate) {
  std::vector<Value> tags1 = {Value{"first"s}};
  ASSERT_OK(db_->update(
      UpdateQuery::on("Item", 0).set("tags", Value{tags1}).build()));

  std::vector<Value> tags2 = {Value{"second"s}, Value{"third"s}};
  ASSERT_OK(db_->update(
      UpdateQuery::on("Item", 0).set("tags", Value{tags2}).build()));

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  ASSERT_EQ(tags.size(), 2);
  EXPECT_EQ(tags[0], "second");
  EXPECT_EQ(tags[1], "third");
}

TEST_F(ArrayQueryTest, UpdateByMatchSetsArray) {
  auto q = Query::from("i:Item")
               .where("i.name", CompareOp::Eq, Value("Bob"s))
               .build();
  std::vector<Value> new_tags = {Value{"matched"s}};
  auto uq = UpdateQuery::match(q).set("i.tags", Value{new_tags}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  auto table = query_items();
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", bob_row);
  ASSERT_EQ(tags.size(), 1);
  EXPECT_EQ(tags[0], "matched");
}

// =========================================================================
// APPEND tests — append elements to existing array fields
// =========================================================================

TEST_F(ArrayQueryTest, AppendToStringArrayById) {
  // Alice has tags ["cpp", "rust"], append "python"
  std::vector<Value> to_append = {Value{"python"s}};
  auto uq = UpdateQuery::on("Item", 0).append("tags", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  ASSERT_EQ(tags.size(), 3);
  EXPECT_EQ(tags[0], "cpp");
  EXPECT_EQ(tags[1], "rust");
  EXPECT_EQ(tags[2], "python");
}

TEST_F(ArrayQueryTest, AppendToInt32ArrayById) {
  // Alice has scores [90, 85], append 95
  std::vector<Value> to_append = {Value{int32_t(95)}};
  auto uq =
      UpdateQuery::on("Item", 0).append("scores", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto scores = get_list_at_row<int32_t>(table, "i.scores", alice_row);
  ASSERT_EQ(scores.size(), 3);
  EXPECT_EQ(scores[0], 90);
  EXPECT_EQ(scores[1], 85);
  EXPECT_EQ(scores[2], 95);
}

TEST_F(ArrayQueryTest, AppendMultipleElementsToArray) {
  // Alice has tags ["cpp", "rust"], append "go" and "java"
  std::vector<Value> to_append = {Value{"go"s}, Value{"java"s}};
  auto uq = UpdateQuery::on("Item", 0).append("tags", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  ASSERT_EQ(tags.size(), 4);
  EXPECT_EQ(tags[0], "cpp");
  EXPECT_EQ(tags[1], "rust");
  EXPECT_EQ(tags[2], "go");
  EXPECT_EQ(tags[3], "java");
}

TEST_F(ArrayQueryTest, AppendToEmptyArray) {
  // Bob has empty scores [], append 42
  std::vector<Value> to_append = {Value{int32_t(42)}};
  auto uq =
      UpdateQuery::on("Item", 1).append("scores", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  auto table = query_items();
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);

  auto scores = get_list_at_row<int32_t>(table, "i.scores", bob_row);
  ASSERT_EQ(scores.size(), 1);
  EXPECT_EQ(scores[0], 42);
}

TEST_F(ArrayQueryTest, AppendDoesNotAffectOtherNodes) {
  // Append to Alice, Bob should be unchanged
  std::vector<Value> to_append = {Value{"extra"s}};
  auto uq = UpdateQuery::on("Item", 0).append("tags", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  auto table = query_items();
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);

  auto bob_tags = get_list_at_row<std::string>(table, "i.tags", bob_row);
  ASSERT_EQ(bob_tags.size(), 3);
  EXPECT_EQ(bob_tags[0], "java");
  EXPECT_EQ(bob_tags[1], "go");
  EXPECT_EQ(bob_tags[2], "python");
}

TEST_F(ArrayQueryTest, SequentialAppends) {
  // Append "a" then "b" to Charlie's empty tags
  {
    std::vector<Value> to_append = {Value{"a"s}};
    auto uq =
        UpdateQuery::on("Item", 2).append("tags", Value{to_append}).build();
    ASSERT_OK(db_->update(uq));
  }
  {
    std::vector<Value> to_append = {Value{"b"s}};
    auto uq =
        UpdateQuery::on("Item", 2).append("tags", Value{to_append}).build();
    ASSERT_OK(db_->update(uq));
  }

  auto table = query_items();
  int64_t charlie_row = find_row_by_name(table, "Charlie");
  ASSERT_GE(charlie_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", charlie_row);
  ASSERT_EQ(tags.size(), 2);
  EXPECT_EQ(tags[0], "a");
  EXPECT_EQ(tags[1], "b");
}

TEST_F(ArrayQueryTest, AppendEmptyVectorIsNoop) {
  // Append empty vector to Alice's tags — should remain unchanged
  std::vector<Value> to_append = {};
  auto uq = UpdateQuery::on("Item", 0).append("tags", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  auto table = query_items();
  int64_t alice_row = find_row_by_name(table, "Alice");
  ASSERT_GE(alice_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", alice_row);
  ASSERT_EQ(tags.size(), 2);
  EXPECT_EQ(tags[0], "cpp");
  EXPECT_EQ(tags[1], "rust");
}

TEST_F(ArrayQueryTest, AppendByMatchQuery) {
  // Append "matched" to all Items where name = "Bob"
  auto q = Query::from("i:Item")
               .where("i.name", CompareOp::Eq, Value("Bob"s))
               .build();
  std::vector<Value> to_append = {Value{"matched"s}};
  auto uq = UpdateQuery::match(q).append("i.tags", Value{to_append}).build();

  auto result = db_->update(uq);
  ASSERT_OK(result);
  EXPECT_EQ(result.ValueOrDie().updated_count, 1);

  auto table = query_items();
  int64_t bob_row = find_row_by_name(table, "Bob");
  ASSERT_GE(bob_row, 0);

  auto tags = get_list_at_row<std::string>(table, "i.tags", bob_row);
  ASSERT_EQ(tags.size(), 4);
  EXPECT_EQ(tags[0], "java");
  EXPECT_EQ(tags[1], "go");
  EXPECT_EQ(tags[2], "python");
  EXPECT_EQ(tags[3], "matched");
}

// =========================================================================
// Versioned array tests — verify SET/APPEND create proper versions
// =========================================================================

class VersionedArrayTest : public ::testing::Test {
 protected:
  std::shared_ptr<Database> db_;
  std::string test_db_path_;
  MockClock mock_clock_;

  uint64_t t0_;
  uint64_t t1_;
  uint64_t t2_;
  uint64_t t3_;

  void SetUp() override {
    Clock::set_instance(&mock_clock_);

    test_db_path_ = "versioned_array_test_db_" + std::to_string(now_millis());
    auto config = make_config()
                      .with_db_path(test_db_path_)
                      .with_shard_capacity(1000)
                      .with_chunk_size(1000)
                      .with_versioning_enabled(true)
                      .build();

    db_ = std::make_shared<Database>(config);

    auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("tags", arrow::list(arrow::utf8())),
        arrow::field("scores", arrow::list(arrow::int32())),
    });
    db_->get_schema_registry()->create("Item", schema).ValueOrDie();

    t0_ = 1685577600000000000ULL;  // 2023-06-01
    t1_ = 1688169600000000000ULL;  // 2023-07-01
    t2_ = 1690848000000000000ULL;  // 2023-08-01
    t3_ = 1693526400000000000ULL;  // 2023-09-01

    mock_clock_.set_time(t0_);
  }

  void TearDown() override {
    Clock::reset();
    std::filesystem::remove_all(test_db_path_);
  }

  int64_t create_item(const std::string& name, std::vector<Value> tags,
                      std::vector<Value> scores) {
    std::unordered_map<std::string, Value> data = {
        {"name", Value{name}},
        {"tags", Value{tags}},
        {"scores", Value{scores}},
    };
    return db_->create_node("Item", data).ValueOrDie()->id;
  }

  std::shared_ptr<arrow::Table> query_items() {
    auto query = Query::from("i:Item").build();
    return db_->query(query).ValueOrDie()->table();
  }

  std::shared_ptr<arrow::Table> query_items_as_of(uint64_t valid_time) {
    auto query = Query::from("i:Item").as_of_valid_time(valid_time).build();
    return db_->query(query).ValueOrDie()->table();
  }

  template <typename T>
  std::vector<T> get_list_at_row(const std::shared_ptr<arrow::Table>& table,
                                 const std::string& col_name, int64_t row) {
    auto column = table->GetColumnByName(col_name);
    EXPECT_NE(column, nullptr) << "Column not found: " << col_name;
    if (!column) return {};

    int64_t offset = 0;
    for (int c = 0; c < column->num_chunks(); ++c) {
      auto chunk = column->chunk(c);
      if (row < offset + chunk->length()) {
        auto list_arr = std::static_pointer_cast<arrow::ListArray>(chunk);
        int64_t local = row - offset;
        if (list_arr->IsNull(local)) return {};
        auto values = list_arr->value_slice(local);
        std::vector<T> result;
        if constexpr (std::is_same_v<T, int32_t>) {
          auto typed = std::static_pointer_cast<arrow::Int32Array>(values);
          for (int64_t i = 0; i < typed->length(); ++i)
            result.push_back(typed->Value(i));
        } else if constexpr (std::is_same_v<T, std::string>) {
          auto typed = std::static_pointer_cast<arrow::StringArray>(values);
          for (int64_t i = 0; i < typed->length(); ++i)
            result.push_back(typed->GetString(i));
        }
        return result;
      }
      offset += chunk->length();
    }
    return {};
  }
};

TEST_F(VersionedArrayTest, SetArrayCreatesVersion) {
  mock_clock_.set_time(t0_);
  int64_t id = create_item("Alice", {Value{"cpp"s}, Value{"rust"s}},
                           {Value{int32_t(90)}});

  // SET tags to new array at t1
  mock_clock_.set_time(t1_);
  std::vector<Value> new_tags = {Value{"go"s}, Value{"java"s},
                                 Value{"python"s}};
  auto uq = UpdateQuery::on("Item", id).set("tags", Value{new_tags}).build();
  ASSERT_OK(db_->update(uq));

  // Current view: should see the new tags
  auto current = query_items();
  ASSERT_EQ(current->num_rows(), 1);
  auto tags_now = get_list_at_row<std::string>(current, "i.tags", 0);
  ASSERT_EQ(tags_now.size(), 3);
  EXPECT_EQ(tags_now[0], "go");
  EXPECT_EQ(tags_now[1], "java");
  EXPECT_EQ(tags_now[2], "python");

  // Query as-of t0: should see the original tags
  auto past = query_items_as_of(t0_);
  ASSERT_EQ(past->num_rows(), 1);
  auto tags_t0 = get_list_at_row<std::string>(past, "i.tags", 0);
  ASSERT_EQ(tags_t0.size(), 2);
  EXPECT_EQ(tags_t0[0], "cpp");
  EXPECT_EQ(tags_t0[1], "rust");
}

TEST_F(VersionedArrayTest, AppendArrayCreatesVersion) {
  mock_clock_.set_time(t0_);
  int64_t id = create_item("Bob", {Value{"java"s}}, {});

  // APPEND "go" at t1
  mock_clock_.set_time(t1_);
  std::vector<Value> to_append = {Value{"go"s}};
  auto uq =
      UpdateQuery::on("Item", id).append("tags", Value{to_append}).build();
  ASSERT_OK(db_->update(uq));

  // Current: ["java", "go"]
  auto current = query_items();
  auto tags_now = get_list_at_row<std::string>(current, "i.tags", 0);
  ASSERT_EQ(tags_now.size(), 2);
  EXPECT_EQ(tags_now[0], "java");
  EXPECT_EQ(tags_now[1], "go");

  // As-of t0: should still be ["java"]
  auto past = query_items_as_of(t0_);
  auto tags_t0 = get_list_at_row<std::string>(past, "i.tags", 0);
  ASSERT_EQ(tags_t0.size(), 1);
  EXPECT_EQ(tags_t0[0], "java");
}

TEST_F(VersionedArrayTest, MultipleAppendVersions) {
  mock_clock_.set_time(t0_);
  int64_t id = create_item("Charlie", {Value{"a"s}}, {});

  // Append "b" at t1
  mock_clock_.set_time(t1_);
  ASSERT_OK(
      db_->update(UpdateQuery::on("Item", id)
                      .append("tags", Value{std::vector<Value>{Value{"b"s}}})
                      .build()));

  // Append "c" at t2
  mock_clock_.set_time(t2_);
  ASSERT_OK(
      db_->update(UpdateQuery::on("Item", id)
                      .append("tags", Value{std::vector<Value>{Value{"c"s}}})
                      .build()));

  // Current: ["a", "b", "c"]
  auto current = query_items();
  auto tags_now = get_list_at_row<std::string>(current, "i.tags", 0);
  ASSERT_EQ(tags_now.size(), 3);
  EXPECT_EQ(tags_now[0], "a");
  EXPECT_EQ(tags_now[1], "b");
  EXPECT_EQ(tags_now[2], "c");

  // As-of t0: ["a"]
  auto t0_table = query_items_as_of(t0_);
  auto tags_t0 = get_list_at_row<std::string>(t0_table, "i.tags", 0);
  ASSERT_EQ(tags_t0.size(), 1);
  EXPECT_EQ(tags_t0[0], "a");

  // As-of t1: ["a", "b"]
  auto t1_table = query_items_as_of(t1_);
  auto tags_t1 = get_list_at_row<std::string>(t1_table, "i.tags", 0);
  ASSERT_EQ(tags_t1.size(), 2);
  EXPECT_EQ(tags_t1[0], "a");
  EXPECT_EQ(tags_t1[1], "b");
}

TEST_F(VersionedArrayTest, SetAfterAppendPreservesHistory) {
  mock_clock_.set_time(t0_);
  int64_t id = create_item("Dave", {Value{"x"s}}, {});

  // Append "y" at t1
  mock_clock_.set_time(t1_);
  ASSERT_OK(
      db_->update(UpdateQuery::on("Item", id)
                      .append("tags", Value{std::vector<Value>{Value{"y"s}}})
                      .build()));

  // SET to completely new array at t2
  mock_clock_.set_time(t2_);
  std::vector<Value> fresh = {Value{"new1"s}, Value{"new2"s}};
  ASSERT_OK(db_->update(
      UpdateQuery::on("Item", id).set("tags", Value{fresh}).build()));

  // Current: ["new1", "new2"]
  auto current = query_items();
  auto tags_now = get_list_at_row<std::string>(current, "i.tags", 0);
  ASSERT_EQ(tags_now.size(), 2);
  EXPECT_EQ(tags_now[0], "new1");
  EXPECT_EQ(tags_now[1], "new2");

  // As-of t1: ["x", "y"] (append result)
  auto t1_table = query_items_as_of(t1_);
  auto tags_t1 = get_list_at_row<std::string>(t1_table, "i.tags", 0);
  ASSERT_EQ(tags_t1.size(), 2);
  EXPECT_EQ(tags_t1[0], "x");
  EXPECT_EQ(tags_t1[1], "y");

  // As-of t0: ["x"] (original)
  auto t0_table = query_items_as_of(t0_);
  auto tags_t0 = get_list_at_row<std::string>(t0_table, "i.tags", 0);
  ASSERT_EQ(tags_t0.size(), 1);
  EXPECT_EQ(tags_t0[0], "x");
}

TEST_F(VersionedArrayTest, AppendInt32WithVersioning) {
  mock_clock_.set_time(t0_);
  int64_t id = create_item("Eve", {}, {Value{int32_t(10)}, Value{int32_t(20)}});

  // Append 30 at t1
  mock_clock_.set_time(t1_);
  ASSERT_OK(db_->update(
      UpdateQuery::on("Item", id)
          .append("scores", Value{std::vector<Value>{Value{int32_t(30)}}})
          .build()));

  // Current: [10, 20, 30]
  auto current = query_items();
  auto scores_now = get_list_at_row<int32_t>(current, "i.scores", 0);
  ASSERT_EQ(scores_now.size(), 3);
  EXPECT_EQ(scores_now[0], 10);
  EXPECT_EQ(scores_now[1], 20);
  EXPECT_EQ(scores_now[2], 30);

  // As-of t0: [10, 20]
  auto past = query_items_as_of(t0_);
  auto scores_t0 = get_list_at_row<int32_t>(past, "i.scores", 0);
  ASSERT_EQ(scores_t0.size(), 2);
  EXPECT_EQ(scores_t0[0], 10);
  EXPECT_EQ(scores_t0[1], 20);
}

TEST_F(VersionedArrayTest, AppendToEmptyArrayWithVersioning) {
  mock_clock_.set_time(t0_);
  int64_t id = create_item("Frank", {}, {});

  // Append to empty tags at t1
  mock_clock_.set_time(t1_);
  ASSERT_OK(db_->update(
      UpdateQuery::on("Item", id)
          .append("tags", Value{std::vector<Value>{Value{"hello"s}}})
          .build()));

  // Current: ["hello"]
  auto current = query_items();
  auto tags_now = get_list_at_row<std::string>(current, "i.tags", 0);
  ASSERT_EQ(tags_now.size(), 1);
  EXPECT_EQ(tags_now[0], "hello");

  // As-of t0: empty
  auto past = query_items_as_of(t0_);
  auto tags_t0 = get_list_at_row<std::string>(past, "i.tags", 0);
  EXPECT_TRUE(tags_t0.empty());
}

}  // namespace tundradb
