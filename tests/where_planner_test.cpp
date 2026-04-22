#include "query/where_planner.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

#include "core/edge_store.hpp"
#include "query/execution.hpp"
#include "schema/schema.hpp"

using namespace std::string_literals;
using namespace tundradb;

namespace tundradb {

class WherePlannerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = std::make_shared<SchemaRegistry>();

    auto user_res = registry_->create(
        "User", arrow::schema({arrow::field("x", arrow::int32())}));
    ASSERT_TRUE(user_res.ok()) << user_res.status().ToString();

    auto company_res = registry_->create(
        "Company", arrow::schema({arrow::field("z", arrow::int32()),
                                  arrow::field("country", arrow::utf8())}));
    ASSERT_TRUE(company_res.ok()) << company_res.status().ToString();

    auto region_res = registry_->create(
        "Region", arrow::schema({arrow::field("kind", arrow::utf8())}));
    ASSERT_TRUE(region_res.ok()) << region_res.status().ToString();

    edge_store_ = std::make_shared<EdgeStore>(0);

    auto works_at_res = edge_store_->register_edge_schema(
        "WORKS_AT", {std::make_shared<Field>("y", ValueType::INT32)});
    ASSERT_TRUE(works_at_res.ok()) << works_at_res.status().ToString();

    auto located_in_res = edge_store_->register_edge_schema(
        "LOCATED_IN", {std::make_shared<Field>("weight", ValueType::INT32)});
    ASSERT_TRUE(located_in_res.ok()) << located_in_res.status().ToString();
  }

  void prepare_state(const Query& query, QueryState& state) const {
    state.edge_store = edge_store_;

    auto status = prepare_query(query, state);
    EXPECT_TRUE(status.ok()) << status.ToString();
  }

  std::shared_ptr<SchemaRegistry> registry_;
  std::shared_ptr<EdgeStore> edge_store_;
};

TEST_F(WherePlannerTest, SplitsAndAcrossRootTargetAndEdge) {
  auto query = Query::match("u:User")
                   .traverse("u", "WORKS_AT", "c:Company", TraverseType::Inner,
                             std::optional<std::string>{"e"})
                   .where("u.x", CompareOp::Eq, Value(int32_t(1)))
                   .and_where("e.y", CompareOp::Eq, Value(int32_t(2)))
                   .and_where("c.z", CompareOp::Eq, Value(int32_t(3)))
                   .build();

  QueryState state(registry_);
  prepare_state(query, state);
  auto plan_res = build_where_plan(query, state);
  ASSERT_TRUE(plan_res.ok()) << plan_res.status().ToString();
  const auto& plan = plan_res.ValueOrDie();

  ASSERT_EQ(plan.root_filters.size(), 1u);
  EXPECT_EQ(plan.root_filters[0].source_clause_index, 1u);
  EXPECT_EQ(plan.root_filters[0].expr->extract_first_variable(), "u");
  EXPECT_EQ(plan.root_filters[0].mode, PlannedPredicateMode::Consume);

  ASSERT_EQ(plan.traverse_filters.size(), 1u);
  ASSERT_EQ(plan.traverse_filters[0].target_filters.size(), 1u);
  ASSERT_EQ(plan.traverse_filters[0].edge_filters.size(), 1u);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[0].source_clause_index, 1u);
  EXPECT_EQ(plan.traverse_filters[0].edge_filters[0].source_clause_index, 1u);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[0].mode,
            PlannedPredicateMode::Consume);
  EXPECT_EQ(plan.traverse_filters[0].edge_filters[0].mode,
            PlannedPredicateMode::Consume);
  EXPECT_EQ(
      plan.traverse_filters[0].target_filters[0].expr->extract_first_variable(),
      "c");
  EXPECT_EQ(
      plan.traverse_filters[0].edge_filters[0].expr->extract_first_variable(),
      "e");

  ASSERT_EQ(plan.residual_by_clause.size(), 2u);
  EXPECT_EQ(plan.residual_by_clause[1], nullptr);
}

TEST_F(WherePlannerTest, PullsLaterAliasFiltersBackToEarliestTraverseSlot) {
  auto query = Query::match("u:User")
                   .traverse("u", "WORKS_AT", "c:Company")
                   .where("c.z", CompareOp::Eq, Value(int32_t(3)))
                   .traverse("c", "LOCATED_IN", "r:Region")
                   .where("c.country", CompareOp::Eq, Value("US"s))
                   .and_where("r.kind", CompareOp::Eq, Value("hq"s))
                   .build();

  QueryState state(registry_);
  prepare_state(query, state);
  auto plan_res = build_where_plan(query, state);
  ASSERT_TRUE(plan_res.ok()) << plan_res.status().ToString();
  const auto& plan = plan_res.ValueOrDie();

  ASSERT_EQ(plan.traverse_filters.size(), 2u);
  ASSERT_EQ(plan.traverse_filters[0].target_filters.size(), 2u);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[0].source_clause_index, 1u);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[1].source_clause_index, 3u);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[0].mode,
            PlannedPredicateMode::Consume);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[1].mode,
            PlannedPredicateMode::Consume);
  EXPECT_EQ(
      plan.traverse_filters[0].target_filters[0].expr->extract_first_variable(),
      "c");
  EXPECT_EQ(
      plan.traverse_filters[0].target_filters[1].expr->extract_first_variable(),
      "c");

  ASSERT_EQ(plan.traverse_filters[1].target_filters.size(), 1u);
  EXPECT_EQ(plan.traverse_filters[1].target_filters[0].source_clause_index, 3u);
  EXPECT_EQ(plan.traverse_filters[1].target_filters[0].mode,
            PlannedPredicateMode::Consume);
  EXPECT_EQ(
      plan.traverse_filters[1].target_filters[0].expr->extract_first_variable(),
      "r");

  ASSERT_EQ(plan.residual_by_clause.size(), 4u);
  EXPECT_EQ(plan.residual_by_clause[1], nullptr);
  EXPECT_EQ(plan.residual_by_clause[3], nullptr);
}

TEST_F(WherePlannerTest, KeepsMixedAliasOrAsResidual) {
  auto expr = LogicalExpr::or_expr(
      std::make_shared<ComparisonExpr>("u.x", CompareOp::Eq, Value(int32_t(1))),
      std::make_shared<ComparisonExpr>("c.z", CompareOp::Eq,
                                       Value(int32_t(3))));

  auto query = Query::match("u:User")
                   .traverse("u", "WORKS_AT", "c:Company")
                   .where_logical_expr(expr)
                   .build();

  QueryState state(registry_);
  prepare_state(query, state);
  auto plan_res = build_where_plan(query, state);
  ASSERT_TRUE(plan_res.ok()) << plan_res.status().ToString();
  const auto& plan = plan_res.ValueOrDie();

  EXPECT_TRUE(plan.root_filters.empty());
  ASSERT_EQ(plan.traverse_filters.size(), 1u);
  EXPECT_TRUE(plan.traverse_filters[0].target_filters.empty());
  EXPECT_TRUE(plan.traverse_filters[0].edge_filters.empty());

  ASSERT_EQ(plan.residual_by_clause.size(), 2u);
  ASSERT_NE(plan.residual_by_clause[1], nullptr);
  EXPECT_EQ(plan.residual_by_clause[1]->get_all_variables().size(), 2u);
}

TEST_F(WherePlannerTest, PrefiltersNullableLeftTargetAndKeepsResidual) {
  auto query = Query::match("u:User")
                   .traverse("u", "WORKS_AT", "c:Company", TraverseType::Left)
                   .where("c.z", CompareOp::Eq, Value(int32_t(3)))
                   .traverse("c", "LOCATED_IN", "r:Region")
                   .build();

  QueryState state(registry_);
  prepare_state(query, state);
  auto plan_res = build_where_plan(query, state);
  ASSERT_TRUE(plan_res.ok()) << plan_res.status().ToString();
  const auto& plan = plan_res.ValueOrDie();

  ASSERT_EQ(plan.traverse_filters.size(), 2u);
  ASSERT_EQ(plan.traverse_filters[0].target_filters.size(), 1u);
  EXPECT_EQ(plan.traverse_filters[0].target_filters[0].mode,
            PlannedPredicateMode::PrefilterOnly);
  EXPECT_EQ(
      plan.traverse_filters[0].target_filters[0].expr->extract_first_variable(),
      "c");

  ASSERT_EQ(plan.residual_by_clause.size(), 3u);
  ASSERT_NE(plan.residual_by_clause[1], nullptr);
  EXPECT_EQ(plan.residual_by_clause[1]->extract_first_variable(), "c");
}

}  // namespace tundradb
