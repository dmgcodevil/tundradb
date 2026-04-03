#include "../include/string_arena.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

using namespace tundradb;

// ============================================================================
// StringPool direct tests
// ============================================================================

class StringPoolTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = std::make_unique<StringPool>(64, 4096);
    StringArenaRegistry::register_pool(100, pool_.get());
  }

  std::unique_ptr<StringPool> pool_;
};

TEST_F(StringPoolTest, StoreAndRetrieve) {
  auto res = pool_->store_string(std::string("hello"), 100);
  ASSERT_TRUE(res.ok());
  StringRef ref = std::move(*res);
  EXPECT_EQ(ref.view(), "hello");
  EXPECT_EQ(ref.get_ref_count(), 1);
}

TEST_F(StringPoolTest, StringExceedsMaxSize) {
  auto small_pool = std::make_unique<StringPool>(4, 4096);
  auto res = small_pool->store_string(std::string("too_long_string"), 100);
  EXPECT_FALSE(res.ok());
  EXPECT_TRUE(res.status().IsInvalid());
}

TEST_F(StringPoolTest, StoreStringView) {
  std::string_view sv = "from_view";
  auto res = pool_->store_string(sv, 100);
  ASSERT_TRUE(res.ok());
  EXPECT_EQ(res.ValueOrDie().view(), "from_view");
}

TEST_F(StringPoolTest, GetMaxSize) { EXPECT_EQ(pool_->get_max_size(), 64u); }

TEST_F(StringPoolTest, GetActiveAllocs) {
  EXPECT_EQ(pool_->get_active_allocs(), 0);
  auto ref = pool_->store_string(std::string("a"), 100).ValueOrDie();
  EXPECT_EQ(pool_->get_active_allocs(), 1);
}

TEST_F(StringPoolTest, GetTotalAllocated) {
  size_t initial = pool_->get_total_allocated();
  auto ref = pool_->store_string(std::string("abc"), 100).ValueOrDie();
  EXPECT_GE(pool_->get_total_allocated(), initial);
  EXPECT_GT(pool_->get_total_allocated(), 0u);
}

TEST_F(StringPoolTest, GetUsedBytes) {
  EXPECT_EQ(pool_->get_used_bytes(), 0u);
  auto ref = pool_->store_string(std::string("abc"), 100).ValueOrDie();
  EXPECT_GT(pool_->get_used_bytes(), 0u);
}

TEST_F(StringPoolTest, GetStringCountWithDedup) {
  pool_->enable_deduplication(true);
  EXPECT_EQ(pool_->get_string_count(), 0u);
  auto ref1 = pool_->store_string(std::string("x"), 100).ValueOrDie();
  EXPECT_EQ(pool_->get_string_count(), 1u);
  auto ref2 = pool_->store_string(std::string("y"), 100).ValueOrDie();
  EXPECT_EQ(pool_->get_string_count(), 2u);
}

TEST_F(StringPoolTest, GetTotalReferencesWithDedup) {
  pool_->enable_deduplication(true);
  EXPECT_EQ(pool_->get_total_references(), 0u);

  auto ref1 = pool_->store_string(std::string("dup"), 100).ValueOrDie();
  EXPECT_GT(pool_->get_total_references(), 0u);

  auto ref2 = pool_->store_string(std::string("dup"), 100).ValueOrDie();
  EXPECT_GE(pool_->get_total_references(), 2u);
}

TEST_F(StringPoolTest, ResetClearsEverything) {
  auto ref = pool_->store_string(std::string("data"), 100).ValueOrDie();
  EXPECT_GT(pool_->get_used_bytes(), 0u);
  pool_->reset();
  EXPECT_EQ(pool_->get_used_bytes(), 0u);
}

TEST_F(StringPoolTest, ClearDeallocatesMemory) {
  {
    auto ref = pool_->store_string(std::string("data"), 100).ValueOrDie();
  }
  pool_->clear();
  EXPECT_EQ(pool_->get_used_bytes(), 0u);
}

TEST_F(StringPoolTest, DeduplicationSharesMemory) {
  pool_->enable_deduplication(true);
  auto ref1 = pool_->store_string(std::string("shared"), 100).ValueOrDie();
  auto ref2 = pool_->store_string(std::string("shared"), 100).ValueOrDie();
  EXPECT_EQ(ref1.data(), ref2.data());
  EXPECT_GE(ref1.get_ref_count(), 2);
}

TEST_F(StringPoolTest, DisableDeduplicationClearsCache) {
  pool_->enable_deduplication(true);
  auto ref = pool_->store_string(std::string("cached"), 100).ValueOrDie();
  pool_->enable_deduplication(false);
  EXPECT_EQ(pool_->get_string_count(), 0u);
}

// ============================================================================
// StringArena (multi-pool) tests
// ============================================================================

class StringArenaTest : public ::testing::Test {
 protected:
  void SetUp() override { arena_ = std::make_unique<StringArena>(); }
  std::unique_ptr<StringArena> arena_;
};

TEST_F(StringArenaTest, StoreStringAutoRoutesToCorrectPool) {
  auto short_ref = arena_->store_string_auto(std::string("hi")).ValueOrDie();
  EXPECT_EQ(short_ref.pool_id(), 0u);

  std::string medium(20, 'x');
  auto med_ref = arena_->store_string_auto(medium).ValueOrDie();
  EXPECT_EQ(med_ref.pool_id(), 1u);

  std::string longer(50, 'y');
  auto long_ref = arena_->store_string_auto(longer).ValueOrDie();
  EXPECT_EQ(long_ref.pool_id(), 2u);

  std::string huge(100, 'z');
  auto huge_ref = arena_->store_string_auto(huge).ValueOrDie();
  EXPECT_EQ(huge_ref.pool_id(), 3u);
}

TEST_F(StringArenaTest, StoreStringByPoolId) {
  auto ref = arena_->store_string(std::string("test"), 3).ValueOrDie();
  EXPECT_EQ(ref.pool_id(), 3u);
  EXPECT_EQ(ref.view(), "test");
}

TEST_F(StringArenaTest, StoreStringInvalidPoolId) {
  auto res = arena_->store_string(std::string("oops"), 99);
  EXPECT_FALSE(res.ok());
  EXPECT_TRUE(res.status().IsInvalid());
}

TEST_F(StringArenaTest, GetStringView) {
  auto ref = arena_->store_string_auto(std::string("readable")).ValueOrDie();
  std::string_view sv = arena_->get_string_view(ref);
  EXPECT_EQ(sv, "readable");
}

TEST_F(StringArenaTest, MarkForDeletion) {
  auto ref = arena_->store_string_auto(std::string("deleteme")).ValueOrDie();
  arena_->mark_for_deletion(ref);
  EXPECT_TRUE(ref.is_marked_for_deletion());
}

TEST_F(StringArenaTest, MarkForDeletionNullRef) {
  StringRef null_ref;
  arena_->mark_for_deletion(null_ref);
}

TEST_F(StringArenaTest, GetActiveAllocs) {
  EXPECT_EQ(arena_->get_active_allocs(), 0);
  auto ref = arena_->store_string_auto(std::string("a")).ValueOrDie();
  EXPECT_EQ(arena_->get_active_allocs(), 1);
}

TEST_F(StringArenaTest, GetPoolValid) {
  auto* pool = arena_->get_pool(0);
  EXPECT_NE(pool, nullptr);
  EXPECT_EQ(pool->get_max_size(), 16u);
}

TEST_F(StringArenaTest, GetPoolInvalid) {
  auto* pool = arena_->get_pool(99);
  EXPECT_EQ(pool, nullptr);
}

TEST_F(StringArenaTest, EnableDeduplication) {
  arena_->enable_deduplication(true);
  auto ref1 = arena_->store_string_auto(std::string("dup")).ValueOrDie();
  auto ref2 = arena_->store_string_auto(std::string("dup")).ValueOrDie();
  EXPECT_EQ(ref1.data(), ref2.data());
}

TEST_F(StringArenaTest, ResetAllPools) {
  auto ref = arena_->store_string_auto(std::string("reset_me")).ValueOrDie();
  arena_->reset();
}

TEST_F(StringArenaTest, ClearAllPools) {
  auto ref = arena_->store_string_auto(std::string("clear_me")).ValueOrDie();
  arena_->clear();
}

// ============================================================================
// StringPoolRegistry
// ============================================================================

TEST(StringPoolRegistryTest, GetPoolUnregistered) {
  auto* pool = StringArenaRegistry::get_pool(9999);
  EXPECT_EQ(pool, nullptr);
}
