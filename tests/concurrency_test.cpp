#include "concurrency.hpp"

#include <gtest/gtest.h>
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

// Helper class to measure memory usage
class MemoryUsageTracker {
 private:
  struct rusage usage_start_;
  struct rusage usage_peak_;
  std::mutex mutex_;
  std::atomic<bool> tracking_{false};
  std::thread tracker_thread_;

  void tracker_loop() {
    while (tracking_.load()) {
      struct rusage usage_current;
      getrusage(RUSAGE_SELF, &usage_current);

      {
        std::lock_guard<std::mutex> lock(mutex_);
        // Update peak memory if current is higher
        if (usage_current.ru_maxrss > usage_peak_.ru_maxrss) {
          usage_peak_ = usage_current;
        }
      }

      // Sleep for a short period to avoid excessive CPU usage
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

 public:
  void start() {
    std::lock_guard<std::mutex> lock(mutex_);
    getrusage(RUSAGE_SELF, &usage_start_);
    usage_peak_ = usage_start_;
    tracking_ = true;
    tracker_thread_ = std::thread(&MemoryUsageTracker::tracker_loop, this);
  }

  void stop() {
    tracking_ = false;
    if (tracker_thread_.joinable()) {
      tracker_thread_.join();
    }
  }

  size_t get_peak_memory_kb() {
    std::lock_guard<std::mutex> lock(mutex_);
    return usage_peak_.ru_maxrss;
  }

  size_t get_memory_increase_kb() {
    std::lock_guard<std::mutex> lock(mutex_);
    return usage_peak_.ru_maxrss - usage_start_.ru_maxrss;
  }
};

// Test fixture for concurrent set tests
class RCUConcurrentSetTest : public ::testing::Test {
 protected:
  MemoryUsageTracker memory_tracker_;

  void SetUp() override { memory_tracker_.start(); }

  void TearDown() override {
    memory_tracker_.stop();
    std::cout << "Peak memory usage: " << memory_tracker_.get_peak_memory_kb()
              << " KB" << std::endl;
    std::cout << "Memory increase: " << memory_tracker_.get_memory_increase_kb()
              << " KB" << std::endl;
  }

  // Helper function to run worker threads with different operations
  template <typename T>
  void run_concurrent_test(tundradb::RCUConcurrentSet<T>& set,
                           int num_insert_threads, int num_delete_threads,
                           int num_read_threads, int num_elements,
                           double delete_ratio = 1.0) {
    // Create shared data structures
    std::vector<T> all_values(num_elements);
    std::iota(all_values.begin(), all_values.end(), 1);  // Fill with 1...N

    std::atomic<int> next_value_idx{0};
    std::vector<T> values_to_delete;
    std::mutex delete_mutex;
    std::atomic<bool> keep_reading{true};

    // Lambda for insert workers
    auto insert_worker = [&](int worker_id) {
      int ops = 0;
      std::mt19937 rng(worker_id + 1000);  // Create a seeded random generator
      std::uniform_real_distribution<double> dist(0.0,
                                                  1.0);  // Create distribution

      // Create a local buffer to minimize lock contention
      std::vector<T> local_values_to_delete;

      while (true) {
        int idx = next_value_idx.fetch_add(1);
        if (idx >= num_elements) break;

        T value = all_values[idx];
        if (set.insert(value)) {
          ops++;
          // Add some values to delete list based on delete_ratio
          if (dist(rng) < delete_ratio) {
            // Instead of locking for each value, add to local buffer
            local_values_to_delete.push_back(value);

            // Periodically flush the local buffer to the shared vector
            if (local_values_to_delete.size() >= 100) {
              std::lock_guard<std::mutex> lock(delete_mutex);
              values_to_delete.insert(values_to_delete.end(),
                                      local_values_to_delete.begin(),
                                      local_values_to_delete.end());
              local_values_to_delete.clear();
            }
          }
        }
      }

      // Don't forget to flush any remaining values
      if (!local_values_to_delete.empty()) {
        std::lock_guard<std::mutex> lock(delete_mutex);
        values_to_delete.insert(values_to_delete.end(),
                                local_values_to_delete.begin(),
                                local_values_to_delete.end());
      }

      return ops;
    };

    // Lambda for delete workers
    auto delete_worker = [&](int worker_id) {
      int ops = 0;
      std::mt19937 rng(worker_id);

      while (true) {
        // Check if we should continue running, with mutex protection
        bool should_continue;
        T value_to_delete;
        bool has_value = false;

        {
          std::lock_guard<std::mutex> lock(delete_mutex);
          // Check if we should continue (either reading is active or we have
          // values to delete)
          should_continue = keep_reading.load(std::memory_order_acquire) ||
                            !values_to_delete.empty();
          if (!should_continue) {
            break;  // Exit the loop if we're done
          }

          // Try to get a value to delete
          if (!values_to_delete.empty()) {
            // Pick a random value to delete
            if (values_to_delete.size() > 1) {
              std::uniform_int_distribution<size_t> idx_dist(
                  0, values_to_delete.size() - 1);
              size_t idx = idx_dist(rng);
              std::swap(values_to_delete[idx], values_to_delete.back());
            }
            value_to_delete = values_to_delete.back();
            values_to_delete.pop_back();
            has_value = true;
          }
        }

        // If we didn't get a value but should continue, yield and try again
        if (!has_value) {
          std::this_thread::yield();
          continue;
        }

        // Process the value (outside the lock for better concurrency)
        if (set.remove(value_to_delete)) {
          ops++;
        }
      }
      return ops;
    };

    // Lambda for read workers
    auto read_worker = [&](int worker_id) {
      int ops = 0;
      while (keep_reading.load(std::memory_order_acquire)) {
        auto snapshot = set.get_all();
        // Do something with the snapshot to ensure it's not optimized away
        if (snapshot->size() > 0) {
          auto it = snapshot->begin();
          volatile T first_value = *it;
          (void)first_value;  // Prevent unused variable warning
        }
        ops++;
      }
      return ops;
    };

    // Launch threads
    std::vector<std::future<int>> insert_futures;
    for (int i = 0; i < num_insert_threads; i++) {
      insert_futures.push_back(
          std::async(std::launch::async, insert_worker, i));
    }

    std::vector<std::future<int>> read_futures;
    for (int i = 0; i < num_read_threads; i++) {
      read_futures.push_back(std::async(std::launch::async, read_worker, i));
    }

    std::vector<std::future<int>> delete_futures;
    for (int i = 0; i < num_delete_threads; i++) {
      delete_futures.push_back(
          std::async(std::launch::async, delete_worker, i));
    }

    // Wait for all inserts to complete
    int total_inserts = 0;
    for (auto& f : insert_futures) {
      total_inserts += f.get();
    }

    // Signal read threads to stop
    keep_reading.store(false, std::memory_order_release);

    // Wait for delete threads to finish
    int total_deletes = 0;
    for (auto& f : delete_futures) {
      total_deletes += f.get();
    }

    // Wait for read threads
    int total_reads = 0;
    for (auto& f : read_futures) {
      total_reads += f.get();
    }

    // Print statistics
    std::cout << "Total inserts: " << total_inserts << std::endl;
    std::cout << "Total deletes: " << total_deletes << std::endl;
    std::cout << "Total reads: " << total_reads << std::endl;
  }
};

// Test with different thread configurations
struct ThreadConfig {
  int insert_threads;
  int delete_threads;
  int read_threads;
  int elements;
  double delete_ratio;
  const char* name;
};

// Define test configurations
class RCUConcurrentSetParamTest
    : public RCUConcurrentSetTest,
      public ::testing::WithParamInterface<ThreadConfig> {};

TEST_P(RCUConcurrentSetParamTest, ConcurrentOperations) {
  // Get test parameters
  const ThreadConfig& config = GetParam();

  std::cout << "\n=== Running test with " << config.name << " ===" << std::endl;
  std::cout << "Insert threads: " << config.insert_threads
            << ", Delete threads: " << config.delete_threads
            << ", Read threads: " << config.read_threads
            << ", Elements: " << config.elements
            << ", Delete ratio: " << config.delete_ratio << std::endl;

  // Create the set and run the test
  tundradb::RCUConcurrentSet<int64_t> set;

  run_concurrent_test(set, config.insert_threads, config.delete_threads,
                      config.read_threads, config.elements,
                      config.delete_ratio);

  // Verify the final state
  auto final_snapshot = set.get_all();
  size_t expected_size = config.elements * (1.0 - config.delete_ratio);

  std::cout << "Final snapshot size: " << final_snapshot->size() << std::endl;
  std::cout << "Expected size: " << expected_size << std::endl;
  std::cout << "Actual set size: " << set.size() << std::endl;

  EXPECT_EQ(final_snapshot->size(), set.size())
      << "Snapshot size and set size should be equal";

  EXPECT_NEAR(final_snapshot->size(), expected_size, expected_size * 0.1)
      << "Final size should be approximately " << expected_size;
}

// Define test configurations
INSTANTIATE_TEST_SUITE_P(
    VariousThreadConfigs, RCUConcurrentSetParamTest,
    ::testing::Values(
        // All elements deleted
        ThreadConfig{2, 2, 4, 1000, 1.0, "All Elements Deleted"},

        // Half elements deleted
        ThreadConfig{2, 2, 4, 1000, 0.5, "Half Elements Deleted"},

        // No elements deleted
        ThreadConfig{2, 2, 4, 1000, 0.0, "No Elements Deleted"},

        // High concurrency test
        ThreadConfig{4, 4, 8, 10000, 0.5, "High Concurrency"},

        // Read-heavy workload
        ThreadConfig{1, 1, 10, 1000, 0.5, "Read Heavy"},

        // Write-heavy workload
        ThreadConfig{6, 4, 2, 1000, 0.7, "Write Heavy"}),
    [](const ::testing::TestParamInfo<ThreadConfig>& info) {
      // Generate test names without spaces and special characters
      std::string name = info.param.name;
      std::replace(name.begin(), name.end(), ' ', '_');
      return name;
    });

// Standalone test for memory leak detection
TEST_F(RCUConcurrentSetTest, MemoryLeakTest) {
  const int NUM_ITERATIONS = 10;
  const int NUM_ELEMENTS = 10000;

  // Create and destroy snapshots multiple times to check for leaks
  for (int i = 0; i < NUM_ITERATIONS; i++) {
    tundradb::RCUConcurrentSet<int64_t> set;

    // Insert elements
    for (int64_t j = 0; j < NUM_ELEMENTS; j++) {
      set.insert(j);
    }

    // Get multiple snapshots
    for (int j = 0; j < 5; j++) {
      auto snapshot = set.get_all();
      EXPECT_EQ(snapshot->size(), NUM_ELEMENTS);
    }

    // Delete some elements
    for (int64_t j = 0; j < NUM_ELEMENTS / 2; j++) {
      set.remove(j);
    }

    // Get more snapshots
    for (int j = 0; j < 5; j++) {
      auto snapshot = set.get_all();
      EXPECT_EQ(snapshot->size(), NUM_ELEMENTS / 2);
    }

    // Insert different elements
    for (int64_t j = NUM_ELEMENTS; j < NUM_ELEMENTS * 2; j++) {
      set.insert(j);
    }

    // Final snapshot
    auto final_snapshot = set.get_all();
    EXPECT_EQ(final_snapshot->size(), NUM_ELEMENTS + NUM_ELEMENTS / 2);

    std::cout << "Iteration " << i << " completed" << std::endl;
  }

  // Memory usage will be reported in TearDown()
}

// Test edge cases
TEST_F(RCUConcurrentSetTest, EmptySetTest) {
  tundradb::RCUConcurrentSet<int> set;
  auto snapshot = set.get_all();

  EXPECT_EQ(snapshot->size(), 0);
  EXPECT_EQ(set.size(), 0);
}

TEST_F(RCUConcurrentSetTest, InsertDeleteSameElementRepeatedly) {
  tundradb::RCUConcurrentSet<int> set;
  const int VALUE = 42;
  const int ITERATIONS = 1000;

  for (int i = 0; i < ITERATIONS; i++) {
    EXPECT_TRUE(set.insert(VALUE));
    EXPECT_TRUE(set.remove(VALUE));
  }

  auto snapshot = set.get_all();
  EXPECT_EQ(snapshot->size(), 0);
  EXPECT_EQ(set.size(), 0);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}