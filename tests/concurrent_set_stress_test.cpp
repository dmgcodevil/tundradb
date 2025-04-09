#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_set.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "concurrency.hpp"

using namespace std::string_literals;
using namespace tundradb;

// Test parameters
constexpr int NUM_THREADS = 128;
constexpr int OPERATIONS_PER_THREAD = 2000;
constexpr double INSERT_PROBABILITY = 0.6;  // 60% inserts, 40% removes

class SimplifiedTest {
 private:
  tundradb::ConcurrentSet<int64_t> concurrent_set_;
  std::atomic<int64_t> next_id_{0};
  std::atomic<size_t> total_operations_{0};
  std::atomic<size_t> successful_inserts_{0};
  std::atomic<size_t> successful_removes_{0};
  tbb::concurrent_set<std::string> errors;

  // Keep track of which elements should be in the final set
  tbb::concurrent_hash_map<int64_t, bool> expected_elements_;

  std::string join(const std::vector<std::string>& strings,
                   const std::string& delimiter) {
    if (strings.empty()) {
      return "";
    }
    return std::accumulate(
        std::next(strings.begin()), strings.end(), strings[0],
        [&](std::string a, std::string b) { return a + delimiter + b; });
  }

 public:
  void run_test() {
    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> worker_threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
      worker_threads.emplace_back(&SimplifiedTest::worker, this, i);
    }

    // Start progress monitor thread
    std::atomic<bool> monitor_running{true};
    std::thread monitor_thread(&SimplifiedTest::monitor, this,
                               std::ref(monitor_running));

    // Wait for all worker threads to complete
    for (auto& t : worker_threads) {
      t.join();
    }

    // Stop monitor thread
    monitor_running = false;
    monitor_thread.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    validate_final_state();
    print_stats(duration.count());
  }

 private:
  void worker(int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd() +
                     thread_id);  // Use thread_id to ensure different seeds
    std::uniform_real_distribution<> op_dist(0.0, 1.0);

    // Queue of elements this thread has inserted
    tbb::concurrent_queue<int64_t> inserted_elements;
    for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
      bool do_insert = op_dist(gen) < INSERT_PROBABILITY;

      if (do_insert) {
        // Insert operation
        int64_t id = next_id_.fetch_add(1);
        bool success = concurrent_set_.insert(id);

        if (success) {
          if (!concurrent_set_.contains(id)) {
            std::cout << "doesn't have an insertion success" << std::endl;
          }
          successful_inserts_.fetch_add(1);
          inserted_elements.push(id);

          // Mark element as expected in final set
          typename tbb::concurrent_hash_map<int64_t, bool>::accessor acc;
          if (expected_elements_.insert(acc, id)) {
            acc->second = true;
          }

        } else {
          errors.insert("Failed to insert id: " + std::to_string(id));
        }
      } else {
        int64_t id;
        if (inserted_elements.try_pop(id)) {
          bool success = concurrent_set_.remove(id);
          if (success) {
            successful_removes_.fetch_add(1);
            // Mark element as not expected in final set
            typename tbb::concurrent_hash_map<int64_t, bool>::accessor acc;
            if (expected_elements_.find(acc, id)) {
              acc->second = false;
            }
          } else {
            errors.insert("Failed to remove id: " + std::to_string(id));
          }
        }
      }

      total_operations_.fetch_add(1);

      // Occasionally do a read operation
      if (i % 100 == 0) {
        // Just get all elements as a simple read test
        auto snapshot = concurrent_set_.get_all();
        volatile size_t size = snapshot->size();  // Prevent optimization
      }
    }
  }

  void monitor(std::atomic<bool>& running) {
    size_t last_ops = 0;

    while (running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      auto current_ops = total_operations_.load();
      auto ops_per_second = current_ops - last_ops;
      last_ops = current_ops;

      std::cout << "Progress: " << current_ops << "/"
                << (NUM_THREADS * OPERATIONS_PER_THREAD) << " operations ("
                << (current_ops * 100.0 / (NUM_THREADS * OPERATIONS_PER_THREAD))
                << "%), " << ops_per_second << " ops/sec, "
                << "Current size: " << concurrent_set_.size() << std::endl;
    }
  }

  void validate_final_state() {
    std::cout << "\nValidating final state..." << std::endl;

    // Get final snapshot
    auto final_snapshot = concurrent_set_.get_all();
    std::cout << "Final set size (from size()): " << concurrent_set_.size()
              << std::endl;
    std::cout << "Final set size (from snapshot): " << final_snapshot->size()
              << std::endl;

    // Build expected set
    std::unordered_set<int64_t> expected_set;
    auto range = expected_elements_.range();

    for (auto it = range.begin(); it != range.end(); ++it) {
      if (it->second) {  // Only include elements marked as true (should be
                         // present)
        expected_set.insert(it->first);
      }
    }

    std::cout << "Expected elements count: " << expected_set.size()
              << std::endl;

    // Compare sizes
    if (final_snapshot->size() != expected_set.size()) {
      std::cout
          << "FAILED: Size mismatch between expected set and final snapshot"
          << std::endl;
    } else {
      std::cout << "PASSED: Sizes match between expected set and final snapshot"
                << std::endl;
    }
    if (!errors.empty()) {
      std::cout << "ERRORS: "
                << join(std::vector<std::string>(errors.begin(), errors.end()),
                        ", ")
                << std::endl;
    }

    // Check each element
    bool all_match = true;

    // Check that all expected elements are in the snapshot
    for (const auto& item : expected_set) {
      if (final_snapshot->find(item) == final_snapshot->end()) {
        all_match = false;
        std::cout << "ERROR: Expected element " << item
                  << " missing from snapshot" << std::endl;
      }
    }

    // Check that all snapshot elements are expected
    for (const auto& item : *final_snapshot) {
      if (expected_set.find(item) == expected_set.end()) {
        all_match = false;
        std::cout << "ERROR: Unexpected element " << item
                  << " found in snapshot" << std::endl;
      }
    }

    if (all_match) {
      std::cout << "PASSED: All elements match between expected set and final "
                   "snapshot"
                << std::endl;
    } else {
      std::cout
          << "FAILED: Element mismatch between expected set and final snapshot"
          << std::endl;
    }

    // Verify consistency with contains() method
    bool contains_consistent = true;
    for (const auto& item : expected_set) {
      if (!concurrent_set_.contains(item)) {
        contains_consistent = false;
        std::cout << "ERROR: Element " << item
                  << " should be in the set according to contains()"
                  << std::endl;
      }
    }

    // Also check non-existence of removed elements
    int num_checked = 0;
    for (int64_t i = 0; i < next_id_.load() && num_checked < 1000; i++) {
      if (expected_set.find(i) == expected_set.end()) {
        // This is an element that should NOT be in the set
        if (concurrent_set_.contains(i)) {
          contains_consistent = false;
          std::cout << "ERROR: Element " << i
                    << " should NOT be in the set according to contains()"
                    << std::endl;
        }
        num_checked++;
      }
    }

    if (contains_consistent) {
      std::cout << "PASSED: contains() method is consistent with expected set"
                << std::endl;
    } else {
      std::cout << "FAILED: contains() method is inconsistent with expected set"
                << std::endl;
    }
  }

  void print_stats(int64_t duration_ms) {
    auto final_inserts = successful_inserts_.load();
    auto final_removes = successful_removes_.load();
    auto final_ops = total_operations_.load();

    std::cout << "\n=== Test Statistics ===" << std::endl;
    std::cout << "Test duration: " << duration_ms << "ms" << std::endl;
    std::cout << "Total operations: " << final_ops << std::endl;
    std::cout << "Successful inserts: " << final_inserts << std::endl;
    std::cout << "Successful removes: " << final_removes << std::endl;
    std::cout << "Operations per second: " << (final_ops * 1000.0 / duration_ms)
              << std::endl;

    // Theoretical final size should be inserts - removes
    std::cout << "Theoretical size (inserts - removes): "
              << (final_inserts - final_removes) << std::endl;
    std::cout << "Actual final size: " << concurrent_set_.size() << std::endl;
  }
};

int main(int argc, char** argv) {
  std::cout << "C++ version: " << __cplusplus << std::endl;
  std::cout << "RCUConcurrentSet Simplified Test" << std::endl;
  std::cout << "===============================" << std::endl;
  std::cout << "Threads: " << NUM_THREADS << std::endl;
  std::cout << "Operations per thread: " << OPERATIONS_PER_THREAD << std::endl;
  std::cout << "Insert probability: " << (INSERT_PROBABILITY * 100) << "%"
            << std::endl;

  SimplifiedTest test;
  test.run_test();
}
