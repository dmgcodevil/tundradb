#ifndef CONCURRENCY_HPP
#define CONCURRENCY_HPP

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_set.h>

#include <set>

namespace tundradb {

template <typename T>
struct RCUConcurrentSet {
  tbb::concurrent_hash_map<T, bool> data_;  // false means element is deleted
  std::atomic<size_t> remove_counter;
  std::shared_ptr<std::set<T>> snapshot_;
  std::atomic<bool> generate_snapshot_;  // snapshot generation in progress
  std::atomic<int64_t> version_;
  std::atomic<int64_t> snapshot_version_;
  std::vector<std::shared_ptr<std::set<T>>> old_snapshots;

  RCUConcurrentSet()
      : remove_counter(0),
        snapshot_(std::make_shared<std::set<T>>()),
        generate_snapshot_(false),
        version_(0),
        snapshot_version_(0) {}

  bool insert(T t) {
    typename tbb::concurrent_hash_map<T, bool>::accessor acc;
    if (data_.insert(acc, t)) {
      acc->second = true;
      version_.fetch_add(1, std::memory_order_release);
      return true;
    }
    bool prev = acc->second;
    acc->second = true;
    if (!prev) {
      version_.fetch_add(1, std::memory_order_release);
    }
    return !prev;
  }

  bool remove(T t) {
    typename tbb::concurrent_hash_map<T, bool>::accessor acc;
    if (data_.find(acc, t) && acc->second) {
      remove_counter.fetch_add(1, std::memory_order_release);
      acc->second = false;
      version_.fetch_add(1, std::memory_order_release);
      return true;
    }
    return false;
  }

  void unsafe_delete_(T t) {
    typename tbb::concurrent_hash_map<T, bool>::accessor acc;
    if (data_.find(acc, t)) {
      data_.erase(acc);
    }
  }

  bool contains(T t) {
    typename tbb::concurrent_hash_map<T, bool>::const_accessor acc;
    return data_.find(acc, t) && acc->second;
  }

  // Clean up old snapshots that are no longer in use
  void cleanup_old_snapshots() {
    // Remove snapshots that only we reference (use_count == 1)
    auto it = std::remove_if(old_snapshots.begin(), old_snapshots.end(),
                             [](const std::shared_ptr<std::set<T>>& ptr) {
                               if (ptr.use_count() == 1) {
                                 // Clear the contents to free memory
                                 // immediately
                                 ptr->clear();
                                 return true;  // Remove from vector
                               }
                               return false;  // Keep in vector
                             });

    old_snapshots.erase(it, old_snapshots.end());
  }

  // eventually consistent
  int64_t size() const {
    auto current_snapshot_version =
        snapshot_version_.load(std::memory_order_acquire);
    auto current_version = version_.load(std::memory_order_acquire);

    if (current_snapshot_version >= current_version) {
      return snapshot_->size();
    }
    auto total = data_.size();
    auto removed = remove_counter.load(std::memory_order_acquire);
    return (total > removed) ? (total - removed) : 0;
  }

  std::shared_ptr<std::set<T>> get_all() {
    auto current_snapshot_version =
        snapshot_version_.load(std::memory_order_acquire);
    auto current_version = version_.load(std::memory_order_acquire);

    if (current_snapshot_version >= current_version) {
      return snapshot_;
    }

    bool expected = false;
    if (generate_snapshot_.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
      try {
        current_snapshot_version =
            snapshot_version_.load(std::memory_order_acquire);
        current_version = version_.load(std::memory_order_acquire);

        if (current_snapshot_version < current_version) {
          std::set<T> selected;
          std::vector<T> to_delete;
          typename tbb::concurrent_hash_map<T, bool>::range_type range =
              data_.range();
          for (auto it = range.begin(); it != range.end(); ++it) {
            if (it->second) {
              selected.insert(it->first);
            } else {
              to_delete.push_back(it->first);
            }
          }

          size_t deleted_count = 0;
          for (const auto& key : to_delete) {
            if (data_.erase(key)) {
              deleted_count++;
            }
          }

          if (deleted_count > 0) {
            remove_counter.fetch_sub(deleted_count, std::memory_order_release);
          }

          old_snapshots.push_back(snapshot_);

          auto new_snapshot =
              std::make_shared<std::set<T>>(std::move(selected));
          snapshot_ = new_snapshot;
          snapshot_version_.store(current_version, std::memory_order_release);
          cleanup_old_snapshots();
        }
      } catch (...) {
        generate_snapshot_.store(false, std::memory_order_release);
        throw;
      }

      generate_snapshot_.store(false, std::memory_order_release);
    }

    return snapshot_;
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP
