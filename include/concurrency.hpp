#ifndef CONCURRENCY_HPP
#define CONCURRENCY_HPP

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_set.h>

#include <iostream>
#include <set>
namespace tundradb {
#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <memory>
#include <set>

template <typename T>
struct ConcurrentSet {
  tbb::concurrent_hash_map<T, std::monostate> data_;

  // Cached snapshot and version tracking
  std::shared_ptr<std::set<T>> cached_snapshot_{
      std::make_shared<std::set<T>>()};
  std::atomic<int64_t> version_{0};
  std::atomic<int64_t> snapshot_version_{0};

  bool insert(T t) {
    typename tbb::concurrent_hash_map<T, std::monostate>::accessor acc;
    if (data_.insert(acc, t)) {
      version_.fetch_add(1, std::memory_order_release);
      return true;
    }
    return false;
  }

  bool contains(T t) {
    typename tbb::concurrent_hash_map<T, std::monostate>::const_accessor acc;
    return data_.find(acc, t);
  }

  bool remove(T t) {
    if (data_.erase(t)) {
      version_.fetch_add(1, std::memory_order_release);
      return true;
    }
    return false;
  }

  int64_t size() const {
    // Use cached size if available and up-to-date
    auto current_snapshot_version =
        snapshot_version_.load(std::memory_order_acquire);
    auto current_version = version_.load(std::memory_order_acquire);

    if (current_snapshot_version >= current_version) {
      return cached_snapshot_->size();
    }

    return data_.size();
  }

  std::shared_ptr<std::set<T>> get_all() {
    // Fast path: If cached snapshot is up-to-date, return it
    auto current_snapshot_version =
        snapshot_version_.load(std::memory_order_acquire);
    auto current_version = version_.load(std::memory_order_acquire);

    if (current_snapshot_version >= current_version) {
      return cached_snapshot_;
    }

    // Need to rebuild the cache
    auto new_snapshot = std::make_shared<std::set<T>>();

    // Collect all keys from the hash map
    for (auto it = data_.begin(); it != data_.end(); ++it) {
      new_snapshot->insert(it->first);
    }

    if (snapshot_version_.compare_exchange_strong(current_snapshot_version,
                                                  current_version)) {
      cached_snapshot_ = new_snapshot;
      return new_snapshot;
    }

    return cached_snapshot_;
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP
