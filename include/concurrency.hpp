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
      version_.fetch_add(1);
      return true;
    }
    bool prev = acc->second;
    acc->second = true;
    if (!prev) {
      version_.fetch_add(1);
    }
    return !prev;
  }

  bool remove(T t) {
    typename tbb::concurrent_hash_map<T, bool>::accessor acc;
    if (data_.find(acc, t) && acc->second) {
      remove_counter.fetch_add(1);
      acc->second = false;
      version_.fetch_add(1);
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

  // eventually consistent
  int64_t size() const {
    if (snapshot_version_.load() >= version_.load()) {
      return snapshot_->size();
    }
    auto total = data_.size();
    auto removed = remove_counter.load();
    return (total > removed) ? (total - removed) : 0;
  }

  std::shared_ptr<std::set<T>> get_all() {
    if (snapshot_version_.load() >= version_.load()) {
      return snapshot_;
    }
    bool expected = false;
    if (generate_snapshot_.compare_exchange_strong(expected, true)) {
      try {
        if (snapshot_version_.load() < version_.load()) {
          int64_t current_version = version_.load();
          std::set<T> selected;
          std::set<T> deleted;
          for (const auto& item : data_) {
            if (item.second) {
              selected.insert(item.first);
            } else {
              deleted.insert(item.first);
            }
          }
          for (const auto& item : deleted) {
            remove_counter.fetch_sub(1);
            unsafe_delete_(item);
          }
          snapshot_version_.store(current_version);
          snapshot_ = make_shared<std::set<T>>(selected);
        }
      } catch (...) {
        generate_snapshot_.store(false);
        throw;
      }
      generate_snapshot_.store(false);
    }

    return snapshot_;
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP
