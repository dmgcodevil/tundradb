#ifndef CONCURRENCY_HPP
#define CONCURRENCY_HPP

#include <tbb/concurrent_hash_map.h>

#include <iostream>
#include <set>
#include <shared_mutex>
#include <mutex>
#include <variant>

namespace tundradb {

template <typename T>
struct ConcurrentSet {
 private:
  mutable tbb::concurrent_hash_map<T, std::monostate> data_;
  mutable std::shared_mutex rw_mutex_;

 public:
  bool insert(T t) {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    typename tbb::concurrent_hash_map<T, std::monostate>::accessor acc;
    if (data_.insert(acc, t)) {
      return true;
    }
    return false;
  }

  bool contains(T t) {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    typename tbb::concurrent_hash_map<T, std::monostate>::const_accessor acc;
    return data_.find(acc, t);
  }

  bool remove(T t) {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    if (data_.erase(t)) {
      return true;
    }
    return false;
  }

  int64_t size() const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    // Simply return the size of the underlying data structure
    return data_.size();
  }

  std::shared_ptr<std::set<T>> get_all() const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    // Create a new snapshot
    auto new_snapshot = std::make_shared<std::set<T>>();

    // Collect all keys from the hash map
    for (auto it = data_.begin(); it != data_.end(); ++it) {
      new_snapshot->insert(it->first);
    }

    // Return the new snapshot directly without updating the cached one
    // This avoids race conditions and use-after-free issues
    return new_snapshot;
  }

  void clear() {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    data_.clear();
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP
