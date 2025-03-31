#ifndef CONCURRENCY_HPP
#define CONCURRENCY_HPP

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_set.h>

#include <iostream>
#include <set>
namespace tundradb {

template <typename T>
struct RCUConcurrentSet {
  tbb::concurrent_hash_map<T, std::monostate> data_;
  bool insert(T t) {
    typename tbb::concurrent_hash_map<T, std::monostate>::accessor acc;
    if (data_.insert(acc, t)) {
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
      return true;
    }
    return false;
  }

  int64_t size() const { return data_.size(); }

  std::set<T> get_all() {
    std::set<T> key_set;
    for (auto it = data_.begin(); it != data_.end(); ++it) {
      key_set.insert(it->first);
    }
    return key_set;
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP
