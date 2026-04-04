#ifndef CONCURRENCY_HPP
#define CONCURRENCY_HPP

#include <tbb/concurrent_hash_map.h>

#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <variant>

namespace tundradb {

/**
 * @brief A thread-safe concurrent set implementation using TBB
 *
 * This implementation provides a thread-safe set data structure that:
 * - Supports concurrent modifications and traversals
 * - Uses TBB's concurrent_hash_map for high performance
 * - Provides a simple interface for common operations
 */
template <typename T>
class ConcurrentSet {
 public:
  /**
   * @brief Unsafe view for direct iteration over the underlying
   * concurrent_hash_map
   *
   * WARNING: This view is NOT thread-safe. Use only when you can guarantee
   * no concurrent modifications are happening.
   */
  class LockedView {
   private:
    const tbb::concurrent_hash_map<T, std::monostate>& data_;

   public:
    explicit LockedView(const tbb::concurrent_hash_map<T, std::monostate>& data)
        : data_(data) {}

    // Iterator that extracts keys from the concurrent_hash_map
    class iterator {
     private:
      typename tbb::concurrent_hash_map<T, std::monostate>::const_iterator it_;

     public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = T;
      using difference_type = std::ptrdiff_t;
      using pointer = const T*;
      using reference = const T&;

      explicit iterator(
          typename tbb::concurrent_hash_map<T, std::monostate>::const_iterator
              it)
          : it_(it) {}

      reference operator*() const { return it_->first; }
      pointer operator->() const { return &(it_->first); }

      iterator& operator++() {
        ++it_;
        return *this;
      }
      iterator operator++(int) {
        iterator tmp = *this;
        ++(*this);
        return tmp;
      }

      bool operator==(const iterator& other) const { return it_ == other.it_; }
      bool operator!=(const iterator& other) const { return it_ != other.it_; }
    };

    iterator begin() const { return iterator(data_.begin()); }
    iterator end() const { return iterator(data_.end()); }
    size_t size() const { return data_.size(); }
  };

 private:
  tbb::concurrent_hash_map<T, std::monostate> data_;
  mutable std::shared_mutex mutex_;  // Read-write mutex for synchronization

 public:
  ConcurrentSet() = default;
  ~ConcurrentSet() = default;

  /**
   * @brief Insert a value into the set
   *
   * Thread-safe operation that:
   * 1. Inserts value if not present
   * 2. Returns true if inserted, false if already present
   */
  bool insert(const T& t) {
    typename tbb::concurrent_hash_map<T, std::monostate>::accessor acc;
    return data_.insert(acc, t);
  }

  /**
   * @brief Check if value exists in set
   *
   * Thread-safe operation that:
   * 1. Returns true if found, false otherwise
   */
  bool contains(const T& t) const {
    typename tbb::concurrent_hash_map<T, std::monostate>::const_accessor acc;
    return data_.find(acc, t);
  }

  /**
   * @brief Remove a value from the set
   *
   * Thread-safe operation that:
   * 1. Acquires write lock
   * 2. Removes value if present
   * 3. Returns true if removed, false if not found
   */
  bool remove(const T& t) {
    std::unique_lock<std::shared_mutex> lock(mutex_);  // Write lock
    return data_.erase(t);
  }

  /**
   * @brief Get current number of elements
   *
   * Thread-safe operation that returns
   * the current count of elements
   */
  size_t size() const { return data_.size(); }

  /**
   * @brief Get a snapshot of all elements
   *
   * Thread-safe operation that:
   * 1. Acquires read lock
   * 2. Creates a new set
   * 3. Copies all elements under the read lock
   * 4. Returns shared pointer to snapshot
   */
  std::shared_ptr<std::set<T>> get_all() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);  // Read lock
    auto snapshot = std::make_shared<std::set<T>>();

    // Iterate over the map while holding the read lock
    for (auto it = data_.begin(); it != data_.end(); ++it) {
      snapshot->insert(it->first);
    }

    return snapshot;
  }

  /**
   * @brief Get an unsafe view for direct iteration
   *
   * WARNING: This is NOT thread-safe! Use only when you can guarantee
   * no concurrent modifications are happening. This avoids the expensive
   * copy operation of get_all() for performance-critical code paths.
   *
   * Returns a view that provides begin()/end() iterators directly over
   * the underlying concurrent_hash_map keys.
   */
  LockedView get_all_unsafe() const { return LockedView(data_); }

  /**
   * @brief Clear all elements from the set
   *
   * Thread-safe operation that:
   * 1. Acquires write lock
   * 2. Removes all elements
   */
  void clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);  // Write lock
    data_.clear();
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP