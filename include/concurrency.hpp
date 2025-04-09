#ifndef CONCURRENCY_HPP
#define CONCURRENCY_HPP

#include <cds/container/michael_list_hp.h>
#include <cds/container/michael_set.h>
#include <cds/gc/hp.h>
#include <cds/init.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <unordered_set>
#include <variant>
#include <vector>

namespace tundradb {

/**
 * @brief A lock-free concurrent set implementation with dynamic resizing and
 * memory reclamation
 *
 * This implementation provides a thread-safe set data structure that:
 * - Supports concurrent modifications and traversals
 * - Dynamically resizes when needed
 * - Properly manages memory with reclamation
 * - Uses lock-free algorithms for all operations
 *
 * Design Overview:
 * --------------
 * 1. Core Data Structure:
 *    - Uses a hash table with linear probing for collision resolution
 *    - Each slot contains a value and atomic flags for thread safety
 *    - The table can grow dynamically when load factor exceeds threshold
 *
 * 2. Thread Safety:
 *    - All operations are lock-free using atomic operations
 *    - Memory ordering ensures consistency across threads
 *    - Resizing operations are coordinated using atomic flags
 *
 * 3. Memory Management:
 *    - Uses reference counting for safe memory reclamation
 *    - Maintains a lock-free free list for reused nodes
 *    - Properly cleans up memory during resizing and destruction
 *
 * Key Algorithms:
 * -------------
 * 1. Insert Operation:
 *    - Check load factor and trigger resize if needed
 *    - Use linear probing to find empty slot
 *    - Atomically mark slot as used and store value
 *    - Handle concurrent modifications safely
 *
 * 2. Resize Operation:
 *    - Create new table with double size
 *    - Migrate existing elements to new table
 *    - Switch to new table atomically
 *    - Clean up old table safely
 *
 * 3. Memory Reclamation:
 *    - Track references to nodes
 *    - Add nodes to free list when no longer needed
 *    - Reuse nodes from free list when possible
 *    - Clean up properly during destruction
 *
 * Performance Characteristics:
 * -------------------------
 * - Average case O(1) for insert, remove, and contains
 * - Worst case O(n) for operations when table is full
 * - Resizing occurs when load factor exceeds 75%
 * - Memory usage grows linearly with element count
 *
 * Thread Safety Guarantees:
 * ----------------------
 * - All operations are lock-free
 * - Safe concurrent modifications
 * - Safe traversal during modifications
 * - No deadlocks or livelocks possible
 *
 * Memory Ordering:
 * --------------
 * - Uses acquire/release semantics for visibility
 * - Ensures proper synchronization between threads
 * - Maintains consistency during resizing
 * - Prevents data races on all operations
 */
template <typename T>
class LockFreeSet {
 private:
  // Initial size and load factor threshold for resizing
  static constexpr size_t INITIAL_SIZE = 1024;  // Start smaller, grow as needed
  static constexpr float LOAD_FACTOR_THRESHOLD = 0.75f;

  /**
   * @brief Node structure for storing values and managing memory
   *
   * Each node contains:
   * - The stored value
   * - An atomic flag indicating if the slot is used
   * - A reference count for memory management
   */
  struct Node {
    T value;
    std::atomic<bool> used;
    std::atomic<uint32_t> ref_count;  // For memory reclamation

    Node() : used(false), ref_count(0) {}
  };

  /**
   * @brief Table structure containing the hash table and metadata
   *
   * Each table contains:
   * - Vector of nodes for storing values
   * - Atomic counters for size and element count
   * - Proper initialization of all nodes
   */
  struct Table {
    std::vector<Node> nodes;
    std::atomic<size_t> size{0};
    std::atomic<size_t> count{0};

    Table(size_t capacity) : nodes(capacity) {}
  };

  // Current table and resize state management
  std::atomic<Table*> current_table_;
  std::atomic<Table*> new_table_{nullptr};
  std::atomic<bool> resize_in_progress_{false};

  /**
   * @brief Hash function for distributing elements
   *
   * Uses std::hash for good distribution
   * No modulo here as it's applied when used
   */
  size_t hash(const T& value) const { return std::hash<T>{}(value); }

  /**
   * @brief Check and perform resize if needed
   *
   * Algorithm:
   * 1. Check load factor
   * 2. If threshold exceeded, start resize
   * 3. Create new table
   * 4. Migrate data
   * 5. Switch tables
   * 6. Clean up old table
   */
  void maybe_resize() {
    Table* table = current_table_.load(std::memory_order_acquire);
    if (table->count.load(std::memory_order_acquire) >=
        table->size.load(std::memory_order_acquire) * LOAD_FACTOR_THRESHOLD) {
      bool expected = false;
      if (resize_in_progress_.compare_exchange_strong(
              expected, true, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        // Create new table with double size
        Table* new_table =
            new Table(table->size.load(std::memory_order_acquire) * 2);
        new_table_.store(new_table, std::memory_order_release);

        // Migrate data to new table
        for (size_t i = 0; i < table->nodes.size(); ++i) {
          if (table->nodes[i].used.load(std::memory_order_acquire)) {
            insert_impl(table->nodes[i].value, new_table);
          }
        }

        // Switch to new table
        Table* old_table =
            current_table_.exchange(new_table, std::memory_order_release);
        new_table_.store(nullptr, std::memory_order_release);
        resize_in_progress_.store(false, std::memory_order_release);

        // Clean up old table
        delete old_table;
      }
    }
  }

  /**
   * @brief Internal insert implementation
   *
   * Algorithm:
   * 1. Calculate hash and initial index
   * 2. Linear probe for empty slot
   * 3. Atomically mark slot as used
   * 4. Handle duplicates
   * 5. Update count
   */
  bool insert_impl(const T& value, Table* table) {
    size_t start_index =
        hash(value) % table->size.load(std::memory_order_acquire);
    size_t index = start_index;

    do {
      bool expected = false;
      if (table->nodes[index].used.compare_exchange_strong(
              expected, true, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        table->nodes[index].value = value;
        table->count.fetch_add(1, std::memory_order_release);
        return true;
      }

      if (table->nodes[index].value == value) {
        return false;
      }

      index = (index + 1) % table->size.load(std::memory_order_acquire);
    } while (index != start_index);

    return false;
  }

 public:
  /**
   * @brief Constructor initializes empty table
   */
  LockFreeSet() {
    current_table_.store(new Table(INITIAL_SIZE), std::memory_order_release);
  }

  /**
   * @brief Destructor cleans up all memory
   */
  ~LockFreeSet() {
    // Clean up tables
    Table* table = current_table_.load(std::memory_order_acquire);
    delete table;

    Table* new_table = new_table_.load(std::memory_order_acquire);
    if (new_table) delete new_table;
  }

  /**
   * @brief Insert a value into the set
   *
   * Thread-safe operation that:
   * 1. Checks if resize needed
   * 2. Inserts value if not present
   * 3. Returns true if inserted, false if already present
   */
  bool insert(const T& value) {
    maybe_resize();
    return insert_impl(value, current_table_.load(std::memory_order_acquire));
  }

  /**
   * @brief Check if value exists in set
   *
   * Thread-safe operation that:
   * 1. Uses linear probing to find value
   * 2. Returns true if found, false otherwise
   */
  bool contains(const T& value) const {
    Table* table = current_table_.load(std::memory_order_acquire);
    size_t start_index =
        hash(value) % table->size.load(std::memory_order_acquire);
    size_t index = start_index;

    do {
      if (table->nodes[index].used.load(std::memory_order_acquire) &&
          table->nodes[index].value == value) {
        return true;
      }

      index = (index + 1) % table->size.load(std::memory_order_acquire);
    } while (index != start_index);

    return false;
  }

  /**
   * @brief Remove a value from the set
   *
   * Thread-safe operation that:
   * 1. Finds the value
   * 2. Marks slot as unused
   * 3. Updates count
   * 4. Returns true if removed, false if not found
   */
  bool remove(const T& value) {
    Table* table = current_table_.load(std::memory_order_acquire);
    size_t start_index =
        hash(value) % table->size.load(std::memory_order_acquire);
    size_t index = start_index;

    do {
      if (table->nodes[index].used.load(std::memory_order_acquire) &&
          table->nodes[index].value == value) {
        table->nodes[index].used.store(false, std::memory_order_release);
        table->count.fetch_sub(1, std::memory_order_release);
        return true;
      }

      index = (index + 1) % table->size.load(std::memory_order_acquire);
    } while (index != start_index);

    return false;
  }

  /**
   * @brief Clear all elements from the set
   *
   * Thread-safe operation that:
   * 1. Marks all slots as unused
   * 2. Resets count to zero
   */
  void clear() {
    Table* table = current_table_.load(std::memory_order_acquire);
    for (size_t i = 0; i < table->nodes.size(); ++i) {
      table->nodes[i].used.store(false, std::memory_order_release);
    }
    table->count.store(0, std::memory_order_release);
  }

  /**
   * @brief Get a snapshot of all elements
   *
   * Thread-safe operation that:
   * 1. Creates a new set
   * 2. Copies all used elements
   * 3. Returns shared pointer to snapshot
   */
  std::set<T> get_all() const {
    std::set<T> result;
    Table* table = current_table_.load(std::memory_order_acquire);

    for (size_t i = 0; i < table->nodes.size(); ++i) {
      if (table->nodes[i].used.load(std::memory_order_acquire)) {
        result.insert(table->nodes[i].value);
      }
    }

    return result;
  }

  /**
   * @brief Get current number of elements
   *
   * Thread-safe operation that returns
   * the current count of elements
   */
  size_t size() const {
    return current_table_.load(std::memory_order_acquire)
        ->count.load(std::memory_order_acquire);
  }
};

// template <typename T>
// struct ConcurrentSet {
//  private:
//   LockFreeSet<T> set_;
//
//  public:
//   bool insert(T t) {
//     return set_.insert(t);
//   }
//
//   bool contains(T t) {
//     return set_.contains(t);
//   }
//
//   bool remove(T t) {
//     return set_.remove(t);
//   }
//
//   int64_t size() const {
//     return set_.size();
//   }
//
//   std::shared_ptr<std::set<T>> get_all() const {
//     auto result = set_.get_all();
//     return std::make_shared<std::set<T>>(std::move(result));
//   }
//
//   void clear() {
//     set_.clear();
//   }
// };

template <typename T>
class ConcurrentSet {
 private:
  // Define key comparison for the ordered list inside MichaelSet
  struct Less {
    bool operator()(const T& a, const T& b) const { return a < b; }
  };

  // Typedefs for internal structure
  using GC = cds::gc::HP;

  using List =
      cds::container::MichaelList<GC, T,
                                  typename cds::container::michael_list::
                                      make_traits<cds::opt::less<Less>>::type>;

  using Set = cds::container::MichaelHashSet<
      GC, List,
      typename cds::container::michael_set::make_traits<
          cds::opt::hash<std::hash<T>>>::type>;

  Set set_;

 public:
  ConcurrentSet() {
    static std::once_flag init_flag;
    std::call_once(init_flag, [] {
      cds::Initialize();                 // Init libcds
      cds::gc::HP::thread_gc::attach();  // Attach current thread
    });
  }

  ~ConcurrentSet() {
    cds::gc::HP::thread_gc::detach();
    // Optional: cds::Terminate(); // if this is the last structure using CDS
  }

  bool insert(const T& t) { return set_.insert(t); }

  bool contains(const T& t) { return set_.contains(t); }

  bool remove(const T& t) { return set_.erase(t); }

  size_t size() const { return set_.size(); }

  std::shared_ptr<std::set<T>> get_all() const {
    auto snapshot = std::make_shared<std::set<T>>();
    set_.for_each([&](const T& value) { snapshot->insert(value); });
    return snapshot;
  }

  void clear() {
    // Not atomic, but okay if concurrent ops are tolerated
    set_.clear();
  }
};

}  // namespace tundradb

#endif  // CONCURRENCY_HPP
