#ifndef EDGE_STORE_HPP
#define EDGE_STORE_HPP

#include <arrow/api.h>

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "concurrency.hpp"
#include "edge.hpp"
#include "utils.hpp"

namespace tundradb {

// Forward declaration for EdgeView
class EdgeStore;

/**
 * @brief A view over edges that avoids copying shared_ptr<Edge> objects
 *
 * This class provides iteration over edges without materializing them into
 * a vector, reducing memory allocations and improving performance.
 */
class EdgeView {
 public:
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::shared_ptr<Edge>;
    using difference_type = std::ptrdiff_t;
    using pointer = const value_type *;
    using reference = const value_type &;

    iterator(const EdgeStore *store,
             ConcurrentSet<int64_t>::LockedView::iterator edge_ids_it,
             ConcurrentSet<int64_t>::LockedView::iterator edge_ids_end,
             const std::string &type_filter)
        : store_(store),
          edge_ids_it_(edge_ids_it),
          edge_ids_end_(edge_ids_end),
          type_filter_(type_filter) {
      advance_to_valid();
    }

    iterator &operator++() {
      ++edge_ids_it_;
      advance_to_valid();
      return *this;
    }

    iterator operator++(int) {
      iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    reference operator*() const { return current_edge_; }
    pointer operator->() const { return &current_edge_; }

    bool operator==(const iterator &other) const {
      return edge_ids_it_ == other.edge_ids_it_;
    }

    bool operator!=(const iterator &other) const { return !(*this == other); }

   private:
    void advance_to_valid();

    const EdgeStore *store_;
    ConcurrentSet<int64_t>::LockedView::iterator edge_ids_it_;
    ConcurrentSet<int64_t>::LockedView::iterator edge_ids_end_;
    std::string type_filter_;
    std::shared_ptr<Edge> current_edge_;
  };

  EdgeView(const EdgeStore *store, const ConcurrentSet<int64_t> &edge_ids,
           const std::string &type_filter = "")
      : store_(store),
        edge_ids_view_(edge_ids.get_all_unsafe()),
        type_filter_(type_filter) {}

  iterator begin() const {
    return iterator(store_, edge_ids_view_.begin(), edge_ids_view_.end(),
                    type_filter_);
  }

  iterator end() const {
    return iterator(store_, edge_ids_view_.end(), edge_ids_view_.end(),
                    type_filter_);
  }

  // Convenience method to count matching edges without materializing them
  size_t count() const {
    size_t result = 0;
    for (auto it = begin(); it != end(); ++it) {
      ++result;
    }
    return result;
  }

 private:
  const EdgeStore *store_;
  ConcurrentSet<int64_t>::LockedView edge_ids_view_;
  std::string type_filter_;
};

// todo rename to EdgeManager
class EdgeStore {
  struct TableCache;

 private:
  tbb::concurrent_hash_map<int64_t, std::shared_ptr<Edge>> edges;
  tbb::concurrent_hash_map<std::string, ConcurrentSet<int64_t>> edges_by_type_;
  tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>> outgoing_edges_;
  tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>> incoming_edges_;

  tbb::concurrent_hash_map<std::string, std::atomic<int64_t>>
      versions_;  // version
  std::atomic<int64_t> edge_id_counter_{0};

  ConcurrentSet<int64_t> edge_ids_;

  tbb::concurrent_hash_map<std::string, std::shared_ptr<TableCache>>
      tables_;  // cache
  std::string data_file_;
  int64_t chunk_size_;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_edges_from_map(
      const tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>> &edge_map,
      int64_t id, const std::string &type) const;

  arrow::Result<std::shared_ptr<arrow::Table>> generate_table(
      const std::string &edge_type) const;

  arrow::Result<int64_t> get_version_snapshot(
      const std::string &edge_type) const;

  struct TableCache {
    std::shared_ptr<arrow::Table> table;
    std::atomic<int64_t> version{0};
    std::mutex lock;
  };

 public:
  explicit EdgeStore(const int64_t init_edge_id_counter,
                     const int64_t chunk_size = 1000)
      : edge_id_counter_(init_edge_id_counter), chunk_size_(chunk_size) {}

  ~EdgeStore() {
    edges.clear();
    edges_by_type_.clear();
    outgoing_edges_.clear();
    incoming_edges_.clear();
    versions_.clear();
    edge_ids_.clear();
    tables_.clear();
  }

  int64_t get_edge_id_counter() const { return edge_id_counter_; }

  arrow::Result<std::shared_ptr<Edge>> create_edge(
      int64_t source_id, const std::string &type, int64_t target_id,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
          properties = {});

  arrow::Result<bool> add(const std::shared_ptr<Edge> &edge);

  arrow::Result<bool> remove(int64_t edge_id);

  arrow::Result<std::shared_ptr<Edge>> get(int64_t edge_id) const;

  std::vector<std::shared_ptr<Edge>> get(const std::set<int64_t> &ids) const;

  int64_t get_count_by_type(const std::string &type) const {
    if (auto res = get_by_type(type); res.ok()) {
      return res.ValueOrDie().size();
    }
    return 0;
  }

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_outgoing_edges(
      int64_t id, const std::string &type = "") const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_incoming_edges(
      int64_t id, const std::string &type = "") const;

  // New view-based methods that avoid copying
  arrow::Result<EdgeView> get_outgoing_edges_view(
      int64_t id, const std::string &type = "") const;

  arrow::Result<EdgeView> get_incoming_edges_view(
      int64_t id, const std::string &type = "") const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_by_type(
      const std::string &type) const;

  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &edge_type);

  arrow::Result<int64_t> get_version(const std::string &edge_type) const;

  std::set<std::string> get_edge_types() const;

  int64_t get_chunk_size() const { return chunk_size_; }

  size_t size() const { return edges.size(); }

  bool empty() const { return edges.empty(); }

  // todo temporary solution, add initialize instead
  void set_id_seq(const int64_t v) {
    edge_id_counter_.store(v, std::memory_order_relaxed);
  }

  // Friend class for EdgeView iterator access
  friend class EdgeView::iterator;
};
}  // namespace tundradb

#endif  // EDGE_STORE_HPP
