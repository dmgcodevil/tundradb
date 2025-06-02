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

  arrow::Result<bool> add(std::shared_ptr<Edge> edge);

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
  void set_id_seq(int64_t v) {
    edge_id_counter_.store(v, std::memory_order_relaxed);
  }
};
}  // namespace tundradb

#endif  // EDGE_STORE_HPP
