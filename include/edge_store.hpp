#ifndef EDGE_STORE_HPP
#define EDGE_STORE_HPP

#include <arrow/api.h>
#include <tbb/concurrent_hash_map.h>

#include <mutex>
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

  tbb::concurrent_hash_map<std::string, ConcurrentSet<int64_t>> edges_by_type;
  tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>> outgoing_edges;
  tbb::concurrent_hash_map<int64_t, ConcurrentSet<int64_t>> incoming_edges;

  tbb::concurrent_hash_map<std::string, std::atomic<int64_t>>
      versions;  // version
  std::atomic<int64_t> edge_id_counter{0};

  ConcurrentSet<int64_t> edge_ids;

  tbb::concurrent_hash_map<std::string, std::shared_ptr<TableCache>>
      tables;  // cache
  std::string data_file;
  int64_t chunk_size;

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
  explicit EdgeStore(int64_t init_edge_id_counter, int64_t chunk_size = 1000)
      : edge_id_counter(init_edge_id_counter), chunk_size(chunk_size) {}

  ~EdgeStore() {
    // Clear all concurrent containers to ensure proper cleanup
    edges.clear();
    edges_by_type.clear();
    outgoing_edges.clear();
    incoming_edges.clear();
    versions.clear();
    edge_ids.clear();
    tables.clear();
  }

  arrow::Result<std::shared_ptr<Edge>> create_edge(
      int64_t source_id, int64_t target_id, const std::string &type,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
          properties = {});

  arrow::Result<bool> add(std::shared_ptr<Edge> edge);

  arrow::Result<bool> remove(int64_t edge_id);

  arrow::Result<std::shared_ptr<Edge>> get(int64_t edge_id) const;

  std::vector<std::shared_ptr<Edge>> get(const std::set<int64_t> &ids) const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_outgoing_edges(
      int64_t id, const std::string &type = "") const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_incoming_edges(
      int64_t id, const std::string &type = "") const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_by_type(
      const std::string &type) const;

  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &edge_type = "");

  arrow::Result<int64_t> get_version(const std::string &edge_type) const;

  std::set<std::string> get_edge_types() const;

  int64_t get_chunk_size() const { return chunk_size; }

  size_t size() const { return edges.size(); }

  bool empty() const { return edges.empty(); }
};
}  // namespace tundradb

#endif  // EDGE_STORE_HPP
