#ifndef EDGE_STORE_HPP
#define EDGE_STORE_HPP

#include <arrow/api.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "edge.hpp"

namespace tundradb {
class EdgeStore {
 private:
  // Primary edge storage
  std::unordered_map<int64_t, std::shared_ptr<Edge>> edges;

  // Indexes for efficient access
  std::unordered_map<std::string, std::vector<int64_t>> edges_by_type;
  std::unordered_map<int64_t, std::vector<int64_t>> outgoing_edges;
  std::unordered_map<int64_t, std::vector<int64_t>> incoming_edges;

  // Edge creation tracking
  std::atomic<int64_t> edge_id_counter{0};

  // Change tracking for persistence
  std::atomic<bool> updated{true};
  int64_t updated_ts;

 public:
  EdgeStore();

  // Basic edge operations
  arrow::Result<std::shared_ptr<Edge>> create_edge(
      int64_t source_id, int64_t target_id, const std::string &type,
      const std::string &source_schema, const std::string &target_schema,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
          properties = {});

  void add(const std::shared_ptr<Edge> &edge);

  void remove(int64_t edge_id);

  arrow::Result<std::shared_ptr<Edge>> get(int64_t edge_id) const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_outgoing_edges(
      int64_t id, const std::string &type = "") const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_incoming_edges(
      int64_t id, const std::string &type = "") const;

  arrow::Result<std::vector<std::shared_ptr<Edge>>> get_by_type(
      const std::string &type) const;

  arrow::Result<std::shared_ptr<arrow::Table>> get_table(
      const std::string &edge_type = "") const;

  bool is_updated() const { return updated.load(std::memory_order_acquire); }

  bool set_updated(bool value) {
    return updated.exchange(value, std::memory_order_acq_rel);
  }

  int64_t get_updated_ts() const { return updated_ts; }

  std::vector<std::string> get_edge_types() const;

  size_t size() const { return edges.size(); }
  bool empty() const { return edges.empty(); }
};
}  // namespace tundradb

#endif  // EDGE_STORE_HPP
