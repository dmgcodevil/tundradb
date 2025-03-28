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
  std::unordered_map<int64_t, std::shared_ptr<Edge>> edges;
  std::unordered_map<std::string, std::vector<int64_t>> edges_by_type;
  std::unordered_map<int64_t, std::vector<int64_t>> outgoing_edges;
  std::unordered_map<int64_t, std::vector<int64_t>> incoming_edges;
  std::unordered_map<std::string, std::atomic<int64_t>> last_updated_ts;
  std::atomic<int64_t> edge_id_counter{0};

 public:
  explicit EdgeStore(int64_t edge_id_counter)
      : edge_id_counter(edge_id_counter) {}

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

  // todo address empty map
  int64_t get_updated_ts(const std::string &edge_type) const {
    return last_updated_ts.at(edge_type);
  }

  std::vector<std::string> get_edge_types() const;

  size_t size() const { return edges.size(); }
  bool empty() const { return edges.empty(); }
};
}  // namespace tundradb

#endif  // EDGE_STORE_HPP
