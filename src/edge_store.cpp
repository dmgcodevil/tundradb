

#include "edge_store.hpp"

namespace tundradb {

arrow::Result<std::shared_ptr<Edge>> EdgeStore::create_edge(
    int64_t source_id, int64_t target_id, const std::string& type,
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> properties) {
  std::make_shared<Edge>(this->edge_id_counter++, source_id, target_id, type,
                         properties);
}

arrow::Result<bool> EdgeStore::add(std::shared_ptr<Edge> edge) {
  if (this->edges.contains(edge->get_id())) {
    return arrow::Status::Invalid("Edge already exists");
  }
  this->edges[edge->get_id()] = move(edge);
  this->outgoing_edges[edge->get_source_id()].push_back(edge->get_id());
  this->incoming_edges[edge->get_target_id()].push_back(edge->get_id());
  this->edges_by_type[edge->get_type()].push_back(edge->get_id());
  // todo more indexes
  return true;
}

arrow::Result<int64_t> EdgeStore::get_updated_ts(
    const std::string& edge_type) const {
  if (this->last_updated_ts.contains(edge_type)) {
    return last_updated_ts.at(edge_type);
  }
  return arrow::Status::KeyError("Unknown edge type " + edge_type);
}

}  // namespace tundradb
