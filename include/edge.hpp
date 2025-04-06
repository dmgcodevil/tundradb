#ifndef EDGE_HPP
#define EDGE_HPP

#include "utils.hpp"

namespace tundradb {
class Edge {
 private:
  const int64_t id;          // Unique edge ID
  const int64_t source_id;   // Source node ID
  const int64_t target_id;   // Target node ID
  const std::string type;    // Relationship type (e.g., "FOLLOWS")
  const int64_t created_ts;  // Creation timestamp

  const std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
      properties;

 public:
  Edge(
      int64_t id, int64_t source_id, int64_t target_id, const std::string& type,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> properties,
      int64_t created_ts)
      : id(id),
        source_id(source_id),
        target_id(target_id),
        type(type),
        created_ts(created_ts),
        properties(std::move(properties)) {}

  int64_t get_id() const { return id; }
  int64_t get_source_id() const { return source_id; }
  int64_t get_target_id() const { return target_id; }
  const std::string& get_type() const { return type; }
  int64_t get_created_ts() const { return created_ts; }
  // arrow::Result<std::shared_ptr<arrow::Array>> get_property(
  //    const std::string &name) const;
  // std::vector<std::string> get_property_names() const;
};
}  // namespace tundradb
#endif  // EDGE_HPP
