#ifndef EDGE_HPP
#define EDGE_HPP

#include <utility>

#include "utils.hpp"

namespace tundradb {
class Edge {
 private:
  const int64_t id_;          // Unique edge ID
  const int64_t source_id_;   // Source node ID
  const int64_t target_id_;   // Target node ID
  const std::string type_;    // Relationship type (e.g., "FOLLOWS")
  const int64_t created_ts_;  // Creation timestamp

  const std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
      properties;

 public:
  Edge(
      int64_t id, int64_t source_id, int64_t target_id, std::string type,
      std::unordered_map<std::string, std::shared_ptr<arrow::Array>> properties,
      int64_t created_ts)
      : id_(id),
        source_id_(source_id),
        target_id_(target_id),
        type_(std::move(type)),
        created_ts_(created_ts),
        properties(std::move(properties)) {}

  [[nodiscard]] int64_t get_id() const { return id_; }
  [[nodiscard]] int64_t get_source_id() const { return source_id_; }
  [[nodiscard]] int64_t get_target_id() const { return target_id_; }
  [[nodiscard]] const std::string& get_type() const { return type_; }
  [[nodiscard]] int64_t get_created_ts() const { return created_ts_; }
};
}  // namespace tundradb
#endif  // EDGE_HPP
