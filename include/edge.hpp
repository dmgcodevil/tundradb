#ifndef EDGE_HPP
#define EDGE_HPP

#include "utils.hpp"

namespace tundradb {
class Edge {
 private:
  // All fields are const, making Edge immutable
  const int64_t id;                 // Unique edge ID
  const int64_t source_id;          // Source node ID
  const int64_t target_id;          // Target node ID
  const std::string type;           // Relationship type (e.g., "FOLLOWS")
  const std::string source_schema;  // Schema of source node
  const std::string target_schema;  // Schema of target node
  const int64_t created_ts;         // Creation timestamp

  // Properties are also immutable
  const std::unordered_map<std::string, std::shared_ptr<arrow::Array> >
      properties;

 public:
  // Constructor
  Edge(int64_t id, int64_t source_id, int64_t target_id,
       const std::string &type, const std::string &source_schema,
       const std::string &target_schema,
       std::unordered_map<std::string, std::shared_ptr<arrow::Array> >
           properties,
       int64_t created_ts = now_millis());

  // Getters (no setters since everything is immutable)
  int64_t get_id() const { return id; }
  int64_t get_source_id() const { return source_id; }
  int64_t get_target_id() const { return target_id; }
  const std::string &get_type() const { return type; }
  const std::string &get_source_schema() const { return source_schema; }
  const std::string &get_target_schema() const { return target_schema; }
  int64_t get_created_ts() const { return created_ts; }

  // Property access (read-only)
  arrow::Result<std::shared_ptr<arrow::Array> > get_property(
      const std::string &name) const;

  // Get all property names
  std::vector<std::string> get_property_names() const;
};
}  // namespace tundradb
#endif  // EDGE_HPP
