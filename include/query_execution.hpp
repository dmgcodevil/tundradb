#ifndef QUERY_EXECUTION_HPP
#define QUERY_EXECUTION_HPP

#include <arrow/result.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "query.hpp"
#include "schema.hpp"

namespace tundradb {

// Forward declarations
class SchemaRegistry;
class NodeManager;

/**
 * @brief Runtime connection between two nodes discovered during traversal
 *
 * Represents an edge in the graph that was found during query execution.
 * Different from Traverse (which is part of query AST) - this is actual data.
 */
struct GraphConnection {
  SchemaRef source;
  int64_t source_id;
  std::string edge_type;
  std::string label;
  SchemaRef target;
  int64_t target_id;

  [[nodiscard]] std::string toString() const {
    std::stringstream ss;
    ss << "{(" << source << ":id=" << source_id << "->[:" << edge_type << "]->"
       << "(" << label << ":" << target << ":id=" << target_id << ")}";
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os, const GraphConnection& c) {
    os << c.toString();
    return os;
  }
};

/**
 * @brief Connection pool for reusing GraphConnection objects
 */
class ConnectionPool {
 private:
  std::vector<GraphConnection> pool_;
  size_t next_index_ = 0;

 public:
  explicit ConnectionPool(size_t initial_size = 1000) : pool_(initial_size) {}

  GraphConnection& get() {
    if (next_index_ >= pool_.size()) {
      pool_.resize(pool_.size() * 2);
    }
    return pool_[next_index_++];
  }

  void reset() { next_index_ = 0; }
  size_t size() const { return next_index_; }
};

/**
 * @brief Manages schema resolution and aliases for query execution
 *
 * Responsibilities:
 * - Map aliases (e.g., "u") to schema names (e.g., "User")
 * - Resolve SchemaRef objects to concrete schema names
 * - Validate schema references
 */
class SchemaContext {
 private:
  std::unordered_map<std::string, std::string> aliases_;
  std::shared_ptr<SchemaRegistry> schema_registry_;

 public:
  explicit SchemaContext(std::shared_ptr<SchemaRegistry> registry)
      : schema_registry_(std::move(registry)) {}

  /**
   * Register a schema alias (e.g., "u" -> "User")
   */
  arrow::Result<std::string> register_schema(const SchemaRef& schema_ref);

  /**
   * Resolve schema reference to concrete schema name
   */
  arrow::Result<std::string> resolve(const SchemaRef& schema_ref) const;

  /**
   * Get schema registry
   */
  std::shared_ptr<SchemaRegistry> registry() const { return schema_registry_; }

  /**
   * Get all registered aliases
   */
  const std::unordered_map<std::string, std::string>& get_aliases() const {
    return aliases_;
  }
};

/**
 * @brief Manages graph topology during query execution
 *
 * Responsibilities:
 * - Track active node IDs per schema
 * - Store connections (edges) between nodes
 * - Query connection information
 */
class GraphState {
 private:
  // Node IDs per schema alias
  llvm::StringMap<llvm::DenseSet<int64_t>> node_ids_;

  // Outgoing connections: schema -> source_id -> [connections]
  llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>
      outgoing_;

  // Incoming connections: target_id -> [connections]
  llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>> incoming_;

  // Connection object pool for performance
  mutable ConnectionPool connection_pool_;

 public:
  /**
   * Get node IDs for a schema (mutable)
   */
  llvm::DenseSet<int64_t>& ids(const std::string& schema_alias) {
    return node_ids_[schema_alias];
  }

  /**
   * Get node IDs for a schema (const)
   */
  const llvm::DenseSet<int64_t>& ids(const std::string& schema_alias) const {
    auto it = node_ids_.find(schema_alias);
    if (it != node_ids_.end()) {
      return it->second;
    }
    static const llvm::DenseSet<int64_t> empty;
    return empty;
  }

  /**
   * Add a connection between nodes
   */
  void add_connection(const GraphConnection& conn) {
    auto& pool_conn = connection_pool_.get();
    pool_conn = conn;

    outgoing_[conn.source.value()][conn.source_id].push_back(pool_conn);
    incoming_[conn.target_id].push_back(pool_conn);
  }

  /**
   * Check if node has outgoing edges
   */
  bool has_outgoing(const SchemaRef& schema_ref, int64_t node_id) const {
    return outgoing_.contains(schema_ref.value()) &&
           outgoing_.at(schema_ref.value()).contains(node_id) &&
           !outgoing_.at(schema_ref.value()).at(node_id).empty();
  }

  /**
   * Get outgoing connections for a node
   */
  const llvm::SmallVector<GraphConnection, 4>* get_outgoing(
      const std::string& schema_alias, int64_t node_id) const {
    auto schema_it = outgoing_.find(schema_alias);
    if (schema_it == outgoing_.end()) {
      return nullptr;
    }

    auto node_it = schema_it->second.find(node_id);
    if (node_it == schema_it->second.end()) {
      return nullptr;
    }

    return &node_it->second;
  }

  /**
   * Remove a node from the graph
   */
  void remove_node(int64_t node_id, const SchemaRef& schema_ref) {
    node_ids_[schema_ref.value()].erase(node_id);
  }

  /**
   * Access to outgoing connections map
   */
  const llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>&
  outgoing() const {
    return outgoing_;
  }

  /**
   * Access to incoming connections map
   */
  const llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>&
  incoming() const {
    return incoming_;
  }

  /**
   * Access to incoming connections map (mutable)
   */
  llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>& incoming() {
    return incoming_;
  }

  /**
   * Access to connection pool
   */
  ConnectionPool& connection_pool() const { return connection_pool_; }

  /**
   * Get all node IDs (for backward compatibility)
   */
  llvm::StringMap<llvm::DenseSet<int64_t>>& get_ids() { return node_ids_; }

  const llvm::StringMap<llvm::DenseSet<int64_t>>& get_ids() const {
    return node_ids_;
  }

  /**
   * Get outgoing connections map (direct access to internal structure)
   * Returns: schema -> node_id -> connections
   */
  llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>&
  get_outgoing_map() {
    return outgoing_;
  }

  const llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>&
  get_outgoing_map() const {
    return outgoing_;
  }
};

/**
 * @brief Manages field indexing for efficient row operations
 *
 * Responsibilities:
 * - Assign unique integer IDs to fully-qualified field names
 * - Map field IDs to names and vice versa
 * - Compute field indices per schema
 */
class FieldIndexer {
 private:
  // Fully-qualified field names per schema alias
  llvm::StringMap<std::vector<std::string>> fq_field_names_;

  // Field indices per schema
  llvm::StringMap<std::vector<int>> schema_field_indices_;

  // Bidirectional mapping between field IDs and names
  llvm::SmallDenseMap<int, std::string, 64> field_id_to_name_;
  llvm::StringMap<int> field_name_to_index_;

  // Global field ID counter
  std::atomic<int> next_field_id_{0};

 public:
  /**
   * Compute fully-qualified field names for a schema
   */
  arrow::Result<bool> compute_fq_names(const SchemaRef& schema_ref,
                                       const std::string& resolved_schema,
                                       SchemaRegistry* registry);

  /**
   * Get field indices for a schema
   */
  const std::vector<int>* get_field_indices(
      const std::string& schema_alias) const {
    auto it = schema_field_indices_.find(schema_alias);
    return it != schema_field_indices_.end() ? &it->second : nullptr;
  }

  /**
   * Get field name by ID
   */
  const std::string& get_field_name(int field_id) const {
    return field_id_to_name_.at(field_id);
  }

  /**
   * Get field ID by name
   */
  int get_field_id(const std::string& field_name) const {
    auto it = field_name_to_index_.find(field_name);
    return it != field_name_to_index_.end() ? it->second : -1;
  }

  /**
   * Get all field ID to name mappings (for row operations)
   */
  const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name() const {
    return field_id_to_name_;
  }

  /**
   * Check if schema field names are already computed
   */
  bool has_computed(const std::string& schema_alias) const {
    return fq_field_names_.contains(schema_alias);
  }

  /**
   * Get schema_field_indices (for backward compatibility)
   * Returns the actual internal map of schema -> field_indices
   */
  llvm::StringMap<std::vector<int>>& get_schema_field_indices() {
    return schema_field_indices_;
  }

  const llvm::StringMap<std::vector<int>>& get_schema_field_indices() const {
    return schema_field_indices_;
  }

  /**
   * Get field_id_to_name map (direct access to internal map)
   */
  llvm::SmallDenseMap<int, std::string, 64>& get_field_id_to_name() {
    return field_id_to_name_;
  }

  const llvm::SmallDenseMap<int, std::string, 64>& get_field_id_to_name()
      const {
    return field_id_to_name_;
  }

 private:
  // No cached views needed - we expose the internal maps directly
};

/**
 * @brief Query execution state container
 *
 * Composed of focused components:
 * - SchemaContext: Schema resolution and aliases
 * - GraphState: Graph topology (node IDs, connections)
 * - FieldIndexer: Field indexing for efficient row operations
 * - Tables: Arrow table storage
 */
struct QueryState {
  // Core components
  SchemaContext schemas;
  GraphState graph;
  FieldIndexer fields;

  // Table storage
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables;

  // Source schema for FROM clause
  SchemaRef from;

  // Traversals in query
  std::vector<Traverse> traversals;

  // Node manager for fetching nodes
  std::shared_ptr<NodeManager> node_manager;

  // Temporal context (nullptr = current version)
  std::unique_ptr<TemporalContext> temporal_context;

  // Constructor
  explicit QueryState(std::shared_ptr<SchemaRegistry> registry);

  // Convenience accessors (delegate to components)

  arrow::Result<std::string> register_schema(const SchemaRef& ref) {
    return schemas.register_schema(ref);
  }

  arrow::Result<std::string> resolve_schema(const SchemaRef& ref) const {
    return schemas.resolve(ref);
  }

  llvm::DenseSet<int64_t>& get_ids(const SchemaRef& schema_ref) {
    return graph.ids(schema_ref.value());
  }

  const llvm::DenseSet<int64_t>& get_ids(const SchemaRef& schema_ref) const {
    return graph.ids(schema_ref.value());
  }

  bool has_outgoing(const SchemaRef& ref, int64_t node_id) const {
    return graph.has_outgoing(ref, node_id);
  }

  arrow::Result<bool> compute_fully_qualified_names(
      const SchemaRef& ref, const std::string& resolved_schema) {
    return fields.compute_fq_names(ref, resolved_schema,
                                   schemas.registry().get());
  }

  arrow::Result<bool> compute_fully_qualified_names(const SchemaRef& ref);

  void remove_node(int64_t node_id, const SchemaRef& ref) {
    graph.remove_node(node_id, ref);
  }

  // Backward compatibility accessors for core.cpp migration
  std::shared_ptr<SchemaRegistry> schema_registry() const {
    return schemas.registry();
  }

  const std::unordered_map<std::string, std::string>& aliases() const {
    return schemas.get_aliases();
  }

  llvm::StringMap<llvm::DenseSet<int64_t>>& ids() { return graph.get_ids(); }

  const llvm::StringMap<llvm::DenseSet<int64_t>>& ids() const {
    return graph.get_ids();
  }

  llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>&
  connections() {
    return graph.get_outgoing_map();
  }

  const llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>&
  connections() const {
    return graph.get_outgoing_map();
  }

  // Direct access to incoming connections (by node ID)
  llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>& incoming() {
    return graph.incoming();
  }

  const llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>&
  incoming() const {
    return graph.incoming();
  }

  llvm::StringMap<std::vector<int>>& schema_field_indices() {
    return fields.get_schema_field_indices();
  }

  const llvm::StringMap<std::vector<int>>& schema_field_indices() const {
    return fields.get_schema_field_indices();
  }

  llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name() {
    return fields.get_field_id_to_name();
  }

  const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name() const {
    return fields.get_field_id_to_name();
  }

  // Connection pool accessor (for core.cpp)
  ConnectionPool& connection_pool() { return graph.connection_pool(); }

  const ConnectionPool& connection_pool() const {
    return graph.connection_pool();
  }

  // Complex methods - implemented in query_execution.cpp
  void reserve_capacity(const Query& query);

  arrow::Result<bool> update_table(const std::shared_ptr<arrow::Table>& table,
                                   const SchemaRef& schema_ref);

  std::string ToString() const;
};

}  // namespace tundradb

#endif  // QUERY_EXECUTION_HPP
