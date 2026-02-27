#ifndef QUERY_EXECUTION_HPP
#define QUERY_EXECUTION_HPP

#include <arrow/result.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>

#include <atomic>
#include <map>
#include <memory>
#include <set>
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
 * @brief Object pool for GraphConnection instances to reduce heap allocations.
 */
class ConnectionPool {
 private:
  std::vector<GraphConnection> pool_;
  size_t next_index_ = 0;

 public:
  /** @brief Constructs a pool pre-allocated with @p initial_size slots. */
  explicit ConnectionPool(size_t initial_size = 1000) : pool_(initial_size) {}

  /** @brief Returns a reference to the next available slot, growing if needed.
   */
  GraphConnection& get() {
    if (next_index_ >= pool_.size()) {
      pool_.resize(pool_.size() * 2);
    }
    return pool_[next_index_++];
  }

  /** @brief Resets the pool for reuse without deallocating memory. */
  void reset() { next_index_ = 0; }

  /** @brief Returns the number of connections currently in use. */
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
   * @brief Registers a schema alias (e.g. "u" → "User").
   *
   * @param schema_ref The schema reference containing alias and schema name.
   * @return The resolved concrete schema name, or an error if unknown.
   */
  arrow::Result<std::string> register_schema(const SchemaRef& schema_ref);

  /**
   * @brief Resolves a schema reference to its concrete schema name.
   *
   * @param schema_ref The reference to resolve.
   * @return The concrete schema name, or an error if not registered.
   */
  arrow::Result<std::string> resolve(const SchemaRef& schema_ref) const;

  /** @brief Returns the underlying SchemaRegistry. */
  std::shared_ptr<SchemaRegistry> registry() const { return schema_registry_; }

  /** @brief Returns all registered alias→schema mappings. */
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
   * @brief Returns the mutable set of node IDs for a schema alias.
   *
   * @param schema_alias The alias (e.g. "u").
   * @return Reference to the ID set (created on first access).
   */
  llvm::DenseSet<int64_t>& ids(const std::string& schema_alias) {
    return node_ids_[schema_alias];
  }

  /**
   * @brief Returns an immutable view of the node IDs for a schema alias.
   *
   * @param schema_alias The alias to look up.
   * @return A const reference; returns a static empty set if not found.
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
   * @brief Records a connection (edge) discovered during traversal.
   *
   * The connection is stored in both the outgoing and incoming maps.
   *
   * @param conn The connection to record.
   */
  void add_connection(const GraphConnection& conn) {
    auto& pool_conn = connection_pool_.get();
    pool_conn = conn;

    outgoing_[conn.source.value()][conn.source_id].push_back(pool_conn);
    incoming_[conn.target_id].push_back(pool_conn);
  }

  /**
   * @brief Returns true if the node has at least one outgoing edge in the
   * graph.
   *
   * @param schema_ref The node's schema.
   * @param node_id The node identifier.
   */
  bool has_outgoing(const SchemaRef& schema_ref, int64_t node_id) const {
    return outgoing_.contains(schema_ref.value()) &&
           outgoing_.at(schema_ref.value()).contains(node_id) &&
           !outgoing_.at(schema_ref.value()).at(node_id).empty();
  }

  /**
   * @brief Returns the outgoing connections for a specific node, or nullptr.
   *
   * @param schema_alias The schema alias containing the node.
   * @param node_id The node identifier.
   * @return Pointer to the connection vector, or nullptr if none exist.
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
   * @brief Removes a node ID from the tracked set for its schema.
   *
   * @param node_id The node to remove.
   * @param schema_ref The schema the node belongs to.
   */
  void remove_node(int64_t node_id, const SchemaRef& schema_ref) {
    node_ids_[schema_ref.value()].erase(node_id);
  }

  /** @brief Returns the outgoing connections map (const). schema → node_id →
   * [connections]. */
  const llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>&
  outgoing() const {
    return outgoing_;
  }

  /** @brief Returns the incoming connections map (const). target_id →
   * [connections]. */
  const llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>&
  incoming() const {
    return incoming_;
  }

  /** @brief Returns the incoming connections map (mutable). */
  llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>& incoming() {
    return incoming_;
  }

  /** @brief Returns the connection object pool. */
  ConnectionPool& connection_pool() const { return connection_pool_; }

  /** @brief Returns the full node-ID map (schema alias → ID set). */
  llvm::StringMap<llvm::DenseSet<int64_t>>& get_ids() { return node_ids_; }

  /** @overload */
  const llvm::StringMap<llvm::DenseSet<int64_t>>& get_ids() const {
    return node_ids_;
  }

  /** @brief Returns the outgoing connections map (mutable). schema → node_id →
   * [connections]. */
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
   * @brief Computes fully-qualified field names and assigns field IDs for a
   * schema.
   *
   * @param schema_ref The schema reference (alias used as prefix).
   * @param resolved_schema The concrete schema name.
   * @param registry The schema registry for field lookup.
   * @return True if names were computed (false if already done).
   */
  arrow::Result<bool> compute_fq_names(const SchemaRef& schema_ref,
                                       const std::string& resolved_schema,
                                       SchemaRegistry* registry);

  /**
   * @brief Returns the field-index vector for a schema alias, or nullptr.
   *
   * @param schema_alias The alias to look up.
   * @return Pointer to the index vector, or nullptr if not computed.
   */
  const std::vector<int>* get_field_indices(
      const std::string& schema_alias) const {
    auto it = schema_field_indices_.find(schema_alias);
    return it != schema_field_indices_.end() ? &it->second : nullptr;
  }

  /**
   * @brief Returns the fully-qualified field name for a given field ID.
   *
   * @param field_id The field identifier.
   * @return Reference to the field name string.
   */
  const std::string& get_field_name(int field_id) const {
    return field_id_to_name_.at(field_id);
  }

  /**
   * @brief Looks up a field ID by fully-qualified name.
   *
   * @param field_name The fully-qualified name (e.g. "u.age").
   * @return The field ID, or −1 if not found.
   */
  int get_field_id(const std::string& field_name) const {
    auto it = field_name_to_index_.find(field_name);
    return it != field_name_to_index_.end() ? it->second : -1;
  }

  /** @brief Returns the field-ID → name map (const). */
  const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name() const {
    return field_id_to_name_;
  }

  /**
   * @brief Returns true if field names for @p schema_alias are already
   * computed.
   */
  bool has_computed(const std::string& schema_alias) const {
    return fq_field_names_.contains(schema_alias);
  }

  /** @brief Returns the schema → field-indices map (mutable). */
  llvm::StringMap<std::vector<int>>& get_schema_field_indices() {
    return schema_field_indices_;
  }

  const llvm::StringMap<std::vector<int>>& get_schema_field_indices() const {
    return schema_field_indices_;
  }

  /** @brief Returns the field-ID → name map (mutable). */
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
  SchemaContext schemas;  ///< Schema resolution and aliases.
  GraphState graph;       ///< Graph topology (IDs, connections).
  FieldIndexer fields;    ///< Field indexing for row operations.

  /// Arrow tables keyed by schema alias.
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables;

  SchemaRef from;                    ///< Source schema from the FROM clause.
  std::vector<Traverse> traversals;  ///< Traverse clauses in query order.

  std::shared_ptr<NodeManager> node_manager;  ///< Node storage.
  std::unique_ptr<TemporalContext>
      temporal_context;  ///< Temporal snapshot (nullptr = current).

  /** @brief Constructs a QueryState bound to the given schema registry. */
  explicit QueryState(std::shared_ptr<SchemaRegistry> registry);

  /** @brief Registers a schema alias. @see SchemaContext::register_schema. */
  arrow::Result<std::string> register_schema(const SchemaRef& ref) {
    return schemas.register_schema(ref);
  }

  /** @brief Resolves a schema alias to its concrete name. */
  arrow::Result<std::string> resolve_schema(const SchemaRef& ref) const {
    return schemas.resolve(ref);
  }

  /** @brief Returns the mutable ID set for the given schema. */
  llvm::DenseSet<int64_t>& get_ids(const SchemaRef& schema_ref) {
    return graph.ids(schema_ref.value());
  }

  /** @overload */
  const llvm::DenseSet<int64_t>& get_ids(const SchemaRef& schema_ref) const {
    return graph.ids(schema_ref.value());
  }

  /** @brief Returns true if the node has outgoing edges. */
  bool has_outgoing(const SchemaRef& ref, int64_t node_id) const {
    return graph.has_outgoing(ref, node_id);
  }

  /** @brief Computes FQ field names for the given schema reference. */
  arrow::Result<bool> compute_fully_qualified_names(
      const SchemaRef& ref, const std::string& resolved_schema) {
    return fields.compute_fq_names(ref, resolved_schema,
                                   schemas.registry().get());
  }

  /** @overload Resolves the schema name automatically. */
  arrow::Result<bool> compute_fully_qualified_names(const SchemaRef& ref);

  /** @brief Removes a node from the graph. */
  void remove_node(int64_t node_id, const SchemaRef& ref) {
    graph.remove_node(node_id, ref);
  }

  /** @brief Returns the schema registry. */
  std::shared_ptr<SchemaRegistry> schema_registry() const {
    return schemas.registry();
  }

  /** @brief Returns all alias→schema mappings. */
  const std::unordered_map<std::string, std::string>& aliases() const {
    return schemas.get_aliases();
  }

  /** @brief Returns the full schema→ID-set map (mutable). */
  llvm::StringMap<llvm::DenseSet<int64_t>>& ids() { return graph.get_ids(); }

  /** @overload */
  const llvm::StringMap<llvm::DenseSet<int64_t>>& ids() const {
    return graph.get_ids();
  }

  /** @brief Returns the outgoing connections map (mutable). */
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

  /** @brief Returns the incoming connections map (mutable). */
  llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>& incoming() {
    return graph.incoming();
  }

  const llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>&
  incoming() const {
    return graph.incoming();
  }

  /** @brief Returns the schema → field-indices map (mutable). */
  llvm::StringMap<std::vector<int>>& schema_field_indices() {
    return fields.get_schema_field_indices();
  }

  const llvm::StringMap<std::vector<int>>& schema_field_indices() const {
    return fields.get_schema_field_indices();
  }

  /** @brief Returns the field-ID → name map (mutable). */
  llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name() {
    return fields.get_field_id_to_name();
  }

  const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name() const {
    return fields.get_field_id_to_name();
  }

  /** @brief Returns the connection object pool (mutable). */
  ConnectionPool& connection_pool() { return graph.connection_pool(); }

  /** @overload */
  const ConnectionPool& connection_pool() const {
    return graph.connection_pool();
  }

  /**
   * @brief Pre-allocates internal data structures based on query shape.
   *
   * @param query The query whose traversals determine capacity.
   */
  void reserve_capacity(const Query& query);

  /**
   * @brief Stores (or replaces) an Arrow table for the given schema.
   *
   * Also extracts node IDs from the table's "id" column.
   *
   * @param table The Arrow table.
   * @param schema_ref The schema alias.
   * @return True on success.
   */
  arrow::Result<bool> update_table(const std::shared_ptr<arrow::Table>& table,
                                   const SchemaRef& schema_ref);

  /** @brief Returns a multi-line debug string of all query state. */
  std::string ToString() const;
};

/**
 * @brief Recursively collects all paths from a node in a connection graph
 * (debug).
 *
 * @param id The starting node ID.
 * @param connections The connection adjacency list.
 * @param path Accumulator for the current path.
 * @param[out] res Collects completed path strings.
 */
void debug_connections(
    int64_t id,
    const std::map<int64_t, std::vector<GraphConnection>>& connections,
    std::vector<std::string> path, std::vector<std::string>& res);

/**
 * @brief Prints all paths in a connection graph to the debug log.
 *
 * @param connections The connection adjacency list.
 */
void print_paths(
    const std::map<int64_t, std::vector<GraphConnection>>& connections);

/**
 * @brief Finds root nodes (nodes with no incoming edges) in a connection graph.
 *
 * @param connections The connection adjacency list.
 * @return A set of root node IDs.
 */
std::set<int64_t> get_roots(
    const std::map<int64_t, std::vector<GraphConnection>>& connections);

/**
 * @brief Builds the denormalised Arrow output schema from the query state.
 *
 * Combines fields from all traversed schemas, prefixed by their alias.
 *
 * @param query_state The current execution state.
 * @return The merged Arrow schema, or an error.
 */
arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema(
    const QueryState& query_state);

/**
 * @brief Logs grouped outgoing connections for a node (debug).
 *
 * @param node_id The source node ID.
 * @param grouped_connections Connections grouped by target schema.
 */
void log_grouped_connections(
    int64_t node_id,
    const llvm::SmallDenseMap<llvm::StringRef,
                              llvm::SmallVector<GraphConnection, 4>, 4>&
        grouped_connections);

/**
 * @brief Projects an Arrow table down to the columns listed in a Select clause.
 *
 * @param select The SELECT clause (nullptr or empty means "all columns").
 * @param table The input table.
 * @return The projected table.
 */
std::shared_ptr<arrow::Table> apply_select(
    const std::shared_ptr<Select>& select,
    const std::shared_ptr<arrow::Table>& table);

/**
 * @brief Collects WHERE clauses that can be pushed down into a traversal step.
 *
 * @param target_var The variable name of the traversal target (e.g. "c").
 * @param i The clause index to start scanning from.
 * @param clauses All clauses in the query.
 * @return WHERE expressions that reference only @p target_var.
 */
std::vector<std::shared_ptr<WhereExpr>> get_where_to_inline(
    const std::string& target_var, size_t i,
    const std::vector<std::shared_ptr<Clause>>& clauses);

/**
 * @brief Applies inlined WHERE expressions to a table and updates query state.
 *
 * @param ref The schema reference whose table is being filtered.
 * @param table The table to filter.
 * @param query_state The execution state (IDs are updated after filtering).
 * @param where_exprs The WHERE expressions to apply.
 * @return The filtered table, or an error.
 */
arrow::Result<std::shared_ptr<arrow::Table>> inline_where(
    const SchemaRef& ref, std::shared_ptr<arrow::Table> table,
    QueryState& query_state,
    const std::vector<std::shared_ptr<WhereExpr>>& where_exprs);

/**
 * @brief Prepares a query for execution: registers aliases, resolves fields,
 * precomputes tags.
 *
 * @param query The query to prepare (modified in-place).
 * @param query_state The execution state to populate.
 * @return OK on success, or an error.
 */
arrow::Status prepare_query(Query& query, QueryState& query_state);

}  // namespace tundradb

#endif  // QUERY_EXECUTION_HPP
