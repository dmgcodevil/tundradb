#include "../include/core.hpp"

#include "../include/temporal_context.hpp"

using namespace tundradb;

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/datum.h>
#include <arrow/table.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/DenseSet.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>
#include <tbb/concurrent_unordered_set.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>

#include <chrono>
#include <future>
#include <iostream>
#include <list>
#include <memory>
#include <queue>
#include <ranges>
#include <stack>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "container_concepts.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

namespace tundradb {

constexpr static uint64_t NODE_MASK = (1ULL << 48) - 1;

// Deterministic 16-bit tag from alias string (SchemaRef::value()).
// https://www.ietf.org/archive/id/draft-eastlake-fnv-21.html
static uint16_t compute_tag(const SchemaRef& ref) {
  // FNV-1a 32-bit, then fold to 16 bits.
  const std::string& s = ref.value();
  uint32_t h = 2166136261u;
  for (unsigned char c : s) {
    h ^= c;
    h *= 16777619u;
  }
  h ^= (h >> 16);
  return static_cast<uint16_t>(h & 0xFFFFu);
}

/**
 * @brief Creates a packed 64-bit hash code for schema+node_id pairs
 *
 * This function combines a schema identifier and node ID into a single 64-bit
 * value for efficient storage and comparison in hash sets/maps. This eliminates
 * the need for expensive string concatenation and hashing that was previously
 * used for tracking visited nodes during graph traversal.
 *
 * @param schema The schema reference containing a pre-computed 16-bit tag
 * @param node_id The node identifier (48-bit max)
 *
 * @return A 64-bit packed value with layout:
 *         - Bits 63-48: Schema tag (16 bits)
 *         - Bits 47-0:  Node ID (48 bits, masked)
 *
 * @details
 * Memory Layout:
 * ```
 * 63    56    48    40    32    24    16     8     0
 * |  Schema  |           Node ID (48 bits)          |
 * | (16 bit) |                                       |
 * ```
 *
 * Performance Benefits:
 * - Replaces string operations: "User:12345" → single uint64_t
 * - Enables fast integer comparison instead of string hashing
 * - Reduces memory allocations (no temporary strings)
 * - Compatible with llvm::DenseSet for O(1) lookups
 *
 * Constraints:
 * - Node IDs must fit in 48 bits (max ~281 trillion nodes)
 * - Schema tags must be unique within query context
 * - NODE_MASK = (1ULL << 48) - 1 = 0x0000FFFFFFFFFFFF
 *
 * Example:
 * ```cpp
 * SchemaRef user_schema = SchemaRef::parse("u:User");
 * user_schema.set_tag(0x1234);  // Pre-computed schema tag
 *
 * uint64_t packed = hash_code_(user_schema, 98765);
 * // Result: 0x1234000000018149 (schema=0x1234, node=98765)
 *
 * // Usage in visited tracking:
 * llvm::DenseSet<uint64_t> visited;
 * visited.insert(packed);  // Fast O(1) integer hash
 * ```
 *
 * @see SchemaRef::tag() for schema tag computation
 * @see NODE_MASK constant definition
 */
static uint64_t hash_code_(const SchemaRef& schema, int64_t node_id) {
  const uint16_t schema_id16 = schema.tag();
  return (static_cast<uint64_t>(schema_id16) << 48) |
         (static_cast<uint64_t>(node_id) & NODE_MASK);
}

// Utility function to join containers using C++23 ranges
template <typename Container>
std::string join_container(const Container& container,
                           std::string_view delimiter = ", ") {
  return [&]() {
    std::ostringstream oss;
    for (auto it = container.begin(); it != container.end(); ++it) {
      oss << (it != container.begin() ? delimiter : "") << *it;
    }
    return oss.str();
  }();
}

// Convert Value to Arrow scalar for builders
arrow::Result<std::shared_ptr<arrow::Scalar>> value_to_arrow_scalar(
    const Value& value) {
  switch (value.type()) {
    case ValueType::INT32:
      return arrow::MakeScalar(value.as_int32());
    case ValueType::INT64:
      return arrow::MakeScalar(value.as_int64());
    case ValueType::DOUBLE:
      return arrow::MakeScalar(value.as_double());
    case ValueType::STRING:
      return arrow::MakeScalar(value.as_string());
    case ValueType::BOOL:
      return arrow::MakeScalar(value.as_bool());
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          tundradb::to_string(value.type()));
  }
}

arrow::Result<std::shared_ptr<arrow::Scalar>> value_ptr_to_arrow_scalar(
    const char* ptr, const ValueType type) {
  switch (type) {
    case ValueType::INT32:
      return arrow::MakeScalar(*reinterpret_cast<const int32_t*>(ptr));
    case ValueType::INT64:
      return arrow::MakeScalar(*reinterpret_cast<const int64_t*>(ptr));
    case ValueType::DOUBLE:
      return arrow::MakeScalar(*reinterpret_cast<const double*>(ptr));
    case ValueType::STRING: {
      auto str_ref = *reinterpret_cast<const StringRef*>(ptr);
      return arrow::MakeScalar(str_ref.to_string());
    }
    case ValueType::BOOL:
      return arrow::MakeScalar(*reinterpret_cast<const bool*>(ptr));
    case ValueType::NA:
      return arrow::MakeNullScalar(arrow::null());
    default:
      return arrow::Status::NotImplemented(
          "Unsupported Value type for Arrow scalar conversion: ",
          tundradb::to_string(type));
  }
}

// Recursively convert WhereExpr to Arrow compute expression
arrow::compute::Expression where_condition_to_expression(
    const WhereExpr& condition, bool strip_var) {
  // Use the unified to_arrow_expression() method from WhereExpr
  return condition.to_arrow_expression(strip_var);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_nodes(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Node>>& nodes) {
  auto arrow_schema = schema->arrow();
  IF_DEBUG_ENABLED {
    log_debug("Creating table from {} nodes with schema '{}'", nodes.size(),
              arrow_schema->ToString());
  }

  // Create builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (const auto& field : arrow_schema->fields()) {
    IF_DEBUG_ENABLED {
      log_debug("Creating builder for field '{}' with type {}", field->name(),
                field->type()->ToString());
    }
    auto builder_result = arrow::MakeBuilder(field->type());
    if (!builder_result.ok()) {
      log_error("Failed to create builder for field '{}': {}", field->name(),
                builder_result.status().ToString());
      return builder_result.status();
    }
    builders.push_back(std::move(builder_result.ValueOrDie()));
  }

  // Populate builders with data from each node
  IF_DEBUG_ENABLED {
    log_debug("Adding data from {} nodes to builders", nodes.size());
  }
  for (const auto& node : nodes) {
    // Create temporal view (nullptr = current version)
    auto view = node->view(nullptr);

    // Add each field's value to the appropriate builder
    for (int i = 0; i < schema->num_fields(); i++) {
      auto field = schema->field(i);
      const auto& field_name = field->name();

      // Find the value in the node's data
      auto res = view.get_value_ptr(field);
      if (res.ok()) {
        // Convert Value to Arrow scalar and append to builder
        auto value = res.ValueOrDie();
        if (value) {
          auto scalar_result = value_ptr_to_arrow_scalar(value, field->type());
          if (!scalar_result.ok()) {
            log_error("Failed to convert value to scalar for field '{}': {}",
                      field_name, scalar_result.status().ToString());
            return scalar_result.status();
          }

          const auto& scalar = scalar_result.ValueOrDie();
          auto status = builders[i]->AppendScalar(*scalar);
          if (!status.ok()) {
            log_error("Failed to append scalar for field '{}': {}", field_name,
                      status.ToString());
            return status;
          }
        } else {
          IF_DEBUG_ENABLED {
            log_debug("Null value for field '{}', appending null", field_name);
          }
          auto status = builders[i]->AppendNull();
          if (!status.ok()) {
            log_error("Failed to append null for field '{}': {}", field_name,
                      status.ToString());
            return status;
          }
        }
      } else {
        IF_DEBUG_ENABLED {
          log_debug("Field '{}' not found in node, appending null", field_name);
        }
        auto status = builders[i]->AppendNull();
        if (!status.ok()) {
          log_error("Failed to append null for field '{}': {}", field_name,
                    status.ToString());
          return status;
        }
      }
    }
  }

  // Finish building arrays
  IF_DEBUG_ENABLED { log_debug("Finalizing arrays from builders"); }
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());
  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    auto status = builder->Finish(&array);
    if (!status.ok()) {
      log_error("Failed to finish array builder: {}", status.ToString());
      return status;
    }
    arrays.push_back(array);
  }

  // Create table
  IF_DEBUG_ENABLED {
    log_debug("Creating table with {} rows and {} columns",
              arrays.empty() ? 0 : arrays[0]->length(), arrays.size());
  }
  return arrow::Table::Make(arrow_schema, arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> filter(
    const std::shared_ptr<arrow::Table>& table, const WhereExpr& condition,
    const bool strip_var) {
  IF_DEBUG_ENABLED {
    log_debug("Filtering table with WhereCondition: {}", condition.toString());
  }

  try {
    // Convert WhereCondition to Arrow compute expression
    const auto filter_expr =
        where_condition_to_expression(condition, strip_var);

    IF_DEBUG_ENABLED {
      log_debug("Creating in-memory dataset from table with {} rows",
                table->num_rows());
    }
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);

    // Create scanner builder
    IF_DEBUG_ENABLED { log_debug("Creating scanner builder"); }
    auto scan_builder_result = dataset->NewScan();
    if (!scan_builder_result.ok()) {
      log_error("Failed to create scanner builder: {}",
                scan_builder_result.status().ToString());
      return scan_builder_result.status();
    }
    const auto& scan_builder = scan_builder_result.ValueOrDie();

    IF_DEBUG_ENABLED {
      log_debug("Applying compound filter to scanner builder");
    }
    auto filter_status = scan_builder->Filter(filter_expr);
    if (!filter_status.ok()) {
      log_error("Failed to apply filter: {}", filter_status.ToString());
      return filter_status;
    }

    IF_DEBUG_ENABLED { log_debug("Finishing scanner"); }
    auto scanner_result = scan_builder->Finish();
    if (!scanner_result.ok()) {
      log_error("Failed to finish scanner: {}",
                scanner_result.status().ToString());
      return scanner_result.status();
    }
    const auto& scanner = scanner_result.ValueOrDie();

    IF_DEBUG_ENABLED { log_debug("Executing scan to table"); }
    auto table_result = scanner->ToTable();
    if (!table_result.ok()) {
      log_error("Failed to convert scan results to table: {}",
                table_result.status().ToString());
      return table_result.status();
    }

    auto result_table = table_result.ValueOrDie();
    IF_DEBUG_ENABLED {
      log_debug("Filter completed: {} rows in, {} rows out", table->num_rows(),
                result_table->num_rows());
    }
    return result_table;

  } catch (const std::exception& e) {
    log_error("Failed to convert WhereCondition to Arrow expression: {}",
              e.what());
    return arrow::Status::Invalid("Failed to convert WHERE condition: ",
                                  e.what());
  }
}

void debug_connections(
    int64_t id,
    const std::map<int64_t, std::vector<GraphConnection>>& connections,
    std::vector<std::string> path, std::vector<std::string>& res) {
  if (!connections.contains(id)) {
    res.push_back(join_container(path));
    return;
  }
  for (const auto& conn : connections.at(id)) {
    path.push_back(conn.toString());
    debug_connections(conn.target_id, connections, path, res);
  }
}

void print_paths(
    const std::map<int64_t, std::vector<GraphConnection>>& connections) {
  IF_DEBUG_ENABLED {
    log_debug("Printing all paths in connection graph:");

    if (connections.empty()) {
      log_debug("  No connections found");
      return;
    }

    for (const auto& [source_id, conn_list] : connections) {
      if (conn_list.empty()) {
        log_debug("  Node {} has no outgoing connections", source_id);
        continue;
      }

      for (const auto& conn : conn_list) {
        log_debug("  {} -[{}]-> {}", source_id, conn.edge_type, conn.target_id);
      }
    }

    log_debug("Total of {} source nodes with connections", connections.size());
  }
}

std::set<int64_t> get_roots(
    const std::map<int64_t, std::vector<GraphConnection>>& connections) {
  std::set<int64_t> roots;
  std::unordered_map<int64_t, int64_t> count;
  std::vector<int64_t> stack;
  for (const auto& id : connections | std::views::keys) {
    count[id] = 0;
    stack.push_back(id);
  }

  while (!stack.empty()) {
    auto curr = stack[stack.size() - 1];
    stack.pop_back();

    if (connections.contains(curr)) {
      for (auto const& next : connections.at(curr)) {
        count[next.target_id]++;
        stack.push_back(next.target_id);
      }
    }
  }
  for (const auto& [id, c] : count) {
    if (c == 0) {
      roots.insert(id);
    }
  }
  return roots;
}

struct QueryState {
  SchemaRef from;
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables;
  llvm::StringMap<llvm::DenseSet<int64_t>> ids;
  std::unordered_map<std::string, std::string> aliases;
  // Precomputed fully-qualified field names per alias (SchemaRef::value())
  llvm::StringMap<std::vector<std::string>> fq_field_names;

  // Field index optimization: replace string-based field lookups with integer
  // indices
  llvm::StringMap<std::vector<int>>
      schema_field_indices;  // "User" -> [0, 1, 2], "Company -> [3,4,5]"
  llvm::SmallDenseMap<int, std::string, 64>
      field_id_to_name;                      // 0 -> "user.name"
  llvm::StringMap<int> field_name_to_index;  // "user.name" -> 0
  std::atomic<int> next_field_id{0};         // Global field ID counter

  llvm::StringMap<
      llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>>>
      connections;  // outgoing
  llvm::DenseMap<int64_t, llvm::SmallVector<GraphConnection, 4>> incoming;

  std::shared_ptr<NodeManager> node_manager;
  std::shared_ptr<SchemaRegistry> schema_registry;
  std::vector<Traverse> traversals;

  // Temporal context for time-travel queries (nullptr = current version)
  std::unique_ptr<TemporalContext> temporal_context;

  // Connection object pooling to avoid repeated allocations
  class ConnectionPool {
   private:
    std::vector<GraphConnection> pool_;
    size_t next_index_ = 0;

   public:
    explicit ConnectionPool(size_t initial_size = 1000) : pool_(initial_size) {}

    GraphConnection& get() {
      if (next_index_ >= pool_.size()) {
        pool_.resize(pool_.size() * 2);  // Grow pool if needed
      }
      return pool_[next_index_++];
    }

    void reset() { next_index_ = 0; }  // Reset for reuse
    size_t size() const { return next_index_; }
  };

  mutable ConnectionPool connection_pool_;  // Mutable for const methods

  // Pre-size hash maps to avoid expensive resizing during query execution
  void reserve_capacity(const Query& query) {
    // Estimate schema count from FROM + TRAVERSE clauses
    size_t estimated_schemas = 1;  // FROM clause
    for (const auto& clause : query.clauses()) {
      if (clause->type() == Clause::Type::TRAVERSE) {
        estimated_schemas += 2;  // source + target schemas
      }
    }

    // Pre-size standard containers (LLVM containers don't support reserve)
    tables.reserve(estimated_schemas);
    aliases.reserve(estimated_schemas);

    // Estimate nodes per schema (conservative estimate)
    size_t estimated_nodes_per_schema = 1000;
    incoming.reserve(estimated_nodes_per_schema);

    // Pre-size field mappings
    field_id_to_name.reserve(estimated_schemas * 8);  // ~8 fields per schema
  }

  arrow::Result<std::string> resolve_schema(const SchemaRef& schema_ref) {
    // todo we need to separate functions: assign alias , resolve
    if (aliases.contains(schema_ref.value()) && schema_ref.is_declaration()) {
      IF_DEBUG_ENABLED {
        log_debug("duplicated schema alias '" + schema_ref.value() +
                  "' already assigned to '" + aliases[schema_ref.value()] +
                  "'");
      }
      return aliases[schema_ref.value()];
    }
    if (schema_ref.is_declaration()) {
      aliases[schema_ref.value()] = schema_ref.schema();
      return schema_ref.schema();
    }
    return aliases[schema_ref.value()];
  }

  // Precompute fully-qualified field names for source and target aliases
  arrow::Result<bool> compute_fully_qualified_names(
      const SchemaRef& schema_ref) {
    const auto it = aliases.find(schema_ref.value());
    if (it == aliases.end()) {
      return arrow::Status::KeyError("keyset does not contain alias '{}'",
                                     schema_ref.value());
    }
    return compute_fully_qualified_names(schema_ref, it->second);
  }

  // Precompute fully-qualified field names for source and target aliases
  arrow::Result<bool> compute_fully_qualified_names(
      const SchemaRef& schema_ref, const std::string& resolved_schema) {
    const std::string& alias = schema_ref.value();
    if (fq_field_names.contains(alias)) {
      return false;
    }
    auto schema_res = schema_registry->get(resolved_schema);
    if (!schema_res.ok()) {
      return schema_res.status();
    }
    const auto& schema = schema_res.ValueOrDie();
    std::vector<std::string> names;
    std::vector<int> indices;
    names.reserve(schema->num_fields());
    indices.reserve(schema->num_fields());

    for (const auto& f : schema->fields()) {
      std::string fq_name = alias + "." + f->name();
      int field_id = next_field_id.fetch_add(1);

      names.emplace_back(fq_name);
      indices.emplace_back(field_id);
      field_id_to_name[field_id] = fq_name;
      field_name_to_index[fq_name] = field_id;
    }

    fq_field_names[alias] = std::move(names);
    schema_field_indices[alias] = std::move(indices);
    return true;
  }

  const llvm::DenseSet<int64_t>& get_ids(const SchemaRef& schema_ref) {
    return ids[schema_ref.value()];
  }

  // removes node_id and updates all connections and ids
  void remove_node(int64_t node_id, const SchemaRef& schema_ref) {
    ids[schema_ref.value()].erase(node_id);
  }

  arrow::Result<bool> update_table(const std::shared_ptr<arrow::Table>& table,
                                   const SchemaRef& schema_ref) {
    this->tables[schema_ref.value()] = table;
    auto ids_result = get_ids_from_table(table);
    if (!ids_result.ok()) {
      log_error("Failed to get IDs from table: {}", schema_ref.value());
      return ids_result.status();
    }
    ids[schema_ref.value()] = ids_result.ValueOrDie();
    return true;
  }

  bool has_outgoing(const SchemaRef& schema_ref, int64_t node_id) const {
    return connections.contains(schema_ref.value()) &&
           connections.at(schema_ref.value()).contains(node_id) &&
           !connections.at(schema_ref.value()).at(node_id).empty();
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "QueryState {\n";
    ss << "  From: " << from.toString() << "\n";

    ss << "  Tables (" << tables.size() << "):\n";
    for (const auto& [alias, table_ptr] : tables) {
      if (table_ptr) {
        ss << "    - " << alias << ": " << table_ptr->num_rows() << " rows, "
           << table_ptr->num_columns() << " columns\n";
      } else {
        ss << "    - " << alias << ": (nullptr)\n";
      }
    }

    ss << "  IDs (" << ids.size() << "):\n";
    for (const auto& [alias, id_set] : ids) {
      ss << "    - " << alias.str() << ": " << id_set.size() << " IDs\n";
    }

    ss << "  Aliases (" << aliases.size() << "):\n";
    for (const auto& [alias, schema_name] : aliases) {
      ss << "    - " << alias << " -> " << schema_name << "\n";
    }

    ss << "  Connections (Outgoing) (" << connections.size()
       << " source nodes):";
    for (const auto& [from, conns] : connections) {
      for (const auto& [from_id, conn_vec] : conns) {
        ss << "from " << from.str() << ":" << from_id << ":\n";
        for (const auto& conn : conn_vec) {
          ss << "    - " << conn.target.value() << ":" << conn.target_id
             << "\n";
        }
      }
    }

    ss << "  Connections (Incoming) (" << incoming.size() << " target nodes):";
    int target_nodes_printed = 0;
    for (const auto& [target_id, conns_vec] : incoming) {
      if (target_nodes_printed >= 3 &&
          incoming.size() > 5) {  // Limit nodes printed
        ss << "      ... and " << (incoming.size() - target_nodes_printed)
           << " more target nodes ...\n";
        break;
      }
      ss << "    - Target ID " << target_id << " (" << conns_vec.size()
         << " incoming):";
      int conns_printed_for_target = 0;
      for (const auto& conn : conns_vec) {
        if (conns_printed_for_target >= 3 &&
            conns_vec.size() > 5) {  // Limit connections per node
          ss << "        ... and "
             << (conns_vec.size() - conns_printed_for_target)
             << " more connections ...\n";
          break;
        }
        ss << "        <- " << conn.source.value() << ":" << conn.source_id
           << " (via '" << conn.edge_type << "')\n";
        conns_printed_for_target++;
      }
      target_nodes_printed++;
    }

    ss << "  Traversals (" << traversals.size() << "):\n";
    for (size_t i = 0; i < traversals.size(); ++i) {
      const auto& trav = traversals[i];
      ss << "    - [" << i << "]: " << trav.source().value() << " -["
         << trav.edge_type() << "]-> " << trav.target().value() << " (Type: "
         << (trav.traverse_type() == TraverseType::Inner ? "Inner" : "Other")
         << ")\n";
    }

    ss << "}";
    return ss.str();
  }
};

arrow::Result<std::shared_ptr<arrow::Schema>> build_denormalized_schema(
    const QueryState& query_state) {
  IF_DEBUG_ENABLED { log_debug("Building schema for denormalized table"); }

  std::set<std::string> processed_fields;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::set<std::string> processed_schemas;

  // First add fields from the FROM schema
  std::string from_schema = query_state.from.value();

  IF_DEBUG_ENABLED {
    log_debug("Adding fields from FROM schema '{}'", from_schema);
  }

  auto schema_result =
      query_state.schema_registry->get(query_state.aliases.at(from_schema));
  if (!schema_result.ok()) {
    return schema_result.status();
  }

  auto schema = schema_result.ValueOrDie();
  auto arrow_schema = schema->arrow();
  for (const auto& field : arrow_schema->fields()) {
    std::string prefixed_field_name = from_schema + "." + field->name();
    processed_fields.insert(prefixed_field_name);
    fields.push_back(arrow::field(prefixed_field_name, field->type()));
  }
  processed_schemas.insert(from_schema);

  std::vector<SchemaRef> unique_schemas;
  for (const auto& traverse : query_state.traversals) {
    if (processed_schemas.insert(traverse.source().value()).second) {
      unique_schemas.push_back(traverse.source());
    }
    if (processed_schemas.insert(traverse.target().value()).second) {
      unique_schemas.push_back(traverse.target());
    }
  }

  for (const auto& schema_ref : unique_schemas) {
    IF_DEBUG_ENABLED {
      log_debug("Adding fields from schema '{}'", schema_ref.value());
    }

    schema_result = query_state.schema_registry->get(
        query_state.aliases.at(schema_ref.value()));
    if (!schema_result.ok()) {
      return schema_result.status();
    }

    arrow_schema = schema_result.ValueOrDie()->arrow();
    for (const auto& field : arrow_schema->fields()) {
      std::string prefixed_field_name =
          schema_ref.value() + "." + field->name();
      if (processed_fields.contains(prefixed_field_name)) {
        return arrow::Status::KeyError("Field '{}' already exists",
                                       prefixed_field_name);
      }

      processed_fields.insert(prefixed_field_name);
      fields.push_back(arrow::field(prefixed_field_name, field->type()));
    }
  }

  return std::make_shared<arrow::Schema>(fields);
}

struct PathSegment {
  uint16_t schema_tag;  // Optimized: use 16-bit tag instead of string
  std::string schema;   // Keep for compatibility/debugging
  int64_t node_id;

  // Constructor with both tag and schema for performance
  PathSegment(uint16_t tag, const std::string& schema_name, int64_t id)
      : schema_tag(tag), schema(schema_name), node_id(id) {}

  // Legacy constructor for compatibility
  PathSegment(const std::string& schema_name, int64_t id)
      : schema_tag(0), schema(schema_name), node_id(id) {}

  std::string toString() const {
    return schema + ":" + std::to_string(node_id);
  }

  bool operator==(const PathSegment& other) const {
    // Fast path: compare tags first, then fallback to string comparison
    if (schema_tag != 0 && other.schema_tag != 0) {
      return schema_tag == other.schema_tag && node_id == other.node_id;
    }
    return schema == other.schema && node_id == other.node_id;
  }
};

bool is_prefix(const std::vector<PathSegment>& prefix,
               const std::vector<PathSegment>& path) {
  if (prefix.size() > path.size()) {
    return false;
  }
  int i = 0;
  while (i < prefix.size()) {
    if (prefix[i] != path[i]) return false;
    i++;
  }
  return true;
}

std::string join_schema_path(const std::vector<PathSegment>& schema_path) {
  std::ostringstream oss;
  for (size_t i = 0; i < schema_path.size(); ++i) {
    if (i != 0) oss << "->";
    oss << schema_path[i].toString();
  }
  return oss.str();
}

struct Row {
  int64_t id;
  std::vector<ValueRef> cells;  // Optimized: index-based field access
  std::vector<PathSegment> path;
  std::unordered_map<std::string, int64_t> ids;
  bool ids_populated = false;

  // Optimized constructor that pre-allocates cells
  explicit Row(size_t max_field_count = 64) : cells(max_field_count) {}

  // Optimized: set cell by field index
  void set_cell(int field_id, ValueRef value_ref) {
    if (field_id >= 0 && field_id < static_cast<int>(cells.size())) {
      cells[field_id] = value_ref;
    }
  }

  // Optimized: check value by field index
  [[nodiscard]] bool has_value(int field_id) const {
    return field_id >= 0 && field_id < static_cast<int>(cells.size()) &&
           cells[field_id].data != nullptr;
  }

  // Optimized: set cells from node using field indices
  void set_cell_from_node(const std::vector<int>& field_indices,
                          const std::shared_ptr<Node>& node,
                          TemporalContext* temporal_context = nullptr) {
    // Create temporal view (nullptr = current version)
    auto view = node->view(temporal_context);

    const auto& fields = node->get_schema()->fields();
    const size_t n = std::min(fields.size(), field_indices.size());
    for (size_t i = 0; i < n; ++i) {
      const auto& field = fields[i];
      const int field_id = field_indices[i];
      auto value_ref_result = view.get_value_ref(field);
      if (value_ref_result.ok()) {
        this->set_cell(field_id, value_ref_result.ValueOrDie());
      }
    }
  }

  [[nodiscard]] bool start_with(const std::vector<PathSegment>& prefix) const {
    return is_prefix(prefix, this->path);
  }

  const std::unordered_map<std::string, int64_t>& extract_schema_ids(
      const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name) {
    if (ids_populated) {
      return ids;
    }
    for (int i = 0; i < cells.size(); ++i) {
      const auto& value = cells[i];
      if (!value.data) continue;
      const auto& field_name = field_id_to_name.at(i);
      // Extract schema prefix (everything before the first dot)
      size_t dot_pos = field_name.find('.');
      if (dot_pos != std::string::npos) {
        std::string schema = field_name.substr(0, dot_pos);
        // Store ID for this schema if it's an ID field
        if (field_name.substr(dot_pos + 1) == "id") {
          ids[schema] = value.as_int64();
        }
      }
    }
    return ids;
  }

  // returns new Row which is result of merging this row and other
  [[nodiscard]] std::shared_ptr<Row> merge(
      const std::shared_ptr<Row>& other) const {
    std::shared_ptr<Row> merged =
        std::make_shared<Row>(*this);  // Copy needed for merge result
    IF_DEBUG_ENABLED {
      log_debug("Row::merge() - this: {}", this->ToString());
      log_debug("Row::merge() - other: {}", other->ToString());
    }

    for (int i = 0; i < other->cells.size(); ++i) {
      if (!merged->has_value(i)) {
        IF_DEBUG_ENABLED {
          log_debug("Row::merge() - adding field '{}' with value: {}", i,
                    cells[i].ToString());
        }
        merged->cells[i] = other->cells[i];
      } else {
        IF_DEBUG_ENABLED {
          log_debug("Row::merge() - skipping field '{}' (already has value)",
                    i);
        }
      }
    }
    IF_DEBUG_ENABLED {
      log_debug("Row::merge() - result: {}", merged->ToString());
    }
    return merged;
  }

  [[nodiscard]] std::string ToString() const {
    std::stringstream ss;
    ss << "Row{";
    ss << "path='" << join_schema_path(path) << "', ";

    bool first = true;
    for (int i = 0; i < cells.size(); i++) {
      if (!first) {
        ss << ", ";
      }
      first = false;

      ss << i << ": ";
      const auto value_ref = cells[i];
      if (!value_ref.data) {
        ss << "NULL";
      } else {
        // Handle different scalar types appropriately
        switch (value_ref.type) {
          case ValueType::INT64:
            ss << value_ref.as_int64();
            break;
          case ValueType::INT32:
            ss << value_ref.as_int32();
            break;
          case ValueType::DOUBLE:
            ss << value_ref.as_double();
            break;
          case ValueType::STRING:
            ss << "\"" << value_ref.as_string_ref().to_string() << "\"";
            break;
          case ValueType::BOOL:
            ss << (value_ref.as_bool() ? "true" : "false");
            break;
          default:
            ss << "unknown";
            break;
        }
      }
    }
    ss << "}";
    return ss.str();
  }
};

static Row create_empty_row_from_schema(
    const std::shared_ptr<arrow::Schema>& final_output_schema) {
  Row new_row(final_output_schema->num_fields() +
              32);  // Pre-allocate with some extra space
  new_row.id = -1;
  return new_row;
}

struct RowNode;

std::vector<Row> get_child_rows(const Row& parent,
                                const std::vector<Row>& rows) {
  std::vector<Row> child;
  for (const auto& row : rows) {
    if (parent.id != row.id && row.start_with(parent.path)) {
      child.push_back(row);
    }
  }
  return child;
}

struct RowNode {
  std::optional<std::shared_ptr<Row>> row;
  int depth;
  PathSegment path_segment;
  std::vector<std::unique_ptr<RowNode>> children;

  RowNode() : depth(0), path_segment{"", -1} {}

  RowNode(std::optional<std::shared_ptr<Row>> r, int d,
          std::vector<std::unique_ptr<RowNode>> c = {})
      : row(std::move(r)),
        depth(d),
        path_segment{"", -1},
        children(std::move(c)) {}

  bool leaf() const { return row.has_value(); }

  void insert_row_dfs(size_t path_idx, const std::shared_ptr<Row>& new_row) {
    if (path_idx == new_row->path.size()) {
      this->row =
          new_row;  // Share the same Row - no copy needed in tree insertion
      return;
    }

    for (const auto& n : children) {
      if (n->path_segment == new_row->path[path_idx]) {
        n->insert_row_dfs(path_idx + 1, new_row);
        return;
      }
    }

    auto new_node = std::make_unique<RowNode>();
    new_node->depth = depth + 1;
    new_node->path_segment = new_row->path[path_idx];
    new_node->insert_row_dfs(path_idx + 1, new_row);
    children.emplace_back(std::move(new_node));
  }

  void insert_row(const std::shared_ptr<Row>& new_row) {
    insert_row_dfs(0, new_row);
  }

  std::vector<std::shared_ptr<Row>> merge_rows(
      const llvm::SmallDenseMap<int, std::string, 64>& field_id_to_name) {
    if (this->leaf()) {
      return {this->row.value()};
    }

    // collect all records from child node and group them by schema
    // Optimized: use schema tags instead of strings for faster grouping
    llvm::SmallDenseMap<uint16_t, std::vector<std::shared_ptr<Row>>, 8> grouped;
    for (const auto& c : children) {
      auto child_rows = c->merge_rows(field_id_to_name);
      IF_DEBUG_ENABLED {
        log_debug("Child node {} returned {} rows", c->path_segment.toString(),
                  child_rows.size());
        for (size_t i = 0; i < child_rows.size(); ++i) {
          log_debug("  Child row [{}]: {}", i, child_rows[i]->ToString());
        }
      }
      // Use schema_tag for fast integer-based grouping instead of string lookup
      uint16_t tag = c->path_segment.schema_tag;
      if (tag == 0) {
        // Fallback: compute a simple hash of the schema string
        tag = static_cast<uint16_t>(
            std::hash<std::string>{}(c->path_segment.schema) & 0xFFFFu);
      }
      grouped[tag].insert(grouped[tag].end(), child_rows.begin(),
                          child_rows.end());
    }

    std::vector<std::vector<std::shared_ptr<Row>>> groups_for_product;
    // Add this->row as its own group (that is important for cartesian product)
    // if it exists and has data,
    // to represent the node itself if it should be part of the product
    // independently.
    if (this->row.has_value()) {
      std::shared_ptr<Row> node_self_row = std::make_shared<Row>(
          *this->row.value());  // Create a copy like original
      // Normalize path for the node's own row to ensure it combines correctly
      // and doesn't carry a longer BFS path if it was a leaf of BFS.
      // i.e. current node path can be a:0->b:1->c:2
      // this code sets it to 'c:2'
      node_self_row->path = {this->path_segment};
      IF_DEBUG_ENABLED {
        log_debug("Adding node self row: {}", node_self_row->ToString());
      }
      groups_for_product.push_back({node_self_row});
    }

    for (const auto& pair : grouped) {
      if (!pair.second.empty()) {
        IF_DEBUG_ENABLED {
          log_debug("Adding group for schema '{}' with {} rows", pair.first,
                    pair.second.size());
          for (size_t i = 0; i < pair.second.size(); ++i) {
            log_debug("  Group row [{}]: {}", i, pair.second[i]->ToString());
          }
        }
        groups_for_product.push_back(pair.second);
      }
    }

    IF_DEBUG_ENABLED {
      log_debug("Total groups for Cartesian product: {}",
                groups_for_product.size());
    }

    if (groups_for_product.empty()) {
      return {};
    }
    // If only one group (e.g. only this->row.value() or only one child branch
    // with data), no Cartesian product is needed. Just return its rows, but
    // ensure paths are correct.
    if (groups_for_product.size() == 1) {
      std::vector<std::shared_ptr<Row>> single_group_rows =
          groups_for_product[0];
      // Ensure path is normalized for these rows if they came from children
      // For rows that are just this->row.value(), path is already set.
      // This might be too aggressive if child rows are already fully merged
      // products. For now, let's assume rows from c->merge_rows() are final
      // products of that child branch.
      return single_group_rows;
    }

    std::vector<std::shared_ptr<Row>> final_merged_rows =
        groups_for_product.back();
    IF_DEBUG_ENABLED {
      log_debug("Starting Cartesian product with final group ({} rows)",
                final_merged_rows.size());
      for (size_t i = 0; i < final_merged_rows.size(); ++i) {
        log_debug("  Final group row [{}]: {}", i,
                  final_merged_rows[i]->ToString());
      }
    }

    for (int i = static_cast<int>(groups_for_product.size()) - 2; i >= 0; --i) {
      IF_DEBUG_ENABLED {
        log_debug("Processing group {} with {} rows", i,
                  groups_for_product[i].size());
        for (size_t j = 0; j < groups_for_product[i].size(); ++j) {
          log_debug("  Current group row [{}]: {}", j,
                    groups_for_product[i][j]->ToString());
        }
      }

      std::vector<std::shared_ptr<Row>> temp_product_accumulator;
      for (const auto& r1_from_current_group : groups_for_product[i]) {
        for (const auto& r2_from_previous_product : final_merged_rows) {
          // Check for conflicts in shared variables between rows
          bool can_merge = true;

          // Get variable prefixes (schema names) from cells
          std::unordered_map<std::string, int64_t> schema_ids_r1 =
              r1_from_current_group->extract_schema_ids(field_id_to_name);
          std::unordered_map<std::string, int64_t> schema_ids_r2 =
              r2_from_previous_product->extract_schema_ids(field_id_to_name);

          // Check for conflicts - same schema name but different IDs
          for (const auto& [schema, id1] : schema_ids_r1) {
            if (schema_ids_r2.contains(schema) &&
                schema_ids_r2[schema] != id1) {
              // Found a conflict - same schema but different IDs
              IF_DEBUG_ENABLED {
                log_debug(
                    "Conflict detected: Schema '{}' has different IDs: {} vs "
                    "{}",
                    schema, id1, schema_ids_r2[schema]);
              }
              can_merge = false;
              break;
            }
          }

          // Additional cell-by-cell check for conflicts
          if (can_merge) {
            for (int field_index = 0;
                 field_index < r1_from_current_group->cells.size();
                 ++field_index) {
              if (r1_from_current_group->has_value(i) &&
                  r2_from_previous_product->has_value(i)) {
                // Both rows have this field with non-null values - check if
                // they match
                if (!r1_from_current_group->cells[i].equals(
                        r2_from_previous_product->cells[i])) {
                  IF_DEBUG_ENABLED {
                    log_debug(
                        "Conflict detected: Field '{}' has different values",
                        field_id_to_name.at(i));
                  }
                  can_merge = false;
                  break;
                }
              }
            }
          }

          if (can_merge) {
            std::shared_ptr<Row> merged_r =
                r1_from_current_group->merge(r2_from_previous_product);
            // Set the path of the newly merged row to the path of the current
            // RowNode
            merged_r->path = {this->path_segment};
            IF_DEBUG_ENABLED {
              log_debug("Merged row: {}", merged_r->ToString());
            }
            temp_product_accumulator.push_back(merged_r);
          } else {
            IF_DEBUG_ENABLED {
              log_debug("Cannot merge rows due to conflicts");
            }
          }
        }
      }
      final_merged_rows = std::move(temp_product_accumulator);
      if (final_merged_rows.empty()) {
        IF_DEBUG_ENABLED {
          log_debug("product_accumulator is empty. stop merge");
        }
        break;
      }
    }
    return final_merged_rows;
  }

  std::string toString(bool recursive = true, int indent_level = 0) const {
    // Helper to build indentation string based on level
    auto get_indent = [](int level) { return std::string(level * 2, ' '); };

    std::stringstream ss;
    std::string indent = get_indent(indent_level);

    // Print basic node info
    ss << indent << "RowNode [path=" << path_segment.toString()
       << ", depth=" << depth << "] {\n";

    // Print Row
    if (row.has_value()) {
      ss << indent << "  Path: ";
      if (row.value()->path.empty()) {
        ss << "(empty)";
      } else {
        for (size_t i = 0; i < row.value()->path.size(); ++i) {
          if (i > 0) ss << " → ";
          ss << row.value()->path[i].schema << ":"
             << row.value()->path[i].node_id;
        }
      }
      ss << "\n";

      // Print key cell values (limited to avoid overwhelming output)
      ss << indent << "  Cells: ";
      if (row.value()->cells.empty()) {
        ss << "(empty)";
      } else {
        size_t count = 0;
        ss << "{ ";
        for (int i = 0; i < row.value()->cells.size(); i++) {
          if (count++ > 0) ss << ", ";
          if (count > 5) {  // Limit display
            ss << "... +" << (row.value()->cells.size() - 5) << " more";
            break;
          }

          ss << i << ": ";
          if (!row.value()->has_value(i)) {
            ss << "NULL";
          } else {
            ss << row.value()->cells[i].ToString();
          }
        }
        ss << " }";
      }
    }

    ss << "\n";

    // Print children count
    ss << indent << "  Children: " << children.size() << "\n";

    // Recursively print children if requested
    if (recursive && !children.empty()) {
      ss << indent << "  [\n";
      for (const auto& child : children) {
        if (child) {
          ss << child->toString(true, indent_level + 2);
        } else {
          ss << get_indent(indent_level + 2) << "(null child)\n";
        }
      }
      ss << indent << "  ]\n";
    }

    ss << indent << "}\n";
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& os, const RowNode& node) {
    return os << node.toString();
  }

  void print(const bool recursive = true) const {
    log_debug(toString(recursive));
  }
};

struct QueueItem {
  int64_t node_id;
  SchemaRef schema_ref;
  int level;
  std::shared_ptr<Row> row;
  llvm::SmallDenseSet<uint64_t, 8>
      path_visited_nodes;  // packed (schema_id<<48 | node_id) for this path
  std::vector<PathSegment> path;

  QueueItem(int64_t id, const SchemaRef& schema, int l, std::shared_ptr<Row> r)
      : node_id(id), schema_ref(schema), level(l), row(std::move(r)) {
    path.emplace_back(schema.tag(), schema.value(), id);
  }
};

// Log grouped connections for a node
void log_grouped_connections(
    int64_t node_id,
    const llvm::SmallDenseMap<llvm::StringRef,
                              llvm::SmallVector<GraphConnection, 4>, 4>&
        grouped_connections) {
  IF_DEBUG_ENABLED {
    if (grouped_connections.empty()) {
      log_debug("Node {} has no grouped connections", node_id);
      return;
    }

    log_debug("Node {} has connections to {} target schemas:", node_id,
              grouped_connections.size());

    for (const auto& it : grouped_connections) {
      auto target_schema = it.first;
      const auto& connections = it.second;
      log_debug("  To schema '{}': {} connections", target_schema.str(),
                connections.size());

      for (size_t i = 0; i < connections.size(); ++i) {
        const auto& conn = connections[i];
        log_debug("    [{}] {} -[{}]-> {}.{} (target_id: {})", i,
                  conn.source.value(), conn.edge_type, conn.target.value(),
                  conn.target.schema(), conn.target_id);
      }
    }
  }
}

arrow::Result<std::shared_ptr<std::vector<std::shared_ptr<Row>>>>
populate_rows_bfs(int64_t node_id, const SchemaRef& start_schema,
                  const std::shared_ptr<arrow::Schema>& output_schema,
                  const QueryState& query_state,
                  llvm::DenseSet<uint64_t>& global_visited) {
  IF_DEBUG_ENABLED {
    log_debug("populate_rows_bfs::node={}:{}", start_schema.value(), node_id);
  }
  auto result = std::make_shared<std::vector<std::shared_ptr<Row>>>();
  int64_t row_id_counter = 0;
  auto initial_row =
      std::make_shared<Row>(create_empty_row_from_schema(output_schema));

  std::queue<QueueItem> queue;
  queue.emplace(node_id, start_schema, 0, initial_row);
  // Use precomputed fully-qualified field names from QueryState

  while (!queue.empty()) {
    auto size = queue.size();
    while (size-- > 0) {
      auto item = queue.front();
      queue.pop();
      auto node = query_state.node_manager->get_node(item.node_id).ValueOrDie();
      const auto& it_fq =
          query_state.schema_field_indices.find(item.schema_ref.value());
      if (it_fq == query_state.schema_field_indices.end()) {
        log_error("No fully-qualified field names for schema '{}'",
                  item.schema_ref.value());
        return arrow::Status::KeyError(
            "Missing precomputed fq_field_names for alias {}",
            item.schema_ref.value());
      }
      item.row->set_cell_from_node(it_fq->second, node,
                                   query_state.temporal_context.get());
      const uint64_t packed = hash_code_(item.schema_ref, item.node_id);
      global_visited.insert(packed);
      item.path_visited_nodes.insert(packed);

      // group connections by target schema (small, stack-friendly)
      llvm::SmallDenseMap<llvm::StringRef,
                          llvm::SmallVector<GraphConnection, 4>, 4>
          grouped_connections;

      bool skip = false;
      if (query_state.has_outgoing(item.schema_ref, item.node_id)) {
        for (const auto& conn :
             query_state.connections.at(item.schema_ref.value())
                 .at(item.node_id)) {
          const uint64_t tgt_packed = hash_code_(conn.target, conn.target_id);
          if (!item.path_visited_nodes.contains(tgt_packed)) {
            if (query_state.ids.at(conn.target.value())
                    .contains(conn.target_id)) {
              grouped_connections[conn.target.value()].push_back(conn);
            } else {
              skip = true;
            }
          }
        }
      }
      log_grouped_connections(item.node_id, grouped_connections);

      if (grouped_connections.empty()) {
        // we've done
        if (!skip) {
          auto r = std::make_shared<Row>(
              *item.row);  // Copy needed: each result needs unique ID and path
          r->path = item.path;
          r->id = row_id_counter++;
          IF_DEBUG_ENABLED { log_debug("add row: {}", r->ToString()); }
          result->push_back(r);
        }

      } else {
        for (const auto& pair : grouped_connections) {
          const auto& connections = pair.second;
          if (connections.size() == 1) {
            // continue the path
            auto conn = connections[0];
            auto next =
                QueueItem(connections[0].target_id, connections[0].target,
                          item.level + 1, item.row);

            next.path = item.path;
            next.path.emplace_back(connections[0].target.value(),
                                   connections[0].target_id);
            IF_DEBUG_ENABLED {
              log_debug("continue the path: {}", join_schema_path(next.path));
            }
            queue.push(next);
          } else {
            for (const auto& conn : connections) {
              auto next_row = std::make_shared<Row>(*item.row);
              auto next = QueueItem(conn.target_id, conn.target, item.level + 1,
                                    next_row);
              next.path = item.path;
              next.path.push_back(PathSegment{
                  conn.target.tag(), conn.target.value(), conn.target_id});
              IF_DEBUG_ENABLED {
                log_debug("create a new path {}, node={}",
                          join_schema_path(next.path), conn.target_id);
              }
              queue.push(next);
            }
          }
        }
      }
    }
  }
  RowNode tree;
  tree.path_segment = PathSegment{0, "root", -1};  // Use tag 0 for root
  for (const auto& r : *result) {
    IF_DEBUG_ENABLED { log_debug("bfs result: {}", r->ToString()); }
    // Copy needed: tree merge operations will modify rows, so each needs to be
    // independent
    auto r_copy = std::make_shared<Row>(*r);
    tree.insert_row(r_copy);
  }
  IF_DEBUG_ENABLED { tree.print(); }
  auto merged = tree.merge_rows(query_state.field_id_to_name);
  IF_DEBUG_ENABLED {
    for (const auto& row : merged) {
      log_debug("merge result: {}", row->ToString());
    }
  }
  return std::make_shared<std::vector<std::shared_ptr<Row>>>(merged);
}

// template <NodeIds NodeIdsT>
arrow::Result<std::shared_ptr<std::vector<std::shared_ptr<Row>>>>
populate_batch_rows(const llvm::DenseSet<int64_t>& node_ids,
                    const SchemaRef& schema_ref,
                    const std::shared_ptr<arrow::Schema>& output_schema,
                    const QueryState& query_state, const TraverseType join_type,
                    tbb::concurrent_unordered_set<uint64_t>& global_visited) {
  auto rows = std::make_shared<std::vector<std::shared_ptr<Row>>>();
  rows->reserve(node_ids.size());
  llvm::DenseSet<uint64_t> local_visited;
  // For INNER join: only process nodes that have connections
  // For LEFT join: process all nodes from the "left" side
  for (const auto node_id : node_ids) {
    if (!global_visited.insert(hash_code_(schema_ref, node_id)).second) {
      // Skip if already processed in an earlier traversal
      continue;
    }

    // For INNER JOIN: Skip nodes without connections
    if (join_type == TraverseType::Inner &&
        !query_state.has_outgoing(schema_ref, node_id)) {
      continue;
    }

    auto res = populate_rows_bfs(node_id, schema_ref, output_schema,
                                 query_state, local_visited);
    if (!res.ok()) {
      log_error("Failed to populate rows for node {} in schema '{}': {}",
                node_id, schema_ref.value(), res.status().ToString());
      return res.status();
    }

    const auto& res_value = res.ValueOrDie();
    rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                 std::make_move_iterator(res_value->end()));
  }
  global_visited.insert(local_visited.begin(), local_visited.end());
  return rows;
}

std::vector<llvm::DenseSet<int64_t>> batch_node_ids(
    const llvm::DenseSet<int64_t>& ids, const size_t batch_size) {
  std::vector<llvm::DenseSet<int64_t>> batches;
  batches.reserve(ids.size() / batch_size + 1);
  llvm::DenseSet<int64_t> current_batch;
  current_batch.reserve(batch_size);

  for (const auto& id : ids) {
    current_batch.insert(id);

    if (current_batch.size() >= batch_size) {
      batches.push_back(std::move(current_batch));
      current_batch.clear();
      current_batch.reserve(batch_size);
    }
  }

  if (!current_batch.empty()) {
    batches.push_back(std::move(current_batch));
  }

  return batches;
}

// process all schemas used in traverse
// Phase 1: Process connected nodes
// Phase 2: Handle outer joins for unmatched nodes
arrow::Result<std::shared_ptr<std::vector<std::shared_ptr<Row>>>> populate_rows(
    const ExecutionConfig& execution_config, const QueryState& query_state,
    const std::vector<Traverse>& traverses,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  auto rows = std::make_shared<std::vector<std::shared_ptr<Row>>>();
  std::mutex rows_mtx;
  tbb::concurrent_unordered_set<uint64_t> global_visited;

  // Map schemas to their join types
  std::unordered_map<std::string, TraverseType> schema_join_types;
  schema_join_types.reserve(traverses.size());
  if (traverses.empty()) {
    schema_join_types[query_state.from.value()] = TraverseType::Left;
  } else {
    // FROM is always inner by default
    schema_join_types[query_state.from.value()] = TraverseType::Inner;
  }

  // Only apply LEFT JOIN to FROM schema if the FROM schema is directly involved
  // in a LEFT JOIN traversal
  for (const auto& traverse : traverses) {
    if (traverse.source().value() == query_state.from.value() &&
        (traverse.traverse_type() == TraverseType::Left ||
         traverse.traverse_type() == TraverseType::Full)) {
      schema_join_types[query_state.from.value()] = traverse.traverse_type();
      break;
    }
  }

  // Build ordered list of schema references to process
  std::vector<SchemaRef> ordered_schemas;
  ordered_schemas.push_back(query_state.from);

  // Add schemas from traversals in order
  for (const auto& traverse : traverses) {
    // Update join type for the target schema
    schema_join_types[traverse.target().value()] = traverse.traverse_type();

    // Add target schema to the ordered list if not already present
    if (std::ranges::find_if(ordered_schemas, [&](const SchemaRef& sr) {
          return sr.value() == traverse.target().value();
        }) == ordered_schemas.end()) {
      ordered_schemas.push_back(traverse.target());
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Processing {} schemas with their respective join types",
              ordered_schemas.size());
  }

  // Process each schema in order
  for (const auto& schema_ref : ordered_schemas) {
    TraverseType join_type = schema_join_types[schema_ref.value()];
    IF_DEBUG_ENABLED {
      log_debug("Processing schema '{}' with join type {}", schema_ref.value(),
                static_cast<int>(join_type));
    }

    if (!query_state.ids.contains(schema_ref.value())) {
      log_warn("Schema '{}' not found in query state IDs", schema_ref.value());
      continue;
    }

    // Get all nodes for this schema
    const auto& schema_nodes = query_state.ids.at(schema_ref.value());
    std::vector<std::vector<int64_t>> batch_ids;
    if (execution_config.parallel_enabled) {
      size_t batch_size = 0;
      if (execution_config.parallel_batch_size > 0) {
        batch_size = execution_config.parallel_batch_size;
      } else {
        batch_size = execution_config.calculate_batch_size(schema_nodes.size());
      }
      auto batches = batch_node_ids(schema_nodes, batch_size);
      IF_DEBUG_ENABLED {
        log_debug(
            "process concurrently. thread_count={}, batch_size={}, "
            "batches_count={}",
            execution_config.parallel_thread_count, batch_size, batches.size());
      }
      tbb::task_arena arena(execution_config.parallel_thread_count);
      std::atomic error_occurred{false};
      std::string error_message;
      std::mutex error_mutex;
      arena.execute([&] {
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, batches.size()),
            [&](const tbb::blocked_range<size_t>& range) {
              for (size_t i = range.begin(); i != range.end(); ++i) {
                if (error_occurred.load()) {
                  return;  // Early exit from this thread
                }
                auto res =
                    populate_batch_rows(batches[i], schema_ref, output_schema,
                                        query_state, join_type, global_visited);
                if (!res.ok()) {
                  error_occurred.store(true);
                  std::lock_guard lock(error_mutex);
                  if (error_message.empty()) {  // First error wins
                    error_message = res.status().ToString();
                  }
                  return;
                }
                const auto& batch_rows = res.ValueOrDie();
                std::lock_guard lock(rows_mtx);
                rows->insert(rows->end(), batch_rows->begin(),
                             batch_rows->end());
              }
            });
      });
      if (error_occurred.load()) {
        return arrow::Status::ExecutionError(
            "Parallel batch processing failed: " + error_message);
      }

    } else {
      auto res = populate_batch_rows(schema_nodes, schema_ref, output_schema,
                                     query_state, join_type, global_visited);
      if (!res.ok()) {
        return res.status();
      }
      const auto& res_value = res.ValueOrDie();
      rows->insert(rows->end(), std::make_move_iterator(res_value->begin()),
                   std::make_move_iterator(res_value->end()));
    }

    IF_DEBUG_ENABLED {
      log_debug("Processing schema '{}' nodes: [{}]", schema_ref.value(),
                join_container(schema_nodes));
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Generated {} total rows after processing all schemas",
              rows->size());
  }
  return rows;
}

arrow::Result<std::shared_ptr<arrow::Table>> create_empty_table(
    const std::shared_ptr<arrow::Schema>& schema) {
  // Create empty arrays for each field in the schema
  std::vector<std::shared_ptr<arrow::Array>> empty_arrays;

  for (const auto& field : schema->fields()) {
    std::shared_ptr<arrow::Array> empty_array;

    switch (field->type()->id()) {
      case arrow::Type::INT64: {
        arrow::Int64Builder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::STRING: {
        arrow::StringBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::DOUBLE: {
        arrow::DoubleBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      case arrow::Type::BOOL: {
        arrow::BooleanBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Finish(&empty_array));
        break;
      }
      default:
        // For any other type, create a generic empty array
        empty_array = std::make_shared<arrow::NullArray>(0);
    }

    empty_arrays.push_back(empty_array);
  }

  // Create table from schema and empty arrays
  return arrow::Table::Make(schema, empty_arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> create_table_from_rows(
    const std::shared_ptr<std::vector<std::shared_ptr<Row>>>& rows,
    const std::shared_ptr<arrow::Schema>& output_schema) {
  if (output_schema == nullptr) {
    return arrow::Status::Invalid("output schema is null");
  }
  if (!rows || rows->empty()) {
    return create_empty_table(output_schema);
  }

  // Create array builders for each field
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  std::vector<std::string>
      field_names;  // Cache field names to avoid repeated lookups

  for (const auto& field : output_schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder, arrow::MakeBuilder(field->type()));
    builders.push_back(std::move(builder));
    field_names.push_back(field->name());
  }

  // Pre-allocate builders for better performance
  const size_t num_rows = rows->size();
  for (auto& builder : builders) {
    ARROW_RETURN_NOT_OK(builder->Reserve(num_rows));
  }

  // Populate the builders from each row
  for (const auto& row : *rows) {
    for (size_t i = 0; i < field_names.size(); i++) {
      const auto& field_name = field_names[i];  // Use cached field name

      // Optimization: try indexed access first, fallback to string lookup
      ValueRef value_ref;
      bool has_value = false;

      if (i < row->cells.size() && row->cells[i].data != nullptr) {
        value_ref = row->cells[i];
        has_value = true;
      }

      if (has_value) {
        // We have a value for this field - append directly without creating
        // scalars
        arrow::Status append_status;

        switch (value_ref.type) {
          case ValueType::INT32:
            append_status = static_cast<arrow::Int32Builder*>(builders[i].get())
                                ->Append(value_ref.as_int32());
            break;
          case ValueType::INT64:
            append_status = static_cast<arrow::Int64Builder*>(builders[i].get())
                                ->Append(value_ref.as_int64());
            break;
          case ValueType::DOUBLE:
            append_status =
                static_cast<arrow::DoubleBuilder*>(builders[i].get())
                    ->Append(value_ref.as_double());
            break;
          case ValueType::STRING: {
            const auto& str_ref = value_ref.as_string_ref();
            append_status =
                static_cast<arrow::StringBuilder*>(builders[i].get())
                    ->Append(str_ref.data(), str_ref.length());
            break;
          }
          case ValueType::BOOL:
            append_status =
                static_cast<arrow::BooleanBuilder*>(builders[i].get())
                    ->Append(value_ref.as_bool());
            break;
          default:
            append_status = builders[i]->AppendNull();
            break;
        }

        if (append_status.ok()) {
          continue;
        }
      }

      // Fall back to NULL if we couldn't append the value
      ARROW_RETURN_NOT_OK(builders[i]->AppendNull());
    }
  }

  // Finish building the arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());

  for (const auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder->Finish(&array));
    arrays.push_back(array);
  }

  // Create and return the table
  return arrow::Table::Make(output_schema, arrays);
}

// Function to project a table based on Select clause
std::shared_ptr<arrow::Table> apply_select(
    const std::shared_ptr<Select>& select,
    const std::shared_ptr<arrow::Table>& table) {
  // If select is null or fields are empty, return the original table
  if (!select || select->fields().empty()) {
    return table;
  }

  // Get all column names
  std::vector<std::string> all_columns;
  for (const auto& field : table->schema()->fields()) {
    all_columns.push_back(field->name());
  }

  // Track which columns to keep
  std::unordered_set<int> columns_to_keep;

  // Process each field in the SELECT clause
  for (const auto& field : select->fields()) {
    bool is_prefix = true;

    // Check if it's a fully qualified name (contains a dot)
    if (field.find('.') != std::string::npos) {
      // This is a fully qualified column name, look for exact match
      for (int i = 0; i < all_columns.size(); ++i) {
        if (all_columns[i] == field) {
          columns_to_keep.insert(i);
          break;
        }
      }
      is_prefix = false;
    }

    // If it's a prefix (or no exact match was found), find all columns starting
    // with prefix
    if (is_prefix) {
      std::string prefix = field + ".";
      for (int i = 0; i < all_columns.size(); ++i) {
        // Check if column starts with the prefix
        if (all_columns[i].find(prefix) == 0) {
          columns_to_keep.insert(i);
        }
      }
    }
  }

  // Convert set to vector and sort for consistent column order
  std::vector<int> column_indices(columns_to_keep.begin(),
                                  columns_to_keep.end());
  std::ranges::sort(column_indices);

  // Create a result with selected columns
  arrow::Result<std::shared_ptr<arrow::Table>> result =
      table->SelectColumns(column_indices);

  if (!result.ok()) {
    // Handle error - you might want to define how to handle errors
    // For now, let's return the original table if selection fails
    return table;
  }

  return result.ValueOrDie();
}

std::vector<std::shared_ptr<WhereExpr>> get_where_to_inline(
    const std::string& target_var, size_t i,
    const std::vector<std::shared_ptr<Clause>>& clauses) {
  std::vector<std::shared_ptr<WhereExpr>> inlined;
  for (; i < clauses.size(); i++) {
    if (clauses[i]->type() == Clause::Type::WHERE) {
      auto where_expr = std::dynamic_pointer_cast<WhereExpr>(clauses[i]);
      if (where_expr->can_inline(target_var)) {
        IF_DEBUG_ENABLED {
          log_debug("inline where: '{}'", where_expr->toString());
        }
        inlined.push_back(where_expr);
      }
    }
  }
  return inlined;
}

arrow::Result<std::shared_ptr<arrow::Table>> inline_where(
    const SchemaRef& ref, std::shared_ptr<arrow::Table> table,
    QueryState& query_state,
    const std::vector<std::shared_ptr<WhereExpr>>& where_exprs) {
  auto curr_table = std::move(table);
  for (const auto& exp : where_exprs) {
    IF_DEBUG_ENABLED { log_debug("inline where '{}'", exp->toString()); }
    auto result = filter(curr_table, *exp, true);
    if (!result.ok()) {
      log_error(
          "Where inline. Failed to filter table '{}', where: '{}', error: {}",
          ref.toString(), exp->toString(), result.status().ToString());
      return result.status();
    }
    ARROW_RETURN_NOT_OK(query_state.update_table(result.ValueOrDie(), ref));
    curr_table = result.ValueOrDie();
    exp->set_inlined(true);
  }
  return curr_table;
}

/**
 * @brief Prepare query by populating aliases, traversals, tags, and resolving
 * field references
 *
 * This function processes the query structure and:
 * 1. Populates QueryState.aliases from FROM and TRAVERSE clauses
 * 2. Populates QueryState.traversals
 * 3. Sets schema tags in traversals
 * 4. Resolves FieldRef objects in WHERE clauses with actual Field objects from
 * schemas
 */
arrow::Status prepare_query(Query& query, QueryState& query_state) {
  // Phase 1: Process FROM clause to populate aliases
  {
    ARROW_ASSIGN_OR_RAISE(auto from_schema,
                          query_state.resolve_schema(query.from()));
    // FROM clause already processed in main query() function
  }

  // Phase 2: Process TRAVERSE clauses to populate aliases and traversals
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::TRAVERSE) {
      auto traverse = std::dynamic_pointer_cast<Traverse>(clause);

      // Resolve schemas and populate aliases
      ARROW_ASSIGN_OR_RAISE(auto source_schema,
                            query_state.resolve_schema(traverse->source()));
      ARROW_ASSIGN_OR_RAISE(auto target_schema,
                            query_state.resolve_schema(traverse->target()));

      if (!traverse->source().is_declaration()) {
        traverse->mutable_source().set_schema(source_schema);
      }

      if (!traverse->target().is_declaration()) {
        traverse->mutable_target().set_schema(target_schema);
      }

      // Set tags for both source and target
      traverse->mutable_source().set_tag(compute_tag(traverse->source()));
      traverse->mutable_target().set_tag(compute_tag(traverse->target()));

      // Add to traversals
      query_state.traversals.push_back(*traverse);
    }
  }

  // Phase 3: Resolve all ComparisonExpr field references using populated
  // aliases
  for (const auto& clause : query.clauses()) {
    if (clause->type() == Clause::Type::WHERE) {
      auto where_expr = std::dynamic_pointer_cast<WhereExpr>(clause);
      auto res = where_expr->resolve_field_ref(
          query_state.aliases, query_state.schema_registry.get());
      if (!res.ok()) {
        return res.status();
      }
    }
  }

  return arrow::Status::OK();
}

template <class SetA, class SetB, class OutSet>
void dense_intersection(const SetA& a, const SetB& b, OutSet& out) {
  const auto& small = a.size() < b.size() ? a : b;
  const auto& large = a.size() < b.size() ? b : a;
  out.clear();
  out.reserve(std::min(a.size(), b.size()));
  for (const auto& x : small) {
    if (large.contains(x)) {
      out.insert(x);
    }
  }
}

template <class SetA, class SetB, class OutSet>
void dense_difference(const SetA& a, const SetB& b, OutSet& out) {
  out.clear();
  out.reserve(a.size());
  for (const auto& x : a) {
    if (!b.contains(x)) {
      out.insert(x);
    }
  }
}

arrow::Result<std::shared_ptr<QueryResult>> Database::query(
    const Query& query) const {
  QueryState query_state;
  auto result = std::make_shared<QueryResult>();

  // Initialize temporal context if AS OF clause is present
  if (query.temporal_snapshot().has_value()) {
    query_state.temporal_context =
        std::make_unique<TemporalContext>(query.temporal_snapshot().value());
    IF_DEBUG_ENABLED {
      log_debug("Temporal query: AS OF VALIDTIME={}, TXNTIME={}",
                query_state.temporal_context->snapshot().valid_time,
                query_state.temporal_context->snapshot().tx_time);
    }
  }

  // Pre-size hash maps to avoid expensive resizing during execution
  query_state.reserve_capacity(query);

  IF_DEBUG_ENABLED {
    log_debug("Executing query starting from schema '{}'",
              query.from().toString());
  }
  query_state.node_manager = this->node_manager_;
  query_state.schema_registry = this->schema_registry_;
  query_state.from = query.from();

  {
    IF_DEBUG_ENABLED {
      log_debug("processing 'from' {}", query.from().toString());
    }
    // Precompute tag for FROM schema (alias-based hash)
    query_state.from = query.from();
    query_state.from.set_tag(compute_tag(query_state.from));
    ARROW_ASSIGN_OR_RAISE(auto source_schema,
                          query_state.resolve_schema(query.from()));
    if (!this->schema_registry_->exists(source_schema)) {
      log_error("schema '{}' doesn't exist", source_schema);
      return arrow::Status::KeyError("schema doesn't exit: {}", source_schema);
    }
    ARROW_ASSIGN_OR_RAISE(auto source_table, this->get_table(source_schema));
    ARROW_RETURN_NOT_OK(query_state.update_table(source_table, query.from()));
    if (auto res = query_state.compute_fully_qualified_names(query.from(),
                                                             source_schema);
        !res.ok()) {
      return res.status();
    }
  }

  // PHASE: Query Preparation - Populate aliases, traversals, tags, and resolve
  // field references
  {
    IF_DEBUG_ENABLED {
      log_debug(
          "Preparing query: populating aliases, traversals, and resolving "
          "field references");
    }
    auto preparation_result =
        prepare_query(const_cast<Query&>(query), query_state);
    if (!preparation_result.ok()) {
      log_error("Failed to prepare query: {}", preparation_result.ToString());
      return preparation_result;
    }
    IF_DEBUG_ENABLED { log_debug("Query preparation completed successfully"); }
  }

  {
    auto where_exps =
        get_where_to_inline(query.from().value(), 0, query.clauses());
    result->mutable_execution_stats().num_where_clauses_inlined +=
        where_exps.size();
    auto res =
        inline_where(query.from(), query_state.tables[query.from().value()],
                     query_state, where_exps);
    if (!res.ok()) {
      return res.status();
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Processing {} query clauses", query.clauses().size());
  }

  // Precompute 16-bit alias-based tags for all SchemaRefs
  // Also precompute fully-qualified field names per alias used in the query
  std::vector<std::shared_ptr<WhereExpr>> post_where;
  for (auto i = 0; i < query.clauses().size(); ++i) {
    auto clause = query.clauses()[i];
    switch (clause->type()) {
      case Clause::Type::WHERE: {
        auto where = std::dynamic_pointer_cast<WhereExpr>(clause);
        if (where->inlined()) {
          IF_DEBUG_ENABLED {
            log_debug("where '{}' is inlined, skip", where->toString());
          }
          continue;
        }
        auto variables = where->get_all_variables();
        if (variables.empty()) {
          return arrow::Status::Invalid(
              "where clause field must have variable "
              "<var>.<field>, actual={}",
              where->toString());
        }
        if (variables.size() == 1) {
          IF_DEBUG_ENABLED {
            log_debug("Processing WHERE clause: '{}'", where->toString());
          }

          std::unordered_map<std::string, std::set<int64_t>> new_front_ids;
          std::string variable = *variables.begin();
          if (!query_state.tables.contains(variable)) {
            return arrow::Status::Invalid("Unknown variable '{}'", variable);
          }
          auto table = query_state.tables.at(variable);
          auto filtered_table_result = filter(table, *where, true);
          if (!filtered_table_result.ok()) {
            log_error("Failed to process where: '{}', error: {}",
                      where->toString(),
                      filtered_table_result.status().ToString());
            return filtered_table_result.status();
          }
          ARROW_RETURN_NOT_OK(query_state.update_table(
              filtered_table_result.ValueOrDie(), SchemaRef::parse(variable)));
        } else {
          IF_DEBUG_ENABLED {
            log_debug("Add compound WHERE expression: '{}' to post process",
                      where->toString());
          }
          post_where.emplace_back(where);
        }
        break;
      }
      case Clause::Type::TRAVERSE: {
        auto traverse = std::static_pointer_cast<Traverse>(clause);
        // Tags and schemas are already set during preparation phase

        // Get resolved schemas using const resolve_schema (read-only)
        auto source_schema =
            traverse->source().is_declaration()
                ? traverse->source().schema()
                : query_state.aliases.at(traverse->source().value());
        auto target_schema =
            traverse->target().is_declaration()
                ? traverse->target().schema()
                : query_state.aliases.at(traverse->target().value());

        // Fully-qualified field names should also be precomputed during
        // preparation
        if (auto res = query_state.compute_fully_qualified_names(
                traverse->source(), source_schema);
            !res.ok()) {
          return res.status();
        }
        if (auto res = query_state.compute_fully_qualified_names(
                traverse->target(), target_schema);
            !res.ok()) {
          return res.status();
        }

        std::vector<std::shared_ptr<WhereExpr>> where_clauses;
        if (query.inline_where()) {
          where_clauses = get_where_to_inline(traverse->target().value(), i + 1,
                                              query.clauses());
          result->mutable_execution_stats().num_where_clauses_inlined +=
              where_clauses.size();
        }
        // Traversal already added to query_state.traversals during preparation
        IF_DEBUG_ENABLED {
          log_debug("Processing TRAVERSE {}-({})->{}",
                    traverse->source().toString(), traverse->edge_type(),
                    traverse->target().toString());
        }
        auto source = traverse->source();
        if (!query_state.tables.contains(source.value())) {
          IF_DEBUG_ENABLED {
            log_debug("Source table '{}' not found. Loading",
                      traverse->source().toString());
          }
          ARROW_ASSIGN_OR_RAISE(auto source_table,
                                this->get_table(source_schema));
          ARROW_RETURN_NOT_OK(
              query_state.update_table(source_table, traverse->source()));
        }

        IF_DEBUG_ENABLED {
          log_debug("Traversing from {} source nodes",
                    query_state.ids[source.value()].size());
        }
        llvm::DenseSet<int64_t> matched_source_ids;
        llvm::DenseSet<int64_t> matched_target_ids;
        llvm::DenseSet<int64_t> unmatched_source_ids;
        for (auto source_id : query_state.ids[source.value()]) {
          auto outgoing_edges =
              edge_store_->get_outgoing_edges(source_id, traverse->edge_type())
                  .ValueOrDie();  // todo check result
          IF_DEBUG_ENABLED {
            log_debug("Node {} has {} outgoing edges of type '{}'", source_id,
                      outgoing_edges.size(), traverse->edge_type());
          }

          bool source_had_match = false;
          for (const auto& edge : outgoing_edges) {
            auto target_id = edge->get_target_id();
            if (query_state.ids.contains(traverse->target().value()) &&
                !query_state.ids.at(traverse->target().value())
                     .contains(target_id)) {
              continue;
            }
            auto node_result = node_manager_->get_node(target_id);
            if (node_result.ok()) {
              const auto target_node = node_result.ValueOrDie();
              if (target_node->schema_name == target_schema) {
                // Then apply all WHERE clauses with AND logic
                bool passes_all_filters = true;
                // Multiple conditions - could optimize by creating a
                // temporary table and using Arrow expressions For now, use
                // the existing approach but this could be optimized
                for (const auto& where_clause : where_clauses) {
                  if (!apply_where_to_node(where_clause, target_node)
                           .ValueOrDie()) {
                    passes_all_filters = false;
                    break;
                  }
                }
                if (passes_all_filters) {
                  IF_DEBUG_ENABLED {
                    log_debug("found edge {}:{} -[{}]-> {}:{}", source.value(),
                              source_id, traverse->edge_type(),
                              traverse->target().value(), target_node->id);
                  }
                  // record match immediately to avoid extra containers/copies
                  if (!source_had_match) {
                    matched_source_ids.insert(source_id);
                    source_had_match = true;
                  }
                  matched_target_ids.insert(target_node->id);
                  // Use connection pool to avoid allocation
                  auto& conn = query_state.connection_pool_.get();
                  conn.source = traverse->source();
                  conn.source_id = source_id;
                  conn.edge_type = traverse->edge_type();
                  conn.label = "";
                  conn.target = traverse->target();
                  conn.target_id = target_node->id;

                  query_state.connections[traverse->source().value()][source_id]
                      .push_back(conn);
                  query_state.incoming[target_node->id].push_back(conn);
                }
              }
            } else {
              log_warn("Failed to get node {}:{}, error: {}",
                       traverse->target().value(), target_id,
                       node_result.status().ToString());
            }
          }
          if (!source_had_match) {
            IF_DEBUG_ENABLED {
              log_debug("no edge found from {}:{}", source.value(), source_id);
            }
            unmatched_source_ids.insert(source_id);
          }
        }
        if (traverse->traverse_type() == TraverseType::Inner &&
            !unmatched_source_ids.empty()) {
          for (auto id : unmatched_source_ids) {
            IF_DEBUG_ENABLED {
              log_debug("remove unmatched node={}:{}", source.value(), id);
            }
            query_state.remove_node(id, source);
          }
          IF_DEBUG_ENABLED {
            log_debug("rebuild table for schema {}:{}", source.value(),
                      query_state.aliases[source.value()]);
          }
          auto table_result =
              filter_table_by_id(query_state.tables[source.value()],
                                 query_state.ids[source.value()]);
          if (!table_result.ok()) {
            return table_result.status();
          }
          query_state.tables[source.value()] = table_result.ValueOrDie();
        }
        IF_DEBUG_ENABLED {
          log_debug("found {} neighbors for {}", matched_target_ids.size(),
                    traverse->target().toString());
        }

        if (traverse->traverse_type() == TraverseType::Inner) {
          // intersect
          // a:0 -> c:0
          // b:0 -> c:1
          // after a:0 -> c:0 => ids[c] = {0}
          // after b:0 -> c:1 we need to intersect it with ids[c] =
          // intersect({0}, {1}) => {}
          auto target_ids = query_state.get_ids(traverse->target());
          llvm::DenseSet<int64_t> intersect_ids;
          if (target_ids.empty()) {
            intersect_ids = matched_target_ids;
          } else {
            dense_intersection(target_ids, matched_target_ids, intersect_ids);
          }

          query_state.ids[traverse->target().value()] = intersect_ids;
          IF_DEBUG_ENABLED {
            log_debug("intersect_ids count: {}", intersect_ids.size());
            log_debug("{} intersect_ids: {}", traverse->target().toString(),
                      join_container(intersect_ids));
          }

        } else if (traverse->traverse_type() == TraverseType::Left) {
          query_state.ids[traverse->target().value()].insert(
              matched_target_ids.begin(), matched_target_ids.end());
        } else {  // Right, Full remove nodes with incoming connections
          auto target_ids =
              get_ids_from_table(get_table(target_schema).ValueOrDie())
                  .ValueOrDie();
          IF_DEBUG_ENABLED {
            log_debug(
                "traverse type: '{}', matched_source_ids=[{}], "
                "target_ids=[{}]",
                traverse->target().value(), join_container(matched_source_ids),
                join_container(target_ids));
          }
          llvm::DenseSet<int64_t> result;
          dense_difference(target_ids, matched_source_ids, result);
          query_state.ids[traverse->target().value()] = result;
        }

        std::vector<std::shared_ptr<Node>> neighbors;
        for (auto id : query_state.ids[traverse->target().value()]) {
          auto node_res = node_manager_->get_node(id);
          if (node_res.ok()) {
            neighbors.push_back(node_res.ValueOrDie());
          }
        }
        auto target_table_schema =
            schema_registry_->get(target_schema).ValueOrDie();
        auto table_result =
            create_table_from_nodes(target_table_schema, neighbors);
        if (!table_result.ok()) {
          log_error("Failed to create table from neighbors: {}",
                    table_result.status().ToString());
          return table_result.status();
        }
        ARROW_RETURN_NOT_OK(query_state.update_table(table_result.ValueOrDie(),
                                                     traverse->target()));
        break;
      }
      default:
        log_error("Unsupported clause type: {}",
                  static_cast<int>(clause->type()));
        return arrow::Status::NotImplemented(
            "Database::query unsupported clause");
    }
  }

  IF_DEBUG_ENABLED {
    log_debug("Query processing complete, building result");
    log_debug("Query state: {}", query_state.ToString());
  }

  auto output_schema_res = build_denormalized_schema(query_state);
  if (!output_schema_res.ok()) {
    return output_schema_res.status();
  }
  const auto output_schema = output_schema_res.ValueOrDie();
  IF_DEBUG_ENABLED { log_debug("output_schema={}", output_schema->ToString()); }

  auto row_res = populate_rows(query.execution_config(), query_state,
                               query_state.traversals, output_schema);
  if (!row_res.ok()) {
    return row_res.status();
  }
  auto rows = row_res.ValueOrDie();
  auto output_table_res = create_table_from_rows(rows, output_schema);
  if (!output_table_res.ok()) {
    log_error("Failed to create table from rows: {}",
              output_table_res.status().ToString());
    return output_table_res.status();
  }
  auto output_table = output_table_res.ValueOrDie();
  for (const auto& expr : post_where) {
    result->mutable_execution_stats().num_where_clauses_post_processed++;
    IF_DEBUG_ENABLED { log_debug("post process where: {}", expr->toString()); }
    output_table = filter(output_table, *expr, false).ValueOrDie();
  }
  result->set_table(apply_select(query.select(), output_table));
  return result;
}

}  // namespace tundradb
