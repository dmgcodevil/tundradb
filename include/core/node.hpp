#ifndef NODE_HPP
#define NODE_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>

#include "common/constants.hpp"
#include "common/logger.hpp"
#include "common/types.hpp"
#include "core/node_view.hpp"
#include "core/update_type.hpp"
#include "memory/node_arena.hpp"
#include "query/temporal_context.hpp"
#include "schema/schema.hpp"

namespace tundradb {

/// A graph node backed by an arena-allocated memory block.
///
/// Every node belongs to a schema and stores its fields in a NodeArena
/// via a NodeHandle.  Fields are read through get_value / get_value_ptr
/// and written through update / update_fields.  Temporal views are
/// created with view().
class Node {
 private:
  std::unique_ptr<NodeHandle> handle_;
  std::shared_ptr<NodeArena> arena_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;

 public:
  int64_t id;
  std::string schema_name;

  explicit Node(const int64_t id, std::string schema_name,
                std::unique_ptr<NodeHandle> handle = nullptr,
                std::shared_ptr<NodeArena> arena = nullptr,
                std::shared_ptr<Schema> schema = nullptr,
                std::shared_ptr<SchemaLayout> layout = nullptr)
      : handle_(std::move(handle)),
        arena_(std::move(arena)),
        schema_(std::move(schema)),
        layout_(std::move(layout)),
        id(id),
        schema_name(std::move(schema_name)) {}

  /// Return a raw pointer to the field's in-memory representation.
  arrow::Result<const char *> get_value_ptr(
      const std::shared_ptr<Field> &field) const;

  /// Return a lightweight non-owning reference to the field value.
  [[nodiscard]] ValueRef get_value_ref(
      const std::shared_ptr<Field> &field) const;

  /// Read a field value by Field descriptor (returns a copy).
  arrow::Result<Value> get_value(const std::shared_ptr<Field> &field) const;

  [[nodiscard]] std::shared_ptr<Schema> get_schema() const { return schema_; }
  [[nodiscard]] NodeHandle *get_handle() const { return handle_.get(); }
  [[nodiscard]] NodeArena *get_arena() const { return arena_.get(); }

  /// Apply a batch of field updates atomically (one new version).
  arrow::Result<bool> update_fields(const std::vector<FieldUpdate> &updates);

  /// Update a single field (convenience wrapper around update_fields).
  arrow::Result<bool> update(const std::shared_ptr<Field> &field, Value value,
                             UpdateType update_type = UpdateType::SET);

  /// Shorthand for update(field, value, UpdateType::SET).
  arrow::Result<bool> set_value(const std::shared_ptr<Field> &field,
                                const Value &value);

  /// Create a temporal view of this node.
  /// @param ctx  If nullptr, returns view of current version (no time-travel).
  NodeView view(TemporalContext *ctx = nullptr);
};

/// Owns the shared NodeArena and manages per-schema node collections.
///
/// Responsible for creating, retrieving, and removing nodes, assigning
/// auto-incremented per-schema IDs, and validating field types and
/// required constraints when validation is enabled.
class NodeManager {
 public:
  /// @param validation_enabled  Check field types and required constraints.
  /// @param use_node_arena      Must be true (non-arena path is removed).
  /// @param enable_versioning   Enable temporal version chains in the arena.
  explicit NodeManager(std::shared_ptr<SchemaRegistry> schema_registry,
                       bool validation_enabled = true,
                       bool use_node_arena = true,
                       bool enable_versioning = false);

  ~NodeManager() = default;

  /// Look up a node by schema name and ID.
  arrow::Result<std::shared_ptr<Node>> get_node(const std::string &schema_name,
                                                int64_t id);

  /// Remove a node from the in-memory index. Returns false if not found.
  bool remove_node(const std::string &schema_name, int64_t id);

  /// Create a new node, allocate arena storage, and populate initial fields.
  /// @param add  When true, the caller supplies the "id" value (used during
  ///             snapshot restore); otherwise an auto-incremented ID is used.
  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      const std::unordered_map<std::string, Value> &data,
      bool add = false);

  /// Override the next-ID counter for a schema (used during restore).
  void set_id_counter(const std::string &schema_name, int64_t value);

  /// Return the current value of the per-schema ID counter.
  int64_t get_id_counter(const std::string &schema_name) const;

  /// Return all per-schema ID counters (for snapshot/manifest persistence).
  std::unordered_map<std::string, int64_t> get_all_id_counters() const;

  /// Restore all per-schema ID counters from a snapshot/manifest.
  void set_all_id_counters(
      const std::unordered_map<std::string, int64_t> &counters);

 private:
  std::unordered_map<std::string, std::atomic<int64_t>> id_counters_;
  std::unordered_map<std::string,
                     std::unordered_map<int64_t, std::shared_ptr<Node>>>
      nodes_;
  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::shared_ptr<NodeArena> node_arena_;
  bool validation_enabled_;
  bool use_node_arena_;
  std::string schema_name_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;

  std::shared_ptr<SchemaLayout> create_or_get_layout(
      const std::string &schema_name) const;
  void init_schema(const std::string &schema_name);
};

}  // namespace tundradb

#endif  // NODE_HPP