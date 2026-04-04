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
      const std::shared_ptr<Field> &field) const {
    if (arena_ != nullptr) {
      return arena_->get_value_ptr(*handle_, layout_, field);
    }
    return arrow::Status::NotImplemented("");
  }

  /// Return a lightweight non-owning reference to the field value.
  [[nodiscard]] ValueRef get_value_ref(
      const std::shared_ptr<Field> &field) const {
    const char *ptr = arena_->get_value_ptr(*handle_, layout_, field);
    return {ptr, field->type()};
  }

  /// Read a field value by Field descriptor (returns a copy).
  arrow::Result<Value> get_value(const std::shared_ptr<Field> &field) const {
    if (!arena_ || !handle_) {
      return arrow::Status::Invalid(
          "get_value requires arena-backed node with valid handle");
    }
    return NodeArena::get_value(*handle_, layout_, field);
  }

  [[nodiscard]] std::shared_ptr<Schema> get_schema() const { return schema_; }
  [[nodiscard]] NodeHandle *get_handle() const { return handle_.get(); }
  [[nodiscard]] NodeArena *get_arena() const { return arena_.get(); }

  /// Apply a batch of field updates atomically (one new version).
  arrow::Result<bool> update_fields(const std::vector<FieldUpdate> &updates) {
    if (!arena_ || !handle_) {
      return arrow::Status::Invalid(
          "update_fields requires arena-backed node with valid handle");
    }
    return arena_->apply_updates(*handle_, layout_, updates);
  }

  /// Update a single field (convenience wrapper around update_fields).
  arrow::Result<bool> update(const std::shared_ptr<Field> &field, Value value,
                             UpdateType update_type = UpdateType::SET) {
    return update_fields({{field, std::move(value), update_type}});
  }

  /// Shorthand for update(field, value, UpdateType::SET).
  arrow::Result<bool> set_value(const std::shared_ptr<Field> &field,
                                const Value &value) {
    return update(field, value, UpdateType::SET);
  }

  /**
   * Create a temporal view of this node.
   *
   * @param ctx TemporalContext with snapshot (valid_time, tx_time).
   *            If nullptr, returns view of current version (no time-travel).
   * @return NodeView that resolves version once and caches it.
   *
   * Usage:
   *   TemporalContext ctx(TemporalSnapshot::as_of_valid(timestamp));
   *   auto view = node.view(&ctx);
   *   auto age = view.get_value_ptr(age_field);
   */
  NodeView view(TemporalContext *ctx = nullptr) {
    if (!ctx) {
      // No temporal context  > use the current version
      return {this, handle_->version_info_, arena_.get(), layout_};
    }

    // Resolve version using TemporalContext
    VersionInfo *resolved = ctx->resolve_node_version(id, *handle_);
    return {this, resolved, arena_.get(), layout_};
  }
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
                       const bool validation_enabled = true,
                       const bool use_node_arena = true,
                       const bool enable_versioning = false) {
    validation_enabled_ = validation_enabled;
    use_node_arena_ = use_node_arena;
    schema_registry_ = std::move(schema_registry);
    layout_registry_ = std::make_shared<LayoutRegistry>();
    // Create arena with versioning enabled if requested
    node_arena_ = node_arena_factory::create_free_list_arena(
        layout_registry_, NodeArena::kInitialSize, NodeArena::kMinFragmentSize,
        enable_versioning);
  }

  ~NodeManager() = default;

  /// Look up a node by schema name and ID.
  arrow::Result<std::shared_ptr<Node>> get_node(const std::string &schema_name,
                                                const int64_t id) {
    auto schema_it = nodes_.find(schema_name);
    if (schema_it == nodes_.end()) {
      return arrow::Status::KeyError("Schema not found: ", schema_name);
    }

    auto node_it = schema_it->second.find(id);
    if (node_it == schema_it->second.end()) {
      return arrow::Status::KeyError("Node not found: ", schema_name, ":", id);
    }

    return node_it->second;
  }

  /// Remove a node from the in-memory index. Returns false if not found.
  bool remove_node(const std::string &schema_name, const int64_t id) {
    auto schema_it = nodes_.find(schema_name);
    if (schema_it == nodes_.end()) {
      return false;
    }
    return schema_it->second.erase(id) > 0;
  }

  /// Create a new node, allocate arena storage, and populate initial fields.
  /// @param add  When true, the caller supplies the "id" value (used during
  ///             snapshot restore); otherwise an auto-incremented ID is used.
  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      const std::unordered_map<std::string, Value> &data,
      const bool add = false) {
    if (schema_name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }

    init_schema(schema_name);

    // ARROW_ASSIGN_OR_RAISE(const auto schema,
    //                       schema_registry_->get(schema_name));
    if (validation_enabled_) {
      if (!add && data.contains("id")) {
        return arrow::Status::Invalid("'id' column is auto generated");
      }

      if (add && !data.contains("id")) {
        return arrow::Status::Invalid("'id' is missing");
      }

      for (const auto &field : schema_->fields()) {
        // check required
        if (field->name() != "id" && !field->nullable() &&
            (!data.contains(field->name()) ||
             data.find(field->name())->second.is_null())) {
          return arrow::Status::Invalid("Field '", field->name(),
                                        "' is required");
        }

        if (data.contains(field->name())) {
          const auto value = data.find(field->name())->second;
          if (field->type() != value.type()) {
            return arrow::Status::Invalid(
                "Type mismatch for field '", field->name(), "'. Expected ",
                to_string(field->type()), " but got ", to_string(value.type()));
          }
        }
      }
    }

    int64_t id = 0;
    if (!add) {
      // Get or create per-schema ID counter
      if (id_counters_.find(schema_name) == id_counters_.end()) {
        id_counters_[schema_name].store(0);
      }
      id = id_counters_[schema_name].fetch_add(1);
    } else {
      id = data.at("id").as_int64();
    }

    if (!use_node_arena_) {
      return arrow::Status::NotImplemented(
          "NodeManager without arena is no longer supported");
    }

    NodeHandle node_handle = node_arena_->allocate_node(layout_);

    // Initial population of v0: write directly to base node
    // Use set_field_value_v0 for all fields (doesn't create versions)
    ARROW_RETURN_NOT_OK(node_arena_->set_field_value_v0(
        node_handle, layout_, schema_->get_field(std::string(field_names::kId)),
        Value{id}));

    for (const auto &field : schema_->fields()) {
      if (field->name() == field_names::kId) continue;

      Value value;
      if (data.contains(field->name())) {
        value = data.find(field->name())->second;
      }  // else: Value() = NULL

      ARROW_RETURN_NOT_OK(
          node_arena_->set_field_value_v0(node_handle, layout_, field, value));
    }

    auto node = std::make_shared<Node>(
        id, schema_name, std::make_unique<NodeHandle>(std::move(node_handle)),
        node_arena_, schema_, layout_);
    nodes_[schema_name][id] = node;
    return node;
  }

  /// Override the next-ID counter for a schema (used during restore).
  void set_id_counter(const std::string &schema_name, const int64_t value) {
    id_counters_[schema_name].store(value);
  }

  /// Return the current value of the per-schema ID counter.
  int64_t get_id_counter(const std::string &schema_name) const {
    auto it = id_counters_.find(schema_name);
    if (it == id_counters_.end()) {
      return 0;
    }
    return it->second.load();
  }

  /// Return all per-schema ID counters (for snapshot/manifest persistence).
  std::unordered_map<std::string, int64_t> get_all_id_counters() const {
    std::unordered_map<std::string, int64_t> result;
    for (const auto &[schema_name, counter] : id_counters_) {
      result[schema_name] = counter.load();
    }
    return result;
  }

  /// Restore all per-schema ID counters from a snapshot/manifest.
  void set_all_id_counters(
      const std::unordered_map<std::string, int64_t> &counters) {
    for (const auto &[schema_name, value] : counters) {
      id_counters_[schema_name].store(value);
    }
  }

 private:
  // Per-schema ID counters (schema_name -> counter)
  std::unordered_map<std::string, std::atomic<int64_t>> id_counters_;

  // Per-schema node storage (schema_name -> (node_id -> Node))
  std::unordered_map<std::string,
                     std::unordered_map<int64_t, std::shared_ptr<Node>>>
      nodes_;

  std::shared_ptr<SchemaRegistry> schema_registry_;
  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::shared_ptr<NodeArena> node_arena_;
  bool validation_enabled_;
  bool use_node_arena_;

  // cache schema
  std::string schema_name_;
  std::shared_ptr<Schema> schema_;

  // cache layout
  std::shared_ptr<SchemaLayout> layout_;

  // since node creation is single threaded, we can cache the layout
  // w/o synchronization
  std::shared_ptr<SchemaLayout> create_or_get_layout(
      const std::string &schema_name) const {
    if (layout_registry_->exists(schema_name)) {
      return layout_registry_->get_layout(schema_name);
    }
    auto layout = layout_registry_->create_layout(
        schema_registry_->get(schema_name).ValueOrDie());
    layout_registry_->register_layout(layout);
    return layout;
  }

  // since node creation is single threaded, we can cache the schema
  // w/o synchronization
  void init_schema(const std::string &schema_name) {
    if (schema_name_ == schema_name) return;
    schema_name_ = schema_name;
    schema_ = schema_registry_->get(schema_name).ValueOrDie();
    layout_ = create_or_get_layout(schema_name);
  }
};

// ============================================================================
// NodeView inline implementations (after Node is fully defined)
// ============================================================================

inline arrow::Result<const char *> NodeView::get_value_ptr(
    const std::shared_ptr<Field> &field) const {
  assert(arena_ != nullptr && "NodeView created with null arena");
  assert(node_ != nullptr && "NodeView created with null node");

  if (resolved_version_ == nullptr) {
    return node_->get_value_ptr(field);
  }

  const NodeHandle *handle = node_->get_handle();
  assert(handle != nullptr && "Versioned node must have a handle");

  return arena_->get_value_ptr_at_version(*handle, resolved_version_, layout_,
                                          field);
}

inline arrow::Result<Value> NodeView::get_value(
    const std::shared_ptr<Field> &field) const {
  assert(node_ != nullptr && "NodeView created with null node");

  if (resolved_version_ == nullptr) {
    return node_->get_value(field);
  }

  const NodeHandle *handle = node_->get_handle();
  assert(handle != nullptr && "Versioned node must have a handle");

  return NodeArena::get_value_at_version(*handle, resolved_version_, layout_,
                                         field);
}

inline bool NodeView::is_visible() const {
  assert(arena_ != nullptr && "NodeView created with null arena");
  assert(node_ != nullptr && "NodeView created with null node");
  const NodeHandle *handle = node_->get_handle();
  assert(handle != nullptr && "Node must have a handle");

  // Non-versioned nodes are always visible
  if (!handle->is_versioned()) {
    return true;
  }

  // For versioned nodes, check if we found a visible version at the snapshot
  return resolved_version_ != nullptr;
}

}  // namespace tundradb

#endif  // NODE_HPP