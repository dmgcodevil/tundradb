#ifndef NODE_HPP
#define NODE_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>

#include "logger.hpp"
#include "node_arena.hpp"
#include "node_view.hpp"
#include "schema.hpp"
#include "temporal_context.hpp"
#include "types.hpp"

namespace tundradb {

enum UpdateType {
  SET,
  // todo APPEND for List/Array
};

class Node {
 private:
  std::unordered_map<std::string, Value> data_;
  std::unique_ptr<NodeHandle> handle_;
  std::shared_ptr<NodeArena> arena_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<SchemaLayout> layout_;

 public:
  int64_t id;
  std::string schema_name;

  explicit Node(const int64_t id, std::string schema_name,
                std::unordered_map<std::string, Value> initial_data,
                std::unique_ptr<NodeHandle> handle = nullptr,
                std::shared_ptr<NodeArena> arena = nullptr,
                std::shared_ptr<Schema> schema = nullptr,
                std::shared_ptr<SchemaLayout> layout = nullptr)
      : data_(std::move(initial_data)),
        handle_(std::move(handle)),
        arena_(std::move(arena)),
        schema_(std::move(schema)),
        layout_(std::move(layout)),
        id(id),
        schema_name(std::move(schema_name)) {}

  ~Node() { data_.clear(); }

  void add_field(const std::string &field_name, Value value) {
    data_[field_name] = std::move(value);
  }

  arrow::Result<const char *> get_value_ptr(
      const std::shared_ptr<Field> &field) const {
    if (arena_ != nullptr) {
      return arena_->get_field_value_ptr(*handle_, layout_, field);
    }
    return arrow::Status::NotImplemented("");
  }

  [[nodiscard]] ValueRef get_value_ref(
      const std::shared_ptr<Field> &field) const {
    const char *ptr = arena_->get_field_value_ptr(*handle_, layout_, field);
    return {ptr, field->type()};
  }

  [[deprecated]]
  arrow::Result<Value> get_value(const std::string &field) const {
    log_warn("get_value by string is deprecated");
    return get_value(schema_->get_field(field));
  }

  arrow::Result<Value> get_value(const std::shared_ptr<Field> &field) const {
    if (arena_ != nullptr) {
      return arena_->get_field_value(*handle_, layout_, field);
    }

    const auto it = data_.find(field->name());
    if (it == data_.end()) {
      return arrow::Status::KeyError("Field not found: ", field->name());
    }
    return it->second;
  }

  [[nodiscard]] std::shared_ptr<Schema> get_schema() const { return schema_; }

  // Get node handle (for testing and internal use)
  [[nodiscard]] NodeHandle *get_handle() const { return handle_.get(); }

  [[deprecated]]
  arrow::Result<bool> update(const std::string &field, Value value,
                             UpdateType update_type) {
    return update(schema_->get_field(field), value, update_type);
  }

  arrow::Result<bool> update(const std::shared_ptr<Field> &field, Value value,
                             UpdateType update_type) {
    if (arena_ != nullptr) {
      return arena_->set_field_value(*handle_, layout_, field, value);
    }

    if (const auto it = data_.find(field->name()); it == data_.end()) {
      return arrow::Status::KeyError("Field not found: ", field->name());
    }

    switch (update_type) {
      case SET:
        data_[field->name()] = std::move(value);
        break;
    }

    return true;
  }

  [[deprecated]]
  arrow::Result<bool> set_value(const std::string &field, const Value &value) {
    log_warn("set_value by string is deprecated");
    return update(schema_->get_field(field), value, SET);
  }

  arrow::Result<bool> set_value(const std::shared_ptr<Field> &field,
                                const Value &value) {
    return update(field, value, SET);
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
      // No temporal context or no handle -> use current version
      return {this, handle_->version_info_, arena_.get(), layout_};
    }

    // Resolve version using TemporalContext
    VersionInfo *resolved = ctx->resolve_version(id, *handle_);
    return {this, resolved, arena_.get(), layout_};
  }
};

class NodeManager {
 public:
  explicit NodeManager(std::shared_ptr<SchemaRegistry> schema_registry,
                       const bool validation_enabled = true,
                       const bool use_node_arena = true) {
    validation_enabled_ = validation_enabled;
    use_node_arena_ = use_node_arena;
    schema_registry_ = std::move(schema_registry);
    layout_registry_ = std::make_shared<LayoutRegistry>();
    node_arena_ = node_arena_factory::create_free_list_arena(layout_registry_);
  }

  ~NodeManager() { node_arena_->clear(); }

  arrow::Result<std::shared_ptr<Node>> get_node(const int64_t id) {
    return nodes[id];
  }

  bool remove_node(const int64_t id) { return nodes.erase(id) > 0; }

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
      id = id_counter.fetch_add(1);
    } else {
      id = data.at("id").as_int64();
    }

    if (use_node_arena_) {
      NodeHandle node_handle = node_arena_->allocate_node(layout_);
      // Logger::get_instance().debug("node has been allocated at {}",
      //                              node_handle.ptr);
      node_arena_->set_field_value(node_handle, layout_,
                                   schema_->get_field("id"), Value{id});
      for (const auto &field : schema_->fields()) {
        if (field->name() == "id") continue;
        if (!data.contains(field->name())) {
          // Logger::get_instance().debug("{} set NA value", field->name());
          node_arena_->set_field_value(node_handle, layout_, field, Value());
        } else {
          const auto value = data.find(field->name())->second;
          node_arena_->set_field_value(node_handle, layout_, field, value);
        }
      }

      auto node = std::make_shared<Node>(
          id, schema_name, EMPTY_DATA,
          std::make_unique<NodeHandle>(std::move(node_handle)), node_arena_,
          schema_, layout_);
      nodes[id] = node;
      return node;
    } else {
      std::unordered_map<std::string, Value> normalized_data;
      normalized_data["id"] = Value{id};

      for (const auto &field : schema_->fields()) {
        if (field->name() == "id") continue;
        if (!data.contains(field->name())) {
          normalized_data[field->name()] = Value();
        } else {
          const auto value = data.find(field->name())->second;
          normalized_data[field->name()] = value;
        }
      }

      // populate other fields

      auto node = std::make_shared<Node>(id, schema_name, normalized_data,
                                         std::unique_ptr<NodeHandle>{}, nullptr,
                                         schema_);
      nodes[id] = node;
      return node;
    }
  }

  void set_id_counter(const int64_t value) { id_counter.store(value); }
  int64_t get_id_counter() const { return id_counter.load(); }

 private:
  std::atomic<int64_t> id_counter{0};
  std::unordered_map<int64_t, std::shared_ptr<Node>> nodes;
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

  const std::unordered_map<std::string, Value> EMPTY_DATA{};

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
    // Non-versioned node -> delegate to Node
    return node_->get_value_ptr(field);
  }

  const NodeHandle *handle = node_->get_handle();
  assert(handle != nullptr && "Versioned node must have a handle");

  return arena_->get_field_value_ptr_from_version(*handle, resolved_version_,
                                                  layout_, field);
}

inline arrow::Result<Value> NodeView::get_value(
    const std::shared_ptr<Field> &field) const {
  assert(arena_ != nullptr && "NodeView created with null arena");
  assert(node_ != nullptr && "NodeView created with null node");

  if (resolved_version_ == nullptr) {
    // Non-versioned node -> delegate to Node
    return node_->get_value(field);
  }

  const NodeHandle *handle = node_->get_handle();
  assert(handle != nullptr && "Versioned node must have a handle");

  return arena_->get_field_value_from_version(*handle, resolved_version_,
                                              layout_, field);
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