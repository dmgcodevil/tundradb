#ifndef NODE_HPP
#define NODE_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "node_arena.hpp"
#include "schema.hpp"
#include "types.hpp"

namespace tundradb {

constexpr bool USE_NODE_ARENA = true;

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

 public:
  int64_t id;
  std::string schema_name;

  explicit Node(const int64_t id, std::string schema_name,
                std::unordered_map<std::string, Value> initial_data,
                std::unique_ptr<NodeHandle> handle = nullptr,
                std::shared_ptr<NodeArena> arena = nullptr,
                std::shared_ptr<Schema> schema = nullptr)
      : data_(std::move(initial_data)),
        handle_(std::move(handle)),
        arena_(std::move(arena)),
        schema_(std::move(schema)),
        id(id),
        schema_name(std::move(schema_name)) {}

  ~Node() { data_.clear(); }

  void add_field(const std::string &field_name, Value value) {
    data_[field_name] = std::move(value);
  }

  arrow::Result<Value> get_value(const std::string &field_name) const {
    if (USE_NODE_ARENA) {
      if (schema_->get_field(field_name) == nullptr) {
        Logger::get_instance().debug("Field not found");
        return arrow::Status::KeyError("Field not found: ", field_name);
      }
      return arena_->get_field_value(*handle_, schema_name, field_name);
    }

    const auto it = data_.find(field_name);
    if (it == data_.end()) {
      return arrow::Status::KeyError("Field not found: ", field_name);
    }
    return it->second;
  }

  // todo do we need this ?
  // todo remove
  [[nodiscard]] const std::unordered_map<std::string, Value> data() const {
    std::unordered_map<std::string, Value> result;
    for (auto field : schema_->fields()) {
      result[field->name()] =
          arena_->get_field_value(*handle_, schema_name, field->name());
    }
    return result;
  }

  arrow::Result<bool> update(const std::string &field_name, Value value,
                             UpdateType update_type) {
    if (USE_NODE_ARENA) {
      if (schema_->get_field(field_name) == nullptr) {
        Logger::get_instance().debug("Field not found");
        return arrow::Status::KeyError("Field not found: ", field_name);
      }

      arena_->set_field_value(*handle_, schema_name, field_name, value);
      Logger::get_instance().debug("set value is done");
      return true;
    }

    if (const auto it = data_.find(field_name); it == data_.end()) {
      return arrow::Status::KeyError("Field not found: ", field_name);
    }

    switch (update_type) {
      case SET:
        data_[field_name] = std::move(value);
        break;
    }

    return true;
  }

  arrow::Result<bool> set_value(const std::string &field_name,
                                const Value &value) {
    return update(field_name, value, SET);
  }
};

class NodeManager {
 public:
  NodeManager() {
    layout_registry_ = std::make_shared<LayoutRegistry>();
    node_arena_ = node_arena_factory::create_free_list_arena(layout_registry_);
  }

  arrow::Result<std::shared_ptr<Node>> get_node(const int64_t id) {
    return nodes[id];
  }

  bool add_node(std::shared_ptr<Node> node) {
    // todo check if node exists
    nodes[node->id] = node;
    return true;
  }

  bool remove_node(const int64_t id) { return nodes.erase(id) > 0; }

  arrow::Result<std::shared_ptr<Node>> create_node(
      const std::string &schema_name,
      const std::unordered_map<std::string, Value> &data,
      const std::shared_ptr<SchemaRegistry> &schema_registry) {
    if (schema_name.empty()) {
      return arrow::Status::Invalid("Schema name cannot be empty");
    }

    ARROW_ASSIGN_OR_RAISE(const auto schema, schema_registry->get(schema_name));
    if (data.contains("id")) {
      return arrow::Status::Invalid("'id' column is auto generated");
    }

    for (const auto &field : schema->fields()) {
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

      // if (!data.contains(name)) {
      //   normalized_data[name] = Value();
      // } else {
      //
      //   normalized_data[name] = value;
      // }
    }

    auto id = id_counter.fetch_add(1);

    if (USE_NODE_ARENA) {
      std::shared_ptr<SchemaLayout> layout;
      if (layout_registry_->exists(schema_name)) {
        layout = layout_registry_->get_layout(schema_name);
      } else {
        layout = layout_registry_->create_layout(
            schema_registry->get(schema_name).ValueOrDie());
        layout_registry_->register_layout(layout);
      }
      NodeHandle node_handle = node_arena_->allocate_node(schema_name);
      Logger::get_instance().debug("node has been allocated at {}",
                                   node_handle.ptr);
      node_arena_->set_field_value(node_handle, schema_name, "id", Value{id});
      for (const auto &field : schema->fields()) {
        if (field->name() == "id") continue;
        if (!data.contains(field->name())) {
          Logger::get_instance().debug("{} set NA value", field->name());
          node_arena_->set_field_value(node_handle, schema_name, field->name(),
                                       Value());
        } else {
          const auto value = data.find(field->name())->second;
          node_arena_->set_field_value(node_handle, schema_name, field->name(),
                                       value);
        }
      }

      auto node = std::make_shared<Node>(
          id, schema_name, std::unordered_map<std::string, Value>(),
          std::make_unique<NodeHandle>(node_handle), node_arena_, schema);
      nodes[id] = node;
      return node;
    } else {
      std::unordered_map<std::string, Value> normalized_data;
      normalized_data["id"] = Value{id};
      // populate other fields

      auto node = std::make_shared<Node>(id, schema_name, normalized_data,
                                         std::unique_ptr<NodeHandle>{}, nullptr,
                                         schema);
      nodes[id] = node;
      return node;
    }
  }

  void set_id_counter(const int64_t value) { id_counter.store(value); }
  int64_t get_id_counter() const { return id_counter.load(); }

 private:
  std::atomic<int64_t> id_counter{0};
  std::unordered_map<int64_t, std::shared_ptr<Node>> nodes;
  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::shared_ptr<NodeArena> node_arena_;
};

}  // namespace tundradb

#endif  // NODE_HPP