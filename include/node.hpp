#ifndef NODE_HPP
#define NODE_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "schema.hpp"
#include "types.hpp"

namespace tundradb {

enum UpdateType {
  SET,
  // todo APPEND for List/Array
};

class Node {
 private:
  std::unordered_map<std::string, Value> data_;

 public:
  int64_t id;
  std::string schema_name;

  explicit Node(const int64_t id, std::string schema_name,
                std::unordered_map<std::string, Value> initial_data)
      : data_(std::move(initial_data)),
        id(id),
        schema_name(std::move(schema_name)) {}

  ~Node() { data_.clear(); }

  void add_field(const std::string &field_name, Value value) {
    data_[field_name] = std::move(value);
  }

  arrow::Result<Value> get_field(const std::string &field_name) const {
    const auto it = data_.find(field_name);
    if (it == data_.end()) {
      return arrow::Status::KeyError("Field not found: ", field_name);
    }
    return it->second;
  }

  [[nodiscard]] const std::unordered_map<std::string, Value> &data() const {
    return data_;
  }

  arrow::Result<bool> update(const std::string &field_name, Value value,
                             UpdateType update_type) {
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
  NodeManager() = default;

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

    std::unordered_map<std::string, Value> normalized_data;
    for (const auto &field : schema->fields()) {
      if (field->name() != "id" && !field->nullable() &&
          (!data.contains(field->name()) ||
           data.find(field->name())->second.is_null())) {
        return arrow::Status::Invalid("Field '", field->name(),
                                      "' is required");
      }
      if (!data.contains(field->name())) {
        normalized_data[field->name()] = Value();
      } else {
        const auto value = data.find(field->name())->second;
        if (arrow_type_to_value_type(field->type()) != value.type()) {
          return arrow::Status::Invalid(
              "Type mismatch for field '", field->name(), "'. Expected ",
              field->type()->ToString(), " but got ", to_string(value.type()));
        }
        normalized_data[field->name()] = value;
      }
    }

    auto id = id_counter.fetch_add(1);
    normalized_data["id"] = Value{id};
    auto node = std::make_shared<Node>(id, schema_name, normalized_data);
    nodes[id] = node;
    return node;
  }

  void set_id_counter(const int64_t value) { id_counter.store(value); }
  int64_t get_id_counter() const { return id_counter.load(); }

 private:
  std::atomic<int64_t> id_counter{0};
  std::unordered_map<int64_t, std::shared_ptr<Node>> nodes;
};

}  // namespace tundradb

#endif  // NODE_HPP