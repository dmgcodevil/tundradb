#ifndef NODE_HPP
#define NODE_HPP

#include <arrow/api.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace tundradb {

// Utility functions for creating arrays
static arrow::Result<std::shared_ptr<arrow::Array>> create_int64_array(
    const int64_t value) {
  arrow::Int64Builder int64_builder;
  ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
  ARROW_RETURN_NOT_OK(int64_builder.Append(value));
  std::shared_ptr<arrow::Array> int64_array;
  ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
  return int64_array;
}

static arrow::Result<std::shared_ptr<arrow::Array>> create_str_array(
    const std::string &value) {
  arrow::StringBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(value));
  std::shared_ptr<arrow::Array> string_arr;
  ARROW_RETURN_NOT_OK(builder.Finish(&string_arr));
  return string_arr;
}

static arrow::Result<std::shared_ptr<arrow::Array>> create_null_array(
    const std::shared_ptr<arrow::DataType> &type) {
  switch (type->id()) {
    case arrow::Type::INT64: {
      arrow::Int64Builder builder;
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      return array;
    }
    case arrow::Type::STRING: {
      arrow::StringBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendNull());
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      return array;
    }
    default:
      return arrow::Status::NotImplemented("Unsupported type: ",
                                           type->ToString());
  }
}

// Operation types
enum OperationType { SET };

// Base operation class
struct BaseOperation {
  int64_t node_id;  // The node identifier to apply the operation to
  std::vector<std::string> field_name;  // Name of the field to update

  BaseOperation(int64_t id, const std::vector<std::string> &field)
      : node_id(id), field_name(field) {}

  virtual arrow::Result<bool> apply(const std::shared_ptr<arrow::Array> &array,
                                    int64_t row_index) const = 0;

  virtual OperationType op_type() const = 0;

  virtual bool should_replace_array() const { return false; }
  virtual std::shared_ptr<arrow::Array> get_replacement_array() const {
    return nullptr;
  }

  virtual ~BaseOperation() = default;
};

// Set operation - assigns a new value to a field
struct SetOperation : public BaseOperation {
  std::shared_ptr<arrow::Array> value;  // The new value to set

  SetOperation(int64_t id, const std::vector<std::string> &field,
               const std::shared_ptr<arrow::Array> &v)
      : BaseOperation(id, field), value(v) {}

  ~SetOperation() {
    // Ensure the value is properly cleaned up
    value.reset();
  }

  arrow::Result<bool> apply(const std::shared_ptr<arrow::Array> &array,
                            int64_t row_index) const override {
    // Make a copy of the array data to avoid modifying the original
    auto array_data = array->data()->Copy();
    switch (array->type_id()) {
      case arrow::Type::INT64: {
        auto raw_values = array_data->GetMutableValues<int64_t>(1);
        raw_values[row_index] =
            std::static_pointer_cast<arrow::Int64Array>(value)->Value(0);
        return {true};
      }
      default:
        return arrow::Status::Invalid("not implemented");
    }
  }

  OperationType op_type() const override { return OperationType::SET; }

  bool should_replace_array() const override {
    return value->type_id() == arrow::Type::STRING;
  }

  std::shared_ptr<arrow::Array> get_replacement_array() const override {
    return value;
  }
};

// Node class represents a single data entity
class Node {
 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data;

 public:
  int64_t id;
  std::string schema_name;

  explicit Node(const int64_t id, std::string schema_name,
                std::unordered_map<std::string, std::shared_ptr<arrow::Array>>
                    initial_data)
      : id(id),
        schema_name(std::move(schema_name)),
        data(std::move(initial_data)) {}

  ~Node() {
    // Clear the data map to ensure proper cleanup of Arrow arrays
    data.clear();
  }

  void add_field(const std::string &field_name,
                 const std::shared_ptr<arrow::Array> &value) {
    data.insert(std::make_pair(field_name, value));
  }

  arrow::Result<std::shared_ptr<arrow::Array>> get_field(
      const std::string &field_name) const {
    auto it = data.find(field_name);
    if (it == data.end()) {
      return arrow::Status::KeyError("Field not found: ", field_name);
    }
    return it->second;
  }

  arrow::Result<bool> update(const std::shared_ptr<BaseOperation> &update) {
    if (update->field_name.empty()) {
      return arrow::Status::Invalid("Field name vector is empty");
    }

    auto it = data.find(update->field_name[0]);
    if (it == data.end()) {
      return arrow::Status::KeyError("Field not found: ",
                                     update->field_name[0]);
    }
    if (update->should_replace_array()) {
      data[update->field_name[0]] = update->get_replacement_array();
      return true;
    }
    return update->apply(it->second, 0);
  }
};

}  // namespace tundradb

#endif  // NODE_HPP