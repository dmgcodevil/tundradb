//
// Created by Roman Pliashkou on 3/22/25.
//

#ifndef CORE_HPP
#define CORE_HPP

#include <memory_resource>
#include <string>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <arrow/type.h>

namespace tundradb {
    // Base operation class
    struct BaseOperation {
        int64_t node_id; // The node identifier to apply the operation to
        std::vector<std::string> field_name; // Name of the field to update

        BaseOperation(int64_t id, const std::vector<std::string> &field)
            : node_id(id), field_name(field) {
        }

        virtual arrow::Result<bool> apply(
            const std::shared_ptr<arrow::Array> &array,
            int64_t row_index) const = 0;

        virtual ~BaseOperation() = default;
    };

    struct SetOperation : public BaseOperation {
        std::shared_ptr<arrow::Array> value; // The new value to set

        SetOperation(int64_t id, const std::vector<std::string> &field,
                     const std::shared_ptr<arrow::Array> &v): BaseOperation(id, field), value(v) {
        }

        arrow::Result<bool> apply(const std::shared_ptr<arrow::Array> &array, int64_t row_index) const override {
            auto array_data = array->data();
            switch (array->type_id()) {
                case arrow::Type::INT64: {
                    auto raw_values = array_data->GetMutableValues<int32_t>(1);
                    raw_values[row_index] = std::static_pointer_cast<arrow::Int32Array>(value)->Value(0);
                    return {true};
                }
                default:
                    return arrow::Status::Invalid("not implemented");
            }
        }
    };

    class Node {
    public:
        int64_t id;

        explicit Node(const int64_t id) : id(id) {
        }

        ~Node() = default;

        void add_field(const std::string &field_name, const std::shared_ptr<arrow::Array> &value) {
            data.insert(std::make_pair(field_name, value));
        }

        arrow::Result<std::shared_ptr<arrow::Array> > get_field(const std::string &field_name) const {
            auto it = data.find(field_name);
            if (it == data.end()) {
                return arrow::Status::KeyError("Field not found: ", field_name);
            }
            return it->second;
        }

        arrow::Result<bool> update(const BaseOperation &update) {
            if (update.field_name.empty()) {
                return arrow::Status::Invalid("Field name vector is empty");
            }

            auto it = data.find(update.field_name[0]);
            if (it == data.end()) {
                return arrow::Status::KeyError("Field not found: ", update.field_name[0]);
            }

            return update.apply(it->second, 0);
        }

    private:
        std::pmr::unordered_map<std::string, std::shared_ptr<arrow::Array>> data;
    };

    arrow::Result<bool> demo_single_node();
}

#endif //CORE_HPP
