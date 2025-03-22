//
// Created by Roman Pliashkou on 3/22/25.
//

#ifndef CORE_HPP
#define CORE_HPP
#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/table.h>

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
        std::pmr::unordered_map<std::string, std::shared_ptr<arrow::Array> > data;
    };


    inline void print_table(const std::shared_ptr<arrow::Table> &table) {
        if (!table) {
            std::cout << "Null table" << std::endl;
            return;
        }

        std::cout << "Table Schema:" << std::endl;
        std::cout << table->schema()->ToString() << std::endl;
        std::cout << "Table Data (" << table->num_rows() << " rows):" << std::endl;

        try {
            for (int64_t i = 0; i < table->num_rows(); ++i) {
                for (int j = 0; j < table->num_columns(); ++j) {
                    auto column = table->column(j);
                    if (!column || column->num_chunks() == 0) {
                        std::cout << "NULL\t";
                        continue;
                    }

                    auto chunk = column->chunk(0);
                    if (!chunk) {
                        std::cout << "NULL\t";
                        continue;
                    }

                    if (i >= chunk->length() || chunk->IsNull(i)) {
                        std::cout << "NULL\t";
                        continue;
                    }

                    switch (chunk->type_id()) {
                        case arrow::Type::INT32:
                            std::cout << std::static_pointer_cast<arrow::Int32Array>(chunk)->Value(i);
                            break;
                        case arrow::Type::INT64:
                            std::cout << std::static_pointer_cast<arrow::Int64Array>(chunk)->Value(i);
                            break;
                        case arrow::Type::FLOAT:
                            std::cout << std::static_pointer_cast<arrow::FloatArray>(chunk)->Value(i);
                            break;
                        case arrow::Type::DOUBLE:
                            std::cout << std::static_pointer_cast<arrow::DoubleArray>(chunk)->Value(i);
                            break;
                        case arrow::Type::BOOL:
                            std::cout << (std::static_pointer_cast<arrow::BooleanArray>(chunk)->Value(i)
                                              ? "true"
                                              : "false");
                            break;
                        case arrow::Type::STRING:
                            try {
                                auto str_array = std::static_pointer_cast<arrow::StringArray>(chunk);
                                if (i < str_array->length()) {
                                    std::cout << str_array->GetString(i);
                                } else {
                                    std::cout << "NULL";
                                }
                            } catch (const std::exception &e) {
                                std::cout << "ERROR";
                            }
                            break;
                        case arrow::Type::TIMESTAMP:
                            std::cout << std::static_pointer_cast<arrow::TimestampArray>(chunk)->Value(i);
                            break;
                        default:
                            std::cout << "Unsupported";
                    }
                    std::cout << "\t";
                }
                std::cout << std::endl;
            }
        } catch (const std::exception &e) {
            std::cout << "Error while printing table: " << e.what() << std::endl;
        }
    }

    // Utility function to create a table from nodes
    inline arrow::Result<std::shared_ptr<arrow::Table> > create_table(
        const std::vector<std::shared_ptr<Node> > &nodes,
        const std::vector<std::string> &field_names,
        int64_t chunk_size = 0) {
        if (nodes.empty()) {
            return arrow::Status::Invalid("Empty nodes list");
        }
        if (field_names.empty()) {
            return arrow::Status::Invalid("Empty field names list");
        }

        // Collect arrays for each field
        std::vector<std::vector<std::shared_ptr<arrow::Array> > > field_arrays(field_names.size());

        // For each field
        for (size_t field_idx = 0; field_idx < field_names.size(); ++field_idx) {
            const auto &field_name = field_names[field_idx];

            // For each node
            for (const auto &node: nodes) {
                auto field_result = node->get_field(field_name);
                if (!field_result.ok()) {
                    return field_result.status();
                }
                field_arrays[field_idx].push_back(field_result.ValueOrDie());
            }
        }

        // Create schema from the first node's fields
        std::vector<std::shared_ptr<arrow::Field> > schema_fields;
        for (size_t i = 0; i < field_names.size(); ++i) {
            auto first_array = field_arrays[i][0];
            schema_fields.push_back(arrow::field(field_names[i], first_array->type()));
        }
        auto schema = arrow::schema(schema_fields);

        // Concatenate arrays for each field
        std::vector<std::shared_ptr<arrow::ChunkedArray> > columns;
        for (const auto &arrays: field_arrays) {
            if (chunk_size <= 0) {
                // Single chunk - concatenate all arrays
                ARROW_ASSIGN_OR_RAISE(auto concat_array, arrow::Concatenate(arrays));
                columns.push_back(std::make_shared<arrow::ChunkedArray>(concat_array));
            } else {
                // Multiple chunks - create chunks of specified size
                std::vector<std::shared_ptr<arrow::Array> > chunks;
                int64_t total_length = 0;
                std::vector<std::shared_ptr<arrow::Array> > current_chunk;

                for (const auto &array: arrays) {
                    current_chunk.push_back(array);
                    total_length += array->length();

                    if (total_length >= chunk_size) {
                        ARROW_ASSIGN_OR_RAISE(auto concat_chunk, arrow::Concatenate(current_chunk));
                        chunks.push_back(concat_chunk);
                        current_chunk.clear();
                        total_length = 0;
                    }
                }

                // Handle remaining arrays
                if (!current_chunk.empty()) {
                    ARROW_ASSIGN_OR_RAISE(auto concat_chunk, arrow::Concatenate(current_chunk));
                    chunks.push_back(concat_chunk);
                }

                columns.push_back(std::make_shared<arrow::ChunkedArray>(chunks));
            }
        }

        // Create table
        return arrow::Table::Make(schema, columns);
    }

    arrow::Result<bool> demo_single_node();

    arrow::Result<bool> demo_batch_update();
}

#endif //CORE_HPP
