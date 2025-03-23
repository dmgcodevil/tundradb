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
    class Database;
    class Node;

    static arrow::Result<std::shared_ptr<arrow::Array> > create_int64_array(const int64_t value) {
        arrow::Int64Builder int64_builder;
        ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
        ARROW_RETURN_NOT_OK(int64_builder.Append(value));
        std::shared_ptr<arrow::Array> int64_array;
        ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
        return int64_array;
    }

    static arrow::Result<std::shared_ptr<arrow::Array> > create_str_array(const std::string &value) {
        arrow::StringBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Append(value));
        std::shared_ptr<arrow::Array> string_arr;
        ARROW_RETURN_NOT_OK(builder.Finish(&string_arr));
        return string_arr;
    }


    static arrow::Result<std::shared_ptr<arrow::Array> > create_null_array(
        const std::shared_ptr<arrow::DataType> &type) {
        switch (type->id()) {
            // Use type->id() which returns Type::type enum
            case arrow::Type::INT64: {
                // Use blocks {} to scope variables
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
                return arrow::Status::NotImplemented("Unsupported type: ", type->ToString());
        }
    }

    enum OperationType {
        SET
    };

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

        virtual OperationType op_type() const = 0;

        virtual bool should_replace_array() const { return false; }
        virtual std::shared_ptr<arrow::Array> get_replacement_array() const { return nullptr; }

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
        OperationType op_type() const override {
            return OperationType::SET;
        }
        bool should_replace_array() const override {
            return value->type_id() == arrow::Type::STRING;
        }
        std::shared_ptr<arrow::Array> get_replacement_array() const override {
            return value; // copying pointer is 2x faster than creating a new string array
            //  1024590 UPS vs 2044989 UPS (copy ptr)
            // return create_str_array(value->ToString()).ValueOrDie();
        }
    };


    class Node {
    private:
        std::unordered_map<std::string, std::shared_ptr<arrow::Array> > data;

    public:
        int64_t id;
        std::string schema_name;

        explicit Node(const int64_t id, std::string schema_name,
                      std::unordered_map<std::string, std::shared_ptr<arrow::Array> > initial_data): id(id),
            schema_name(std::move(schema_name)), data(std::move(initial_data)) {
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

        arrow::Result<bool> update(const std::shared_ptr<BaseOperation> &update) {
            if (update->field_name.empty()) {
                return arrow::Status::Invalid("Field name vector is empty");
            }

            auto it = data.find(update->field_name[0]);
            if (it == data.end()) {
                return arrow::Status::KeyError("Field not found: ", update->field_name[0]);
            }
            if (update->should_replace_array()) {
                data[update->field_name[0]] = update->get_replacement_array();
                return true;
            }
            return update->apply(it->second, 0);

        }
    };


    class Database {
    private:
        std::unordered_map<std::string, std::shared_ptr<arrow::Schema> > schema_registry;
        // Memory pool for nodes
        std::pmr::monotonic_buffer_resource nodes_memory_pool;

        // Nodes storage using the pool
        std::pmr::unordered_map<std::string, std::pmr::unordered_map<int64_t, std::shared_ptr<Node> > > nodes;
        std::atomic<int64_t> id_counter;

    public:
        explicit Database(size_t nodes_buffer_size = 1024 * 1024 * 1024) // 1GB default
            : nodes_memory_pool(nodes_buffer_size) // Fixed size pool for nodes
              , nodes(&nodes_memory_pool), id_counter(0) // Use the pool for nodes map
        {
        }

        arrow::Result<std::shared_ptr<arrow::Schema> > prepend_id_field(
            const std::shared_ptr<arrow::Schema> &schema) {
            // Create ID field (always non-nullable)
            auto id_field = arrow::field("id", arrow::int64(), false);

            // Get existing fields
            auto fields = schema->fields();

            // Check if ID field already exists
            if (schema->GetFieldIndex("id") != -1) {
                return arrow::Status::Invalid("Schema already contains 'id' field");
            }

            // Create new vector and reserve space
            std::vector<std::shared_ptr<arrow::Field> > new_fields;
            new_fields.reserve(fields.size() + 1);

            // Add ID field first
            new_fields.push_back(id_field);

            // Add existing fields
            new_fields.insert(new_fields.end(), fields.begin(), fields.end());

            // Create new schema
            return arrow::schema(new_fields);
        }

        // Register a new schema
        arrow::Result<bool> register_schema(const std::string &name, const std::shared_ptr<arrow::Schema> &schema) {
            if (name.empty()) {
                return arrow::Status::Invalid("Schema name cannot be empty");
            }
            if (!schema) {
                return arrow::Status::Invalid("Schema cannot be null");
            }

            if (schema->GetFieldIndex("id") != -1) {
                return arrow::Status::Invalid("'id' field name is reserved");
            }
            ARROW_ASSIGN_OR_RAISE(auto final_schema, prepend_id_field(schema));
            auto [it, inserted] = schema_registry.try_emplace(name, final_schema);
            if (!inserted) {
                return arrow::Status::AlreadyExists("Schema '", name, "' already exists");
            }
            return true;
        }

        // Check if a schema exists
        bool has_schema(const std::string &name) const {
            return schema_registry.contains(name);
        }

        // Get a schema by name
        arrow::Result<std::shared_ptr<arrow::Schema> > get_schema(const std::string &name) const {
            auto it = schema_registry.find(name);
            if (it == schema_registry.end()) {
                return arrow::Status::KeyError("Schema '", name, "' not found");
            }
            return it->second;
        }

        // Update existing schema
        arrow::Result<bool> update_schema(const std::string &name, const std::shared_ptr<arrow::Schema> &schema) {
            if (name.empty()) {
                return arrow::Status::Invalid("Schema name cannot be empty");
            }
            if (!schema) {
                return arrow::Status::Invalid("Schema cannot be null");
            }

            auto it = schema_registry.find(name);
            if (it == schema_registry.end()) {
                return arrow::Status::KeyError("Schema '", name, "' not found");
            }

            it->second = schema;
            return true;
        }

        // Remove a schema
        arrow::Result<bool> remove_schema(const std::string &name) {
            if (name.empty()) {
                return arrow::Status::Invalid("Schema name cannot be empty");
            }

            if (schema_registry.erase(name) == 0) {
                return arrow::Status::KeyError("Schema '", name, "' not found");
            }
            return true;
        }

        // Get all schema names
        [[nodiscard]] std::vector<std::string> get_schema_names() const {
            std::vector<std::string> names;
            names.reserve(schema_registry.size());
            for (const auto &[name, _]: schema_registry) {
                names.push_back(name);
            }
            return names;
        }



        arrow::Result<std::shared_ptr<Node> > create_node(const std::string &schema_name,
                                                          std::unordered_map<std::string, std::shared_ptr<
                                                              arrow::Array> > &data) {
            if (schema_name.empty()) {
                return arrow::Status::Invalid("Schema name cannot be empty");
            }
            ARROW_ASSIGN_OR_RAISE(auto schema, get_schema(schema_name));
            if (data.contains("id")) {
                return arrow::Status::Invalid("'id' column is auto generated");
            }
            std::unordered_map<std::string, std::shared_ptr<arrow::Array> > normalized_data;
            for (auto field: schema->fields()) {
                if (field->name() != "id" && !field->nullable() && (!data.contains(field->name()) ||
                                                                    data.find(field->name())->second->IsNull(0))) {
                    return arrow::Status::Invalid("Field '", field->name(), "' is required");
                }
                if (!data.contains(field->name())) {
                    normalized_data[field->name()] = create_null_array(field->type()).ValueOrDie();
                } else {
                    auto array = data.find(field->name())->second;
                    if (!array->type()->Equals(field->type())) {
                        return arrow::Status::Invalid("Type mismatch for field '", field->name(),
                                                      "'. Expected ", field->type()->ToString(),
                                                      " but got ", array->type()->ToString());
                    }
                    normalized_data[field->name()] = array;
                }
            }
            auto id = id_counter.fetch_add(1);
            normalized_data["id"] = create_int64_array(id).ValueOrDie();
            auto node = std::make_shared<Node>(id, schema_name, normalized_data);
            nodes[schema_name].insert(std::make_pair(id, node));
            return node;
        }

        arrow::Result<std::vector<std::shared_ptr<Node> > > get_nodes(const std::string &schema_name) {
            if (!has_schema(schema_name)) {
                return arrow::Status::Invalid("Schema '", schema_name, "' not found");
            }
            std::vector<std::shared_ptr<Node> > result;
            std::ranges::transform(nodes[schema_name], std::back_inserter(result),
                                   [](const auto &pair) { return pair.second; });
            return result;
        }
    };


    // Helper function to print a single row
    inline void print_row(const std::shared_ptr<arrow::Table> &table, int64_t row_index) {
        for (int j = 0; j < table->num_columns(); ++j) {
            auto column = table->column(j);
            if (!column || column->num_chunks() == 0) {
                std::cout << "NULL\t";
                continue;
            }

            // Find the chunk containing this row
            int64_t accumulated_length = 0;
            std::shared_ptr<arrow::Array> chunk;
            int64_t chunk_offset = row_index;

            for (int c = 0; c < column->num_chunks(); ++c) {
                chunk = column->chunk(c);
                if (accumulated_length + chunk->length() > row_index) {
                    chunk_offset = row_index - accumulated_length;
                    break;
                }
                accumulated_length += chunk->length();
            }

            if (!chunk || chunk_offset >= chunk->length() || chunk->IsNull(chunk_offset)) {
                std::cout << "NULL\t";
                continue;
            }

            switch (chunk->type_id()) {
                case arrow::Type::INT32:
                    std::cout << std::static_pointer_cast<arrow::Int32Array>(chunk)->Value(chunk_offset);
                    break;
                case arrow::Type::INT64:
                    std::cout << std::static_pointer_cast<arrow::Int64Array>(chunk)->Value(chunk_offset);
                    break;
                case arrow::Type::FLOAT:
                    std::cout << std::static_pointer_cast<arrow::FloatArray>(chunk)->Value(chunk_offset);
                    break;
                case arrow::Type::DOUBLE:
                    std::cout << std::static_pointer_cast<arrow::DoubleArray>(chunk)->Value(chunk_offset);
                    break;
                case arrow::Type::BOOL:
                    std::cout << (std::static_pointer_cast<arrow::BooleanArray>(chunk)->Value(chunk_offset)
                                      ? "true"
                                      : "false");
                    break;
                case arrow::Type::STRING:
                    try {
                        auto str_array = std::static_pointer_cast<arrow::StringArray>(chunk);
                        if (chunk_offset < str_array->length()) {
                            std::cout << str_array->GetString(chunk_offset);
                        } else {
                            std::cout << "NULL";
                        }
                    } catch (const std::exception &e) {
                        std::cout << "ERROR";
                    }
                    break;
                case arrow::Type::TIMESTAMP:
                    std::cout << std::static_pointer_cast<arrow::TimestampArray>(chunk)->Value(chunk_offset);
                    break;
                default:
                    std::cout << "Unsupported";
            }
            std::cout << "\t";
        }
        std::cout << std::endl;
    }

    inline void print_table(const std::shared_ptr<arrow::Table> &table, int64_t max_rows = 100) {
        if (!table) {
            std::cout << "Null table" << std::endl;
            return;
        }

        std::cout << "Table Schema:" << std::endl;
        std::cout << table->schema()->ToString() << std::endl;

        // Print chunk information
        std::cout << "\nChunk Information:" << std::endl;
        for (int j = 0; j < table->num_columns(); ++j) {
            auto column = table->column(j);
            std::cout << "Column '" << table->schema()->field(j)->name() << "': "
                    << column->num_chunks() << " chunk size = " << column->chunk(0)->length() << std::endl;

            // for (int c = 0; c < column->num_chunks(); ++c) {
            //     std::cout << column->chunk(c)->length();
            //     if (c < column->num_chunks() - 1) std::cout << ", ";
            // }
            // std::cout << " ]" << std::endl;
        }

        const int64_t total_rows = table->num_rows();
        std::cout << "\nTable Data (" << total_rows << " rows):" << std::endl;

        try {
            // Determine how many rows to print
            bool use_ellipsis = max_rows > 0 && total_rows > max_rows + 1;
            int64_t rows_to_print = use_ellipsis ? max_rows : total_rows;

            // Print rows up to max_rows
            for (int64_t i = 0; i < rows_to_print; ++i) {
                print_row(table, i);
            }

            // Print ellipsis and last row if needed
            if (use_ellipsis) {
                std::cout << "....\t" << std::endl;
                print_row(table, total_rows - 1);
            }
        } catch (const std::exception &e) {
            std::cout << "Error while printing table: " << e.what() << std::endl;
        }
    }


    // Utility function to create a table from nodes
    inline arrow::Result<std::shared_ptr<arrow::Table> > create_table(
        const std::shared_ptr<arrow::Schema> &schema,
        const std::vector<std::shared_ptr<Node> > &nodes,
        int64_t chunk_size = 0) {
        if (nodes.empty()) {
            return arrow::Status::Invalid("Empty nodes list");
        }
        auto field_names = schema->field_names();
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
