//
// Created by Roman Pliashkou on 3/22/25.
//

#include "../include/core.hpp"
#include <iostream>
#include <chrono>

namespace tundradb {
    arrow::Result<std::shared_ptr<arrow::Array> > create_int64(const int64_t value) {
        arrow::Int64Builder int64_builder;
        ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
        ARROW_RETURN_NOT_OK(int64_builder.Append(value));
        std::shared_ptr<arrow::Array> int64_array;
        ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
        return int64_array;
    }

    arrow::Result<bool> demo_single_node() {
        std::cout << "demo_single_node:\n" << std::endl;
        arrow::Int64Builder int64_builder;
        ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
        ARROW_RETURN_NOT_OK(int64_builder.Append(0));
        std::shared_ptr<arrow::Array> int64_array;
        ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));

        tundradb::Node node(0);
        node.add_field("int64", int64_array);

        arrow::Int64Builder int64_update_builder;
        ARROW_RETURN_NOT_OK(int64_update_builder.Reserve(1));
        ARROW_RETURN_NOT_OK(int64_update_builder.Append(1));
        std::shared_ptr<arrow::Array> int64_update_array;
        ARROW_RETURN_NOT_OK(int64_update_builder.Finish(&int64_update_array));

        tundradb::SetOperation operation(0, {"int64"}, int64_update_array);

        node.update(operation).ValueOrDie();
        auto int64_field = node.get_field("int64").ValueOrDie();
        std::cout << "int64=" << std::static_pointer_cast<arrow::Int64Array>(int64_field)->Value(0) << std::endl;
        return {true};
    }

    arrow::Result<bool> demo_batch_update() {
        int nodes_count = 1000000;
        std::vector<std::shared_ptr<tundradb::Node>> nodes;
        nodes.reserve(nodes_count);

        for (int i = 0; i < nodes_count; i++) {
            auto node = std::make_shared<Node>(i);
            node->add_field("id", create_int64(i).ValueOrDie());
            node->add_field("int64", create_int64(0).ValueOrDie());
            nodes.emplace_back(node);
        }
        tundradb::SetOperation operation(0, {"int64"}, create_int64(1).ValueOrDie());

        // Measure time
        auto start = std::chrono::high_resolution_clock::now();
        
        for (auto node: nodes) {
            node->update(operation).ValueOrDie();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        // Calculate and print metrics
        double seconds = duration.count() / 1000.0;
        double ups = nodes_count / seconds;
        
        std::cout << "\nPerformance Metrics:" << std::endl;
        std::cout << "Total updates: " << nodes_count << std::endl;
        std::cout << "Time taken: " << seconds << " seconds" << std::endl;
        std::cout << "Updates per second: " << static_cast<int64_t>(ups) << " UPS" << std::endl;

        auto table = create_table(nodes, {"id", "int64"}, 10000).ValueOrDie();
        print_table(table);
        
        return true;
    }
}
