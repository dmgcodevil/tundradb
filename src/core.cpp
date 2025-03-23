//
// Created by Roman Pliashkou on 3/22/25.
//

#include "../include/core.hpp"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <future>

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

    void update_batch(const std::vector<std::shared_ptr<Node>>& nodes, 
                     size_t start, size_t end, 
                     const SetOperation& operation) {
        for (size_t i = start; i < end; ++i) {
            nodes[i]->update(operation).ValueOrDie();
        }
    }

    arrow::Result<bool> demo_batch_update() {
        int nodes_count = 1000000;
        int num_threads = 8;  // Number of threads to use

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
        
        // Calculate batch size for each thread
        size_t batch_size = nodes_count / num_threads;
        std::vector<std::future<void>> futures;

        // Launch threads
        for (int t = 0; t < num_threads; ++t) {
            size_t start_idx = t * batch_size;
            size_t end_idx = (t == num_threads - 1) ? nodes_count : (t + 1) * batch_size;
            
            futures.push_back(
                std::async(std::launch::async,
                          update_batch,
                          std::ref(nodes),
                          start_idx,
                          end_idx,
                          std::ref(operation))
            );
        }

        // Wait for all threads to complete
        for (auto& future : futures) {
            future.wait();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        // Calculate and print metrics
        double seconds = duration.count() / 1000.0;
        double ups = nodes_count / seconds;
        
        std::cout << "\nPerformance Metrics:" << std::endl;
        std::cout << "Total updates: " << nodes_count << std::endl;
        std::cout << "Number of threads: " << num_threads << std::endl;
        std::cout << "Time taken: " << seconds << " seconds" << std::endl;
        std::cout << "Updates per second: " << static_cast<int64_t>(ups) << " UPS" << std::endl;
        std::cout << "Updates per second per thread: " << static_cast<int64_t>(ups/num_threads) << " UPS/thread" << std::endl;

        auto table = create_table(nodes, {"id", "int64"}, 10000).ValueOrDie();
        print_table(table);
        
        return true;
    }
}
