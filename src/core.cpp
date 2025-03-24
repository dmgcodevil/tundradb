#include "../include/core.hpp"

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

namespace tundradb {
// arrow::Result<bool> demo_single_node() {
//     std::cout << "demo_single_node:\n" << std::endl;
//     arrow::Int64Builder int64_builder;
//     ARROW_RETURN_NOT_OK(int64_builder.Reserve(1));
//     ARROW_RETURN_NOT_OK(int64_builder.Append(0));
//     std::shared_ptr<arrow::Array> int64_array;
//     ARROW_RETURN_NOT_OK(int64_builder.Finish(&int64_array));
//
//     tundradb::Node node(0, "test-schema", {});
//     node.add_field("int64", int64_array);
//
//     arrow::Int64Builder int64_update_builder;
//     ARROW_RETURN_NOT_OK(int64_update_builder.Reserve(1));
//     ARROW_RETURN_NOT_OK(int64_update_builder.Append(1));
//     std::shared_ptr<arrow::Array> int64_update_array;
//     ARROW_RETURN_NOT_OK(int64_update_builder.Finish(&int64_update_array));
//
//     tundradb::SetOperation operation(0, {"int64"}, int64_update_array);
//
//     node.update(operation).ValueOrDie();
//     auto int64_field = node.get_field("int64").ValueOrDie();
//     std::cout << "int64=" <<
//     std::static_pointer_cast<arrow::Int64Array>(int64_field)->Value(0) <<
//     std::endl; return {true};
// }

void update_node_batch(
    Database& db, const std::vector<int64_t>& node_ids, size_t start,
    size_t end,
    const std::vector<std::shared_ptr<tundradb::BaseOperation>>&
        update_templates) {
  for (size_t i = start; i < end; ++i) {
    int64_t node_id = node_ids[i];

    // Create operation clones with the correct node ID
    for (const auto& update_template : update_templates) {
      // Create a new operation with the same field and value but with this
      // node's ID
      if (auto set_op =
              std::dynamic_pointer_cast<SetOperation>(update_template)) {
        auto op = std::make_shared<SetOperation>(node_id, set_op->field_name,
                                                 set_op->value);

        db.update_node(op).ValueOrDie();
      }
    }
  }
}

arrow::Result<bool> demo_batch_update() {
  std::cout << "\nRunning batch update demo with sharding..." << std::endl;

  int nodes_count = 1000000;
  int num_threads = 8;  // Number of threads to use
  Database database;

  auto name_field = arrow::field("name", arrow::utf8());
  auto name2_field = arrow::field("name2", arrow::utf8());
  auto count_field = arrow::field("count", arrow::int64());
  auto count2_field = arrow::field("count2", arrow::int64());

  // Create schema
  auto schema =
      arrow::schema({count_field, name_field, count2_field, name2_field});
  ARROW_RETURN_NOT_OK(
      database.get_schema_registry()->add("test-schema", schema));

  // Keep track of node IDs for later updates
  std::vector<int64_t> node_ids;
  node_ids.reserve(nodes_count);

  std::cout << "Creating " << nodes_count << " nodes..." << std::endl;
  // Create nodes with initial data
  for (int i = 0; i < nodes_count; i++) {
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> fields = {
        {"name2", create_str_array("@").ValueOrDie()},
        {"name", create_str_array("*").ValueOrDie()},
        {"count", create_int64_array(0).ValueOrDie()},
        {"count2", create_int64_array(0).ValueOrDie()}};

    auto node = database.create_node("test-schema", fields).ValueOrDie();
    node_ids.push_back(node->id);
  }

  // Create operation templates (with node_id=0, will be replaced in threads)
  std::vector<std::string> count_field_name = {"count"};
  std::vector<std::string> count2_field_name = {"count2"};
  std::vector<std::string> name_field_name = {"name"};
  std::vector<std::string> name2_field_name = {"name2"};

  auto update_count = std::make_shared<tundradb::SetOperation>(
      0, count_field_name, create_int64_array(1).ValueOrDie());
  auto update_count2 = std::make_shared<tundradb::SetOperation>(
      0, count2_field_name, create_int64_array(2).ValueOrDie());
  auto update_name = std::make_shared<tundradb::SetOperation>(
      0, name_field_name, create_str_array("tundra").ValueOrDie());
  auto update_name2 = std::make_shared<tundradb::SetOperation>(
      0, name2_field_name, create_str_array("db").ValueOrDie());

  std::vector<std::shared_ptr<tundradb::BaseOperation>> update_templates = {
      update_count, update_count2, update_name, update_name2};

  std::cout << "Running updates on " << num_threads << " threads..."
            << std::endl;
  // Measure time
  auto start = std::chrono::high_resolution_clock::now();

  // Calculate batch size for each thread
  size_t batch_size = nodes_count / num_threads;
  std::vector<std::future<void>> futures;

  // Launch threads
  for (int t = 0; t < num_threads; ++t) {
    size_t start_idx = t * batch_size;
    size_t end_idx =
        (t == num_threads - 1) ? nodes_count : (t + 1) * batch_size;

    futures.push_back(std::async(
        std::launch::async, update_node_batch, std::ref(database),
        std::ref(node_ids), start_idx, end_idx, std::ref(update_templates)));
  }

  // Wait for all threads to complete
  for (auto& future : futures) {
    future.wait();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // Calculate and print metrics
  double seconds = duration.count() / 1000.0;
  double updates_count =
      nodes_count * update_templates.size();  // 4 updates per node
  double ups = updates_count / seconds;

  std::cout << "\nPerformance Metrics:" << std::endl;
  std::cout << "Total nodes: " << nodes_count << std::endl;
  std::cout << "Total updates: " << updates_count << std::endl;
  std::cout << "Number of threads: " << num_threads << std::endl;
  std::cout << "Time taken: " << seconds << " seconds" << std::endl;
  std::cout << "Updates per second: " << static_cast<int64_t>(ups) << " UPS"
            << std::endl;
  std::cout << "Updates per second per thread: "
            << static_cast<int64_t>(ups / num_threads) << " UPS/thread"
            << std::endl;

  std::cout << "\nRetrieving and printing table..." << std::endl;
  // Get and print the table directly from the database
  auto table = database.get_table("test-schema", 100000).ValueOrDie();
  print_table(table);

  return true;
}
}  // namespace tundradb
