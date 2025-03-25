#include "../include/core.hpp"

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
namespace fs = std::filesystem;

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
// Helper function to verify a file exists
bool file_exists(const std::string& path) {
  std::ifstream f(path);
  return f.good();
}

arrow::Result<bool> demo_snapshot_creation() {
  std::cout << "Starting snapshot creation test" << std::endl;

  // Create a temporary directory for the test
  // std::string temp_dir = fs::temp_directory_path().string() +
  // "/tundradb_test_" +
  std::string temp_dir =
      "./tundradb_test_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count());

  std::cout << "Using temporary directory: " << temp_dir << std::endl;

  // Create the directory if it doesn't exist
  fs::create_directories(temp_dir);

  // Create database with persistence enabled
  auto config = make_config()
                    .with_data_directory(temp_dir)
                    .with_persistence_enabled(true)
                    .build();

  Database db(config);

  // Create schemas
  auto schema_registry = db.get_schema_registry();

  // Create "users" schema
  auto user_fields = std::vector<std::shared_ptr<arrow::Field>>{
      arrow::field("name", arrow::utf8()), arrow::field("age", arrow::int64())};
  auto user_schema = arrow::schema(user_fields);
  ARROW_RETURN_NOT_OK(schema_registry->add("users", user_schema));

  // Create "products" schema
  // auto product_fields = std::vector<std::shared_ptr<arrow::Field>>{
  //   arrow::field("title", arrow::utf8()),
  //   arrow::field("price", arrow::int64())
  // };
  // auto product_schema = arrow::schema(product_fields);
  // ARROW_RETURN_NOT_OK(schema_registry->add("products", product_schema));

  // Create user nodes
  for (int i = 0; i < 10; i++) {
    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data;

    auto name_builder = arrow::StringBuilder();
    ARROW_RETURN_NOT_OK(name_builder.Append("User " + std::to_string(i)));
    std::shared_ptr<arrow::Array> name_array;
    ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
    data["name"] = name_array;

    auto age_builder = arrow::Int64Builder();
    ARROW_RETURN_NOT_OK(age_builder.Append(20 + i));
    std::shared_ptr<arrow::Array> age_array;
    ARROW_RETURN_NOT_OK(age_builder.Finish(&age_array));
    data["age"] = age_array;

    ARROW_RETURN_NOT_OK(db.create_node("users", data));
  }

  // Create product nodes
  // for (int i = 0; i < 5; i++) {
  //   std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data;
  //
  //   auto title_builder = arrow::StringBuilder();
  //   ARROW_RETURN_NOT_OK(title_builder.Append("Product " +
  //   std::to_string(i))); std::shared_ptr<arrow::Array> title_array;
  //   ARROW_RETURN_NOT_OK(title_builder.Finish(&title_array));
  //   data["title"] = title_array;
  //
  //   auto price_builder = arrow::DoubleBuilder();
  //   ARROW_RETURN_NOT_OK(price_builder.Append(10.0 * (i + 1)));
  //   std::shared_ptr<arrow::Array> price_array;
  //   ARROW_RETURN_NOT_OK(price_builder.Finish(&price_array));
  //   data["price"] = price_array;
  //
  //   ARROW_RETURN_NOT_OK(db.create_node("products", data));
  // }

  // Create a snapshot
  std::cout << "Creating snapshot..." << std::endl;
  ARROW_RETURN_NOT_OK(db.create_snapshot());

  // Check files were created
  // We would expect to have:
  // 1. At least one metadata file for each schema
  // 2. At least one data file for each schema

  // Check the directory exists and has files
  if (!fs::exists(temp_dir)) {
    std::cerr << "Error: Data directory doesn't exist after snapshot creation"
              << std::endl;
    return false;
  }

  // Count files by extension
  int metadata_files = 0;
  int parquet_files = 0;

  std::cout << "Files created:" << std::endl;
  for (const auto& entry : fs::recursive_directory_iterator(temp_dir)) {
    if (entry.is_regular_file()) {
      std::string path = entry.path().string();
      std::cout << "  " << path << std::endl;

      if (path.find(".metadata.json") != std::string::npos) {
        metadata_files++;
      } else if (path.find(".parquet") != std::string::npos) {
        parquet_files++;
      }
    }
  }

  std::cout << "Found " << metadata_files << " metadata files and "
            << parquet_files << " parquet files" << std::endl;

  // We should have at least one metadata file and one parquet file
  if (metadata_files == 0) {
    std::cerr << "Error: No metadata files were created" << std::endl;
    return false;
  }

  if (parquet_files == 0) {
    std::cerr << "Error: No parquet files were created" << std::endl;
    return false;
  }

  // Check if we have files for both schemas
  bool has_users_files = false;
  bool has_products_files = false;

  for (const auto& entry : fs::recursive_directory_iterator(temp_dir)) {
    if (entry.is_regular_file()) {
      std::string path = entry.path().string();
      if (path.find("users-") != std::string::npos) {
        has_users_files = true;
      } else if (path.find("products-") != std::string::npos) {
        has_products_files = true;
      }
    }
  }

  if (!has_users_files) {
    std::cerr << "Error: No files for 'users' schema found" << std::endl;
    return false;
  }

  // if (!has_products_files) {
  //   std::cerr << "Error: No files for 'products' schema found" << std::endl;
  //   return false;
  // }

  std::cout << "Snapshot creation test passed successfully!" << std::endl;

  // Clean up the temporary directory
  // fs::remove_all(temp_dir);

  return true;
}

arrow::Result<bool> load_shard_demo() {
  std::string temp_dir =
      "./tundradb_test_" +
      std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count());

  std::cout << "Using temporary directory: " << temp_dir << std::endl;

  // Create the directory if it doesn't exist
  fs::create_directories(temp_dir);

  // Create database with persistence enabled
  auto config = make_config()
                    .with_data_directory(temp_dir)
                    .with_persistence_enabled(true)
                    .build();

  Database db(config);
  // Create schemas
  auto schema_registry = db.get_schema_registry();

  // Create "users" schema
  auto user_fields = std::vector<std::shared_ptr<arrow::Field>>{
      arrow::field("name", arrow::utf8()), arrow::field("age", arrow::int64())};
  auto user_schema = arrow::schema(user_fields);
  ARROW_RETURN_NOT_OK(schema_registry->add("users", user_schema));

  // // Create "products" schema
  // auto product_fields = std::vector<std::shared_ptr<arrow::Field>>{
  //   arrow::field("title", arrow::utf8()),
  //   arrow::field("price", arrow::float64())
  // };
  // auto product_schema = arrow::schema(product_fields);
  // ARROW_RETURN_NOT_OK(schema_registry->add("products", product_schema));
  db.load_shard(
        "/Users/dmgcodevil/dev/cpp/tundradb-v1/tundradb/build/"
        "tundradb_test_1742878743153080/users-0-0.metadata.json")
      .ValueOrDie();

  // Create a snapshot
  std::cout << "Creating snapshot..." << std::endl;
  ARROW_RETURN_NOT_OK(db.create_snapshot());
}

}  // namespace tundradb
