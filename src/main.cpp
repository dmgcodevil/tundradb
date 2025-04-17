#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>
// #include <arrow/api.h>
#include "../include/core.hpp"
#include "../include/metadata.hpp"

using namespace std::string_literals;
using namespace tundradb;

int main() {

  auto db_path = "testdb_1744518985889565";
  auto config = make_config()
                    .with_db_path(db_path)
                    .with_shard_capacity(1000)
                    .with_chunk_size(1000)
                    .build();

  auto name_field = arrow::field("name", arrow::utf8());
  auto age_field = arrow::field("age", arrow::int64());
  auto schema = arrow::schema({name_field, age_field});

  Database db(config);

  return 0;
}
