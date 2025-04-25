#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>
// #include <arrow/api.h>
#include "../include/core.hpp"
#include "../include/metadata.hpp"
#include "../include/query.hpp"

using namespace std::string_literals;
using namespace tundradb;

struct User {
  std::string name;
  int64_t age;
};

std::vector<std::shared_ptr<Node>> create_users(
    Database& db, const std::vector<User>& users) {
  std::vector<std::shared_ptr<Node>> nodes;
  for (auto user : users) {
    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    name_builder.Append(user.name);
    name_builder.Finish(&name_array);

    // Create age array
    arrow::Int64Builder age_builder;
    std::shared_ptr<arrow::Array> age_array;
    age_builder.Append(user.age);
    age_builder.Finish(&age_array);

    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
        {"name", name_array}, {"age", age_array}};

    auto node = db.create_node("users", data).ValueOrDie();
    nodes.push_back(node);
  }

  return nodes;
}

int main() {
  auto db_path = "test_db_" + std::to_string(now_millis());
  // auto db_path = "testdb_1744518985889565";
  auto config = make_config()
                    .with_db_path(db_path)
                    .with_shard_capacity(1000)
                    .with_chunk_size(1000)
                    .build();

  auto name_field = arrow::field("name", arrow::utf8());
  auto age_field = arrow::field("age", arrow::int64());
  auto schema = arrow::schema({name_field, age_field});

  Database db(config);
  db.get_schema_registry()->add("users", schema).ValueOrDie();
  const auto users =
      std::vector({User{"alex", 25}, User{"bob", 31}, User{"jeff", 33},
                   User{"sam", 21}, User{"matt", 40}});
  create_users(db, users);

  auto table = db.get_table("users").ValueOrDie();
  print_table(table);

  auto query = Query::from_schema("users")
                   .where("age", CompareOp::Gt, Value(30))
                   .build();
  auto result = db.query(query).ValueOrDie();
  auto tables =  result.tables();

  std::cout << "result:" << std::endl;
  for (auto &[schema_name, table] : tables) {
    std::cout << schema_name << std::endl;
    print_table(table);
  }


  return 0;
}
