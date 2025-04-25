#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>
// #include <arrow/api.h>
#include "../include/core.hpp"
#include "../include/logger.hpp"
#include "../include/metadata.hpp"
#include "../include/query.hpp"

using namespace std::string_literals;
using namespace tundradb;

struct User {
  std::string name;
  int64_t age;
};

struct Company {
  std::string name;
  int64_t size;
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

std::vector<std::shared_ptr<Node>> create_companies(
    Database& db, const std::vector<Company>& companies) {
  std::vector<std::shared_ptr<Node>> nodes;
  for (auto company : companies) {
    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    name_builder.Append(company.name);
    name_builder.Finish(&name_array);

    // Create age array
    arrow::Int64Builder size_builder;
    std::shared_ptr<arrow::Array> size_array;
    size_builder.Append(company.size);
    size_builder.Finish(&size_array);

    std::unordered_map<std::string, std::shared_ptr<arrow::Array>> data = {
      {"name", name_array}, {"size", size_array}};

    auto node = db.create_node("companies", data).ValueOrDie();
    nodes.push_back(node);
  }

  return nodes;
}


std::shared_ptr<arrow::Schema> create_users_schema() {
  auto name_field = arrow::field("name", arrow::utf8());
  auto age_field = arrow::field("age", arrow::int64());
  return arrow::schema({name_field, age_field});
}


std::shared_ptr<arrow::Schema> create_companies_schema() {
  auto name_field = arrow::field("name", arrow::utf8());
  auto size_field = arrow::field("size", arrow::int64());
  return arrow::schema({name_field, size_field});
}


int main() {
  Logger::getInstance().setLevel(LogLevel::DEBUG);
  auto db_path = "test_db_" + std::to_string(now_millis());
  // auto db_path = "testdb_1744518985889565";
  auto config = make_config()
                    .with_db_path(db_path)
                    .with_shard_capacity(1000)
                    .with_chunk_size(1000)
                    .build();


  auto users_schema = create_users_schema();
  auto companies_schema = create_companies_schema();
  Database db(config);
  db.get_schema_registry()->add("users", users_schema).ValueOrDie();
  db.get_schema_registry()->add("companies", companies_schema).ValueOrDie();
  const auto users =
      std::vector({User{"alex", 25}, User{"bob", 31}, User{"jeff", 33},
                   User{"sam", 21}, User{"matt", 40}});
  create_users(db, users);
  const auto companies = std::vector{{Company{"ibm", 1000}, Company{"google", 3000}}};
  create_companies(db, companies);
  db.connect(1, "friend", 0).ValueOrDie();

  auto users_table = db.get_table("users").ValueOrDie();
  auto companies_table = db.get_table("companies").ValueOrDie();
  print_table(users_table);
  print_table(companies_table);

  auto query = Query::from_schema("users")
                   .where("age", CompareOp::Gt, Value(30))
                   .traverse("users", "friend", "friends")
                   .build();
  auto result = db.query(query).ValueOrDie();
  auto tables = result->tables();

  std::cout << "result:" << std::endl;
  for (auto& [schema_name, table] : tables) {
    std::cout << "schema_name = " << schema_name << std::endl;
    print_table(table);
    std::cout << "=============================" << std::endl;
    std::cout << std::endl;
  }

  return 0;
}
