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
  auto config = make_config().with_data_directory("./testdb").build();
  Database database(config);
  database.initialize().ValueOrDie();
  return 0;
}
