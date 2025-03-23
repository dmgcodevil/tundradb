#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>

#include "../include/core.hpp"

int main() {
  // tundradb::demo_single_node();
  tundradb::demo_batch_update().ValueOrDie();
  return 0;
}
