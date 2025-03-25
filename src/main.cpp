#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>

#include "../include/core.hpp"

int main() {
  // tundradb::demo_single_node();
  // tundradb::demo_batch_update().ValueOrDie();
  // tundradb::demo_snapshot_creation().ValueOrDie();
  tundradb::load_shard_demo().ValueOrDie();
  return 0;
}
