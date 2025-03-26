#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>
// #include <arrow/api.h>
#include "../include/core.hpp"
#include "../include/metadata.hpp"

using namespace std::string_literals;
using namespace tundradb;


void demote_write_manifest() {
  MetadataManager manager("./tundradb_test");
  manager.initialize();
  Manifest manifest;
  manifest.id = "m1"s;
  manifest.shards = std::vector<ShardMetadata>();
  ShardMetadata shard;
  shard.chunk_size = 1024;
  shard.data_file = "./data"s;
  shard.min_id = 1;
  shard.max_id = 2;
  shard.record_count = 100;
  shard.shard_id = "2"s;
  shard.timestamp_ms = 123456;
  shard.schema_name = "users";
  manifest.shards.push_back(shard);
  manager.write_manifest(manifest).ValueOrDie();
}

void demo_read_manifest() {
  MetadataManager manager("./tundradb_test");
  manager.initialize();
  Manifest manifest = manager.read_manifest("m1").ValueOrDie();
  std::cout << manifest;
  std::cout << manifest.id << std::endl;
  std::cout << manifest.shards[0].shard_id << std::endl;
}

int main() {
  // tundradb::demo_single_node();
  // tundradb::demo_batch_update().ValueOrDie();
  // tundradb::demo_snapshot_creation().ValueOrDie();
  // tundradb::load_shard_demo().ValueOrDie();
  // demote_write_manifest();
  demo_read_manifest();
  return 0;
}
