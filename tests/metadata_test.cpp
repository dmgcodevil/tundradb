#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "../include/metadata.hpp"
using namespace std::string_literals;
using namespace tundradb;

class MetadataManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary test directory
        test_dir = "./test_metadata_dir_"+std::to_string(
          std::chrono::system_clock::now().time_since_epoch().count());
        std::filesystem::remove_all(test_dir);
        manager = std::make_unique<MetadataManager>(test_dir);
    }

    void TearDown() override {
        // Clean up the test directory
        std::filesystem::remove_all(test_dir);
    }

    std::string test_dir;
    std::unique_ptr<MetadataManager> manager;
};

TEST_F(MetadataManagerTest, Initialize) {
    auto result = manager->initialize();
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.ValueOrDie());
    
    // Check if directory was created
    ASSERT_TRUE(std::filesystem::exists(test_dir + "/manifest"));
}

TEST_F(MetadataManagerTest, WriteAndReadManifest) {
    ASSERT_TRUE(manager->initialize().ok());

    // Create test manifest
    Manifest manifest;
    manifest.id = "test_manifest"s;
    manifest.shards = std::vector<ShardMetadata>();
    
    ShardMetadata shard;
    shard.shard_id = "shard1"s;
    shard.schema_name = "test_schema"s;
    shard.min_id = 1;
    shard.max_id = 100;
    shard.record_count = 100;
    shard.chunk_size = 1024;
    shard.data_file = "data.parquet"s;
    shard.timestamp_ms = 1234567890;
    
    manifest.shards.push_back(shard);

    // Write manifest
    auto write_result = manager->write_manifest(manifest);
    ASSERT_TRUE(write_result.ok());
    ASSERT_TRUE(std::filesystem::exists(write_result.ValueOrDie()));

    // Read manifest
    auto read_result = manager->read_manifest("test_manifest");
    ASSERT_TRUE(read_result.ok());
    
    Manifest read_manifest = read_result.ValueOrDie();
    ASSERT_EQ(read_manifest.id, manifest.id);
    ASSERT_EQ(read_manifest.shards.size(), 1);
    
    const auto& read_shard = read_manifest.shards[0];
    ASSERT_EQ(read_shard.shard_id, shard.shard_id);
    ASSERT_EQ(read_shard.schema_name, shard.schema_name);
    ASSERT_EQ(read_shard.min_id, shard.min_id);
    ASSERT_EQ(read_shard.max_id, shard.max_id);
    ASSERT_EQ(read_shard.record_count, shard.record_count);
    ASSERT_EQ(read_shard.chunk_size, shard.chunk_size);
    ASSERT_EQ(read_shard.data_file, shard.data_file);
    ASSERT_EQ(read_shard.timestamp_ms, shard.timestamp_ms);
}

TEST_F(MetadataManagerTest, WriteAndReadMetadata) {
    ASSERT_TRUE(manager->initialize().ok());

    // Create test metadata
    Metadata metadata;
    metadata.id = "test_metadata"s;
    metadata.snapshot_id = 1;
    metadata.snapshot_parent_id = 0;
    metadata.manifest_location = "manifest.json"s;
    metadata.timestamp_ms = 1234567890;
    
    // Add a child snapshot
    Metadata child_snapshot;
    child_snapshot.id = "child_snapshot"s;
    child_snapshot.snapshot_id = 2;
    child_snapshot.snapshot_parent_id = 1;
    child_snapshot.manifest_location = "child_manifest.json"s;
    child_snapshot.timestamp_ms = 1234567891;
    
    metadata.snapshots.push_back(child_snapshot);

    // Write metadata
    auto write_result = manager->write_metadata(metadata);
    ASSERT_TRUE(write_result.ok());
    ASSERT_TRUE(std::filesystem::exists(write_result.ValueOrDie()));

    // Read metadata
    auto read_result = manager->read_metadata("test_metadata");
    ASSERT_TRUE(read_result.ok());
    
    Metadata read_metadata = read_result.ValueOrDie();
    ASSERT_EQ(read_metadata.id, metadata.id);
    ASSERT_EQ(read_metadata.snapshot_id, metadata.snapshot_id);
    ASSERT_EQ(read_metadata.snapshot_parent_id, metadata.snapshot_parent_id);
    ASSERT_EQ(read_metadata.manifest_location, metadata.manifest_location);
    ASSERT_EQ(read_metadata.timestamp_ms, metadata.timestamp_ms);
    ASSERT_EQ(read_metadata.snapshots.size(), 1);
    
    const auto& read_child = read_metadata.snapshots[0];
    ASSERT_EQ(read_child.id, child_snapshot.id);
    ASSERT_EQ(read_child.snapshot_id, child_snapshot.snapshot_id);
    ASSERT_EQ(read_child.snapshot_parent_id, child_snapshot.snapshot_parent_id);
    ASSERT_EQ(read_child.manifest_location, child_snapshot.manifest_location);
    ASSERT_EQ(read_child.timestamp_ms, child_snapshot.timestamp_ms);
}

TEST_F(MetadataManagerTest, ReadNonExistentManifest) {
    ASSERT_TRUE(manager->initialize().ok());
    
    auto result = manager->read_manifest("non_existent");
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsIOError());
}

TEST_F(MetadataManagerTest, ReadNonExistentMetadata) {
    ASSERT_TRUE(manager->initialize().ok());
    
    auto result = manager->read_metadata("non_existent");
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsIOError());
}

TEST_F(MetadataManagerTest, WriteManifestToNonExistentDirectory) {
    // Don't initialize the manager
    Manifest manifest;
    manifest.id = "test_manifest"s;
    
    auto result = manager->write_manifest(manifest);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsIOError());
}

TEST_F(MetadataManagerTest, WriteMetadataToNonExistentDirectory) {
    // Don't initialize the manager
    Metadata metadata;
    metadata.id = "test_metadata"s;
    
    auto result = manager->write_metadata(metadata);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsIOError());
} 