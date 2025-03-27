#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <cstddef>
#include <string>

namespace tundradb {

// Default configuration constants
namespace defaults {
constexpr size_t SHARD_CAPACITY = 100000;
constexpr size_t CHUNK_SIZE = 10000;
constexpr size_t SHARD_MEMORY_POOL_SIZE = 10 * 1024 * 1024;       // 10 MB
constexpr size_t MANAGER_MEMORY_POOL_SIZE = 100 * 1024 * 1024;    // 100 MB
constexpr size_t DATABASE_MEMORY_POOL_SIZE = 1024 * 1024 * 1024;  // 1 GB
}  // namespace defaults

// Configuration parameters for the database
class DatabaseConfig {
 private:
  // Maximum number of nodes per shard
  size_t shard_capacity = defaults::SHARD_CAPACITY;

  // Size of chunks when creating tables
  size_t chunk_size = defaults::CHUNK_SIZE;

  // Memory pool size for shards (in bytes)
  size_t shard_memory_pool_size = defaults::SHARD_MEMORY_POOL_SIZE;

  // Memory pool size for shard manager (in bytes)
  size_t manager_memory_pool_size = defaults::MANAGER_MEMORY_POOL_SIZE;

  // Memory pool size for database (in bytes)
  size_t database_memory_pool_size = defaults::DATABASE_MEMORY_POOL_SIZE;

  std::string db_path = "";

  // Whether persistence is enabled
  bool persistence_enabled = true;

  // Allow DatabaseConfigBuilder to modify private fields
  friend class DatabaseConfigBuilder;

 public:
  size_t get_shard_capacity() const { return shard_capacity; }
  size_t get_chunk_size() const { return chunk_size; }
  size_t get_shard_memory_pool_size() const { return shard_memory_pool_size; }
  size_t get_manager_memory_pool_size() const {
    return manager_memory_pool_size;
  }
  size_t get_database_memory_pool_size() const {
    return database_memory_pool_size;
  }
  std::string get_db_path() const { return db_path; }
  bool is_persistence_enabled() const { return persistence_enabled; }
};

// Builder class for DatabaseConfig
class DatabaseConfigBuilder {
 private:
  DatabaseConfig config;

 public:
  DatabaseConfigBuilder() = default;

  DatabaseConfigBuilder &with_shard_capacity(size_t capacity) {
    config.shard_capacity = capacity;
    return *this;
  }

  DatabaseConfigBuilder &with_chunk_size(size_t size) {
    config.chunk_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_shard_memory_pool_size(size_t size) {
    config.shard_memory_pool_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_manager_memory_pool_size(size_t size) {
    config.manager_memory_pool_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_database_memory_pool_size(size_t size) {
    config.database_memory_pool_size = size;
    return *this;
  }

  DatabaseConfigBuilder &with_db_path(const std::string &directory) {
    config.db_path = directory;
    return *this;
  }

  DatabaseConfigBuilder &with_persistence_enabled(bool enabled) {
    config.persistence_enabled = enabled;
    return *this;
  }

  // Helper for setting all memory sizes with a single scale factor
  DatabaseConfigBuilder &with_memory_scale_factor(double factor) {
    config.shard_memory_pool_size =
        static_cast<size_t>(defaults::SHARD_MEMORY_POOL_SIZE * factor);
    config.manager_memory_pool_size =
        static_cast<size_t>(defaults::MANAGER_MEMORY_POOL_SIZE * factor);
    config.database_memory_pool_size =
        static_cast<size_t>(defaults::DATABASE_MEMORY_POOL_SIZE * factor);
    return *this;
  }

  [[nodiscard]] DatabaseConfig build() const { return config; }
};

// Helper function to create a config builder
inline DatabaseConfigBuilder make_config() { return {}; }

}  // namespace tundradb

#endif  // CONFIG_HPP