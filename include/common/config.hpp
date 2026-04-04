#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>

namespace tundradb {

namespace defaults {
constexpr size_t SHARD_CAPACITY = 100000;
constexpr size_t CHUNK_SIZE = 10000;
constexpr size_t SHARD_MEMORY_POOL_SIZE = 10 * 1024 * 1024;       // 10 MB
constexpr size_t MANAGER_MEMORY_POOL_SIZE = 100 * 1024 * 1024;    // 100 MB
constexpr size_t DATABASE_MEMORY_POOL_SIZE = 1024 * 1024 * 1024;  // 1 GB
}  // namespace defaults

/// Immutable database tuning: shard sizing, memory pools, storage path, and
/// feature toggles.
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

  bool persistence_enabled = true;

  bool validation_enabled = true;

  // Enable temporal versioning (copy-on-write for time-travel queries)
  bool versioning_enabled_ = false;

  friend class DatabaseConfigBuilder;

 public:
  /// Max nodes per shard before layout or split policy applies.
  size_t get_shard_capacity() const { return shard_capacity; }
  /// Row chunk size used when creating tables.
  size_t get_chunk_size() const { return chunk_size; }
  /// Per-shard memory pool budget in bytes.
  size_t get_shard_memory_pool_size() const { return shard_memory_pool_size; }
  /// Shard-manager memory pool budget in bytes.
  size_t get_manager_memory_pool_size() const {
    return manager_memory_pool_size;
  }
  /// Top-level database memory pool budget in bytes.
  size_t get_database_memory_pool_size() const {
    return database_memory_pool_size;
  }
  /// Filesystem directory for database files (empty if unset).
  std::string get_db_path() const { return db_path; }
  /// Whether data is written through to durable storage.
  bool is_persistence_enabled() const { return persistence_enabled; }
  /// Whether structural or data validation checks run on operations.
  bool is_validation_enabled() const { return validation_enabled; }
  /// Whether temporal versioning (copy-on-write for time travel) is active.
  bool is_versioning_enabled() const { return versioning_enabled_; }
};

/// Fluent builder for \ref DatabaseConfig; initial field values match the
/// constants in namespace defaults.
class DatabaseConfigBuilder {
 private:
  DatabaseConfig config;

 public:
  /// Starts from default capacities, pools, path, and flags.
  DatabaseConfigBuilder() = default;

  /// Sets \ref DatabaseConfig::get_shard_capacity.
  DatabaseConfigBuilder &with_shard_capacity(const size_t capacity) {
    config.shard_capacity = capacity;
    return *this;
  }

  /// Sets \ref DatabaseConfig::get_chunk_size.
  DatabaseConfigBuilder &with_chunk_size(const size_t size) {
    config.chunk_size = size;
    return *this;
  }

  /// Sets \ref DatabaseConfig::get_shard_memory_pool_size (bytes).
  DatabaseConfigBuilder &with_shard_memory_pool_size(const size_t size) {
    config.shard_memory_pool_size = size;
    return *this;
  }

  /// Sets \ref DatabaseConfig::get_manager_memory_pool_size (bytes).
  DatabaseConfigBuilder &with_manager_memory_pool_size(const size_t size) {
    config.manager_memory_pool_size = size;
    return *this;
  }

  /// Sets \ref DatabaseConfig::get_database_memory_pool_size (bytes).
  DatabaseConfigBuilder &with_database_memory_pool_size(const size_t size) {
    config.database_memory_pool_size = size;
    return *this;
  }

  /// Sets \ref DatabaseConfig::get_db_path.
  DatabaseConfigBuilder &with_db_path(const std::string &directory) {
    config.db_path = directory;
    return *this;
  }

  /// Sets \ref DatabaseConfig::is_persistence_enabled.
  DatabaseConfigBuilder &with_persistence_enabled(const bool enabled) {
    config.persistence_enabled = enabled;
    return *this;
  }

  /// Sets \ref DatabaseConfig::is_validation_enabled.
  DatabaseConfigBuilder &with_validation_enabled(const bool enabled) {
    config.validation_enabled = enabled;
    return *this;
  }

  /// Sets \ref DatabaseConfig::is_versioning_enabled.
  DatabaseConfigBuilder &with_versioning_enabled(const bool enabled) {
    config.versioning_enabled_ = enabled;
    return *this;
  }

  /// Scales shard, manager, and database memory pools by \p factor from each
  /// per-layer default byte size.
  DatabaseConfigBuilder &with_memory_scale_factor(const double factor) {
    config.shard_memory_pool_size =
        static_cast<size_t>(defaults::SHARD_MEMORY_POOL_SIZE * factor);
    config.manager_memory_pool_size =
        static_cast<size_t>(defaults::MANAGER_MEMORY_POOL_SIZE * factor);
    config.database_memory_pool_size =
        static_cast<size_t>(defaults::DATABASE_MEMORY_POOL_SIZE * factor);
    return *this;
  }

  /// Returns the configured snapshot; safe to call multiple times (copies out).
  [[nodiscard]] DatabaseConfig build() const { return config; }
};

/// Convenience entry point equivalent to a default-constructed \ref
/// DatabaseConfigBuilder.
inline DatabaseConfigBuilder make_config() { return {}; }

}  // namespace tundradb

#endif  // CONFIG_HPP