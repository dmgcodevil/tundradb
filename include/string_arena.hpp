#ifndef STRING_ARENA_HPP
#define STRING_ARENA_HPP

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "free_list_arena.hpp"
#include "memory_arena.hpp"
#include "types.hpp"

namespace tundradb {

/**
 * String pool for strings of a specific maximum size
 * Backed by FreeListArena for individual string deallocation
 */
class StringPool {
 public:
  explicit StringPool(const size_t max_size,
                      size_t initial_arena_size = 1024 * 1024)
      : max_size_(max_size),
        arena_(std::make_unique<FreeListArena>(initial_arena_size, 16)) {
    // Use 16-byte minimum fragment size for string alignment
  }

  /**
   * Store a string in this pool
   * Returns StringRef pointing to the stored string
   */
  StringRef store_string(const std::string& str, const uint32_t pool_id = 0) {
    if (str.length() > max_size_) {
      // String too large for this pool
      return StringRef{};
    }

    // Check for deduplication with reference counting
    if (enable_deduplication_) {
      if (const auto it = dedup_map_.find(str); it != dedup_map_.end()) {
        // Increment reference count and return existing string
        it->second.second++;
        return it->second.first;
      }
    }

    // Allocate space for string + null terminator
    const size_t alloc_size = str.length() + 1;
    const auto storage = static_cast<char*>(arena_->allocate(alloc_size));
    if (!storage) {
      return StringRef{};  // Allocation failed
    }

    // Copy string data
    std::memcpy(storage, str.c_str(), str.length());
    storage[str.length()] = '\0';

    StringRef ref(storage, static_cast<uint32_t>(str.length()), pool_id);

    // Add to deduplication map with reference count = 1
    if (enable_deduplication_) {
      dedup_map_[str] = std::make_pair(ref, 1);
    }

    return ref;
  }

  /**
   * Store a string view in this pool
   */
  StringRef store_string(const std::string_view str,
                         const uint32_t pool_id = 0) {
    return store_string(std::string(str), pool_id);
  }

  /**
   * Deallocate a string with reference counting support
   */
  void deallocate_string(const StringRef& ref) {
    if (!ref.is_null()) {
      if (enable_deduplication_) {
        // Find string by reverse lookup (expensive but necessary)
        for (auto it = dedup_map_.begin(); it != dedup_map_.end(); ++it) {
          if (it->second.first.data == ref.data) {
            // Decrement reference count
            // uint32_t old_count = it->second.second;
            it->second.second--;
            // uint32_t new_count = it->second.second;

            // Logger::get_instance().debug(
            //     "deallocate_string: string '{}' ref count: {} -> {}",
            //     it->first, old_count, new_count);

            // Only deallocate when reference count reaches 0
            if (it->second.second == 0) {
              // Logger::get_instance().debug(
              //     "deallocate_string: ref count reached 0, deallocating
              //     string "
              //     "'{}'",
              //     it->first);
              arena_->deallocate(const_cast<char*>(ref.data));
              dedup_map_.erase(it);
              // Logger::get_instance().debug(
              //     "deallocate_string: removed '{}' from dedup_map",
              //     it->first);
            } else {
              // Logger::get_instance().debug(
              //     "deallocate_string: string '{}' still has {} references, "
              //     "keeping alive",
              //     it->first, new_count);
            }
            return;
          }
        }
        // If not found in dedup_map_, it might be a non-deduplicated string
        // Logger::get_instance().debug(
        //     "deallocate_string: not found in dedup_map_");
        arena_->deallocate(const_cast<char*>(ref.data));
      } else {
        // No deduplication - always deallocate
        arena_->deallocate(const_cast<char*>(ref.data));
      }
    }
  }

  /**
   * Get string content from reference (zero-copy view)
   */
  static std::string_view get_string_view(const StringRef& ref) {
    if (ref.is_null()) {
      return std::string_view{};
    }
    return std::string_view{ref.data, ref.length};
  }

  // Configuration
  void enable_deduplication(const bool enable = true) {
    enable_deduplication_ = enable;
    if (!enable) {
      dedup_map_.clear();
    }
  }

  // Statistics
  size_t get_max_size() const { return max_size_; }
  size_t get_total_allocated() const { return arena_->get_total_allocated(); }
  size_t get_used_bytes() const {
    if (const auto* free_list = dynamic_cast<FreeListArena*>(arena_.get())) {
      return free_list->get_used_bytes();
    }
    return 0;  // Fallback if not FreeListArena
  }
  size_t get_string_count() const { return dedup_map_.size(); }

  // Get total reference count (for debugging)
  size_t get_total_references() const {
    size_t total = 0;
    for (const auto& [fst, snd] : dedup_map_ | std::views::values) {
      total += snd;
    }
    return total;
  }

  void reset() {
    arena_->reset();
    dedup_map_.clear();
  }

  void clear() {
    arena_->clear();
    dedup_map_.clear();
  }

 private:
  size_t max_size_;
  std::unique_ptr<FreeListArena> arena_;
  bool enable_deduplication_ = false;
  // Reference counting for deduplication safety
  std::unordered_map<std::string, std::pair<StringRef, uint32_t>> dedup_map_;
};

/**
 * Multi-pool string arena that manages strings of different sizes
 * Routes strings to appropriate pools based on their size and type
 */
class StringArena {
 public:
  StringArena() {
    // Create pools for different string size categories
    pools_ = std::vector<std::unique_ptr<StringPool>>();
    pools_.reserve(4);  // Reserve space for 4 pools
    pools_.emplace_back(std::make_unique<StringPool>(16));
    pools_.emplace_back(std::make_unique<StringPool>(32));
    pools_.emplace_back(std::make_unique<StringPool>(64));
    pools_.emplace_back(std::make_unique<StringPool>(SIZE_MAX));
  }

  /**
   * Store a string in the appropriate pool based on its type
   */
  StringRef store_string(const std::string& str,
                         ValueType type = ValueType::STRING) {
    if (!is_string_type(type)) {
      return StringRef{};  // Not a string type
    }
    const uint32_t pool_id = type_to_index(type);
    return pools_[pool_id]->store_string(str, pool_id);
  }

  uint32_t type_to_index(const ValueType type) const {
    switch (type) {
      case ValueType::FIXED_STRING16:
        return 0;
      case ValueType::FIXED_STRING32:
        return 1;
      case ValueType::FIXED_STRING64:
        return 2;
      case ValueType::STRING:
        return 3;
      default:
        return -1;  // Invalid type
    }
  }

  StringPool* get_pool_by_type(const ValueType type) const {
    return pools_[type_to_index(type)].get();
  }

  /**
   * Store string view
   */
  StringRef store_string(const std::string_view str,
                         const ValueType type = ValueType::STRING) {
    return store_string(std::string(str), type);
  }

  /**
   * Store a string, automatically choosing the best pool
   * Picks the smallest pool that can fit the string
   */
  StringRef store_string_auto(const std::string& str) {
    const size_t len = str.length();

    if (len <= 16) {
      return store_string(str, ValueType::FIXED_STRING16);
    }
    if (len <= 32) {
      return store_string(str, ValueType::FIXED_STRING32);
    }
    if (len <= 64) {
      return store_string(str, ValueType::FIXED_STRING64);
    }
    return store_string(str, ValueType::STRING);
  }

  /**
   * Get string content from reference
   */
  std::string_view get_string_view(const StringRef& ref) const {
    return pools_[ref.arena_id]->get_string_view(ref);
  }

  /**
   * Deallocate a string
   */
  void deallocate_string(const StringRef& ref) {
    pools_[ref.arena_id]->deallocate_string(ref);
  }

  /**
   * Configure deduplication for all pools
   */
  void enable_deduplication(const bool enable = true) {
    for (const auto& pool : pools_) {
      pool->enable_deduplication(enable);
    }
  }

  // Statistics
  void print_statistics() const {
    // printf("StringArena Statistics:\n");
    // for (const auto& [type, pool] : pools_) {
    //   printf("  %s pool: max_size=%zu, allocated=%zu bytes, strings=%zu\n",
    //          to_string(type).c_str(), pool->get_max_size(),
    //          pool->get_total_allocated(), pool->get_string_count());
    // }
  }

  void reset() {
    for (const auto& pool : pools_) {
      pool->reset();
    }
  }

  void clear() {
    for (const auto& pool : pools_) {
      pool->clear();
    }
  }

 private:
  std::vector<std::unique_ptr<StringPool>> pools_;
};

}  // namespace tundradb

#endif  // STRING_ARENA_HPP