#include "memory/string_arena.hpp"

namespace tundradb {

// ============================================================================
// StringPool
// ============================================================================

arrow::Result<StringRef> StringPool::store_string(const std::string& str,
                                                  uint32_t pool_id) {
  if (str.length() > max_size_) {
    return arrow::Status::Invalid("StringPool::store_string: string length ",
                                  str.length(), " exceeds pool max_size ",
                                  max_size_);
  }

  if (enable_deduplication_) {
    typename decltype(dedup_cache_)::const_accessor acc;
    if (dedup_cache_.find(acc, str)) {
      return acc->second;
    }
  }

  std::lock_guard<std::mutex> lock(arena_mutex_);

  const size_t alloc_size = StringRef::HEADER_SIZE + str.length() + 1;
  void* raw_storage = arena_->allocate(alloc_size);
  if (!raw_storage) {
    return arrow::Status::OutOfMemory(
        "StringPool::store_string: arena allocation failed (requested ",
        alloc_size, " bytes)");
  }

  auto* header = static_cast<StringRef::StringHeader*>(raw_storage);
  header->ref_count.store(0, std::memory_order_relaxed);
  header->length = static_cast<uint32_t>(str.length());
  header->flags = 0;
  header->padding = 0;

  char* data = reinterpret_cast<char*>(header) + StringRef::HEADER_SIZE;
  std::memcpy(data, str.c_str(), str.length());
  data[str.length()] = '\0';

  StringRef ref(data, static_cast<uint32_t>(str.length()), pool_id);
  active_allocs_.fetch_add(1, std::memory_order_relaxed);

  if (enable_deduplication_) {
    typename decltype(dedup_cache_)::accessor acc;
    dedup_cache_.insert(acc, str);
    acc->second = ref;
  }

  return ref;
}

void StringPool::mark_for_deletion(const char* data) {
  if (!data) return;

  auto* header = reinterpret_cast<StringRef::StringHeader*>(
      const_cast<char*>(data - StringRef::HEADER_SIZE));

  header->mark_for_deletion();

  if (enable_deduplication_) {
    std::string str(data, header->length);
    dedup_cache_.erase(str);
  }
}

void StringPool::release_string(const char* data) {
  if (!data) return;

  auto* header = reinterpret_cast<StringRef::StringHeader*>(
      const_cast<char*>(data - StringRef::HEADER_SIZE));

  active_allocs_.fetch_sub(1, std::memory_order_relaxed);

  std::lock_guard<std::mutex> lock(arena_mutex_);
  arena_->deallocate(header);
}

void StringPool::enable_deduplication(bool enable) {
  enable_deduplication_ = enable;
  if (!enable) {
    dedup_cache_.clear();
  }
}

size_t StringPool::get_used_bytes() const {
  if (auto* free_list = dynamic_cast<FreeListArena*>(arena_.get())) {
    return free_list->get_used_bytes();
  }
  return 0;
}

size_t StringPool::get_total_references() const {
  size_t total = 0;
  typename decltype(dedup_cache_)::const_accessor acc;
  for (auto it = dedup_cache_.begin(); it != dedup_cache_.end(); ++it) {
    if (dedup_cache_.find(acc, it->first)) {
      total += acc->second.get_ref_count();
    }
  }
  return total;
}

// ============================================================================
// StringArena
// ============================================================================

StringArena::StringArena() {
  pools_.reserve(4);
  pools_.emplace_back(std::make_unique<StringPool>(16));
  pools_.emplace_back(std::make_unique<StringPool>(32));
  pools_.emplace_back(std::make_unique<StringPool>(64));
  pools_.emplace_back(std::make_unique<StringPool>(SIZE_MAX));
  register_pools();
}

arrow::Result<StringRef> StringArena::store_string(const std::string& str,
                                                   uint32_t pool_id) {
  if (pool_id >= pools_.size()) {
    return arrow::Status::Invalid(
        "StringArena::store_string: invalid pool_id ", pool_id,
        " (max: ", pools_.size() - 1, ")");
  }
  return pools_[pool_id]->store_string(str, pool_id);
}

arrow::Result<StringRef> StringArena::store_string_auto(
    const std::string& str) {
  size_t len = str.length();
  if (len <= 16) return pools_[0]->store_string(str, 0);
  if (len <= 32) return pools_[1]->store_string(str, 1);
  if (len <= 64) return pools_[2]->store_string(str, 2);
  return pools_[3]->store_string(str, 3);
}

void StringArena::mark_for_deletion(const StringRef& ref) {
  if (!ref.is_null()) {
    pools_[ref.pool_id()]->mark_for_deletion(ref.data());
  }
}

void StringArena::enable_deduplication(bool enable) {
  for (auto& pool : pools_) {
    pool->enable_deduplication(enable);
  }
}

int64_t StringArena::get_active_allocs() const {
  int64_t total = 0;
  for (const auto& pool : pools_) {
    total += pool->get_active_allocs();
  }
  return total;
}

StringPool* StringArena::get_pool(uint32_t pool_id) const {
  if (pool_id < pools_.size()) {
    return pools_[pool_id].get();
  }
  return nullptr;
}

void StringArena::reset() {
  for (auto& pool : pools_) {
    pool->reset();
  }
}

void StringArena::clear() {
  for (auto& pool : pools_) {
    pool->clear();
  }
}

void StringArena::register_pools() {
  for (uint32_t i = 0; i < pools_.size(); ++i) {
    StringArenaRegistry::register_pool(i, pools_[i].get());
  }
}

// ============================================================================
// StringArenaRegistry
// ============================================================================

StringPool* StringArenaRegistry::get_pool(uint32_t pool_id) {
  typename decltype(pool_map_)::const_accessor acc;
  if (instance().pool_map_.find(acc, pool_id)) {
    return acc->second;
  }
  return nullptr;
}

void StringArenaRegistry::release_string(uint32_t pool_id, const char* data) {
  if (auto* pool = get_pool(pool_id)) {
    pool->release_string(data);
  }
}

// ============================================================================
// StringRef::release() implementation
// ============================================================================

void StringRef::release() {
  if (data_) {
    if (auto* header = get_header()) {
      assert(header->ref_count.load(std::memory_order_relaxed) > 0 &&
             "StringRef::release() called with ref_count already 0 — "
             "double-release or missing ref-count increment");

      int32_t old_count =
          header->ref_count.fetch_sub(1, std::memory_order_acq_rel);

      if (old_count == 1 && header->is_marked_for_deletion()) {
        StringArenaRegistry::release_string(pool_id_, data_);
      }
    }

    data_ = nullptr;
    length_ = 0;
    pool_id_ = 0;
  }
}

}  // namespace tundradb
