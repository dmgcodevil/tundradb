#ifndef MEMORY_ARENA_HPP
#define MEMORY_ARENA_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "mem_utils.hpp"

namespace tundradb {

/**
 * Base class for arena-based memory allocation
 * Provides fast bulk allocation with automatic cleanup
 */
class MemoryArena {
 public:
  explicit MemoryArena(size_t initial_size = 1024 * 1024)  // 1MB default
      : chunk_size_(initial_size), current_offset_(0) {
    allocate_new_chunk();
  }

  virtual ~MemoryArena() { clear(); }

  // Non-copyable but movable
  MemoryArena(const MemoryArena&) = delete;  // Copy constructor - DISABLED
  MemoryArena& operator=(const MemoryArena&) =
      delete;  // Copy assignment - DISABLED
  MemoryArena(MemoryArena&&) = default;
  MemoryArena& operator=(MemoryArena&&) = default;

  /**
   * Allocate aligned memory from the arena
   * @param size Number of bytes to allocate
   * @param alignment Memory alignment requirement (default: 8 bytes)
   * @return Pointer to allocated memory, or nullptr if allocation fails
   */
  void* allocate(size_t size, size_t alignment = 8) {
    // Calculate aligned offset within current chunk
    size_t aligned_offset =
        (current_chunk_ != nullptr)
            ? calculate_aligned_offset(current_chunk_, current_offset_,
                                       alignment)
            : current_offset_;  // Will trigger new chunk allocation below

    // Check if we have enough space in the current chunk (or if we have no
    // chunk at all)
    if (current_chunk_ == nullptr || aligned_offset + size > chunk_size_) {
      // Need a new chunk - make it at least as large as the requested size
      size_t new_chunk_size =
          std::max(chunk_size_, size) + get_alignment_overhead(alignment);

      allocate_new_chunk(new_chunk_size);

      // Recalculate aligned offset for the new chunk
      aligned_offset = calculate_aligned_offset(current_chunk_, 0, alignment);
    }

    void* result = current_chunk_ + aligned_offset;
    current_offset_ = aligned_offset + size;
    total_allocated_ += size;

    return result;
  }

  /**
   * Allocate and construct an object in the arena
   */
  template <typename T, typename... Args>
  T* construct(Args&&... args) {
    void* ptr = allocate(sizeof(T), alignof(T));
    return new (ptr) T(std::forward<Args>(args)...);
  }

  /**
   * Reset the arena - keeps allocated chunks but resets usage
   * Much faster than deallocating and reallocating
   */
  void reset() {
    current_offset_ = 0;
    if (!chunks_.empty()) {
      current_chunk_ = chunks_[0].get();
    }
    total_allocated_ = 0;
  }

  /**
   * Clear all allocated memory
   */
  void clear() {
    chunks_.clear();
    current_chunk_ = nullptr;
    current_offset_ = 0;
    total_allocated_ = 0;
  }

  // Statistics
  size_t get_total_allocated() const { return total_allocated_; }
  size_t get_chunk_count() const { return chunks_.size(); }
  size_t get_current_chunk_usage() const { return current_offset_; }
  size_t get_current_chunk_size() const { return chunk_size_; }

 protected:
  void allocate_new_chunk(size_t size = 0) {
    if (size == 0) size = chunk_size_;

    auto new_chunk = std::make_unique<char[]>(size);
    current_chunk_ = new_chunk.get();
    chunks_.push_back(std::move(new_chunk));
    current_offset_ = 0;
    chunk_size_ = size;
  }

  // Make chunks_ accessible to derived classes
  std::vector<std::unique_ptr<char[]>> chunks_;
  char* current_chunk_ = nullptr;

 private:
  size_t chunk_size_;
  size_t current_offset_;
  size_t total_allocated_ = 0;
};

/**
 * Arena allocator that can be used with STL containers
 */
template <typename T>
class ArenaAllocator {
 public:
  using value_type = T;

  explicit ArenaAllocator(MemoryArena* arena) : arena_(arena) {}

  template <typename U>
  ArenaAllocator(const ArenaAllocator<U>& other) : arena_(other.arena_) {}

  T* allocate(size_t n) {
    return static_cast<T*>(arena_->allocate(n * sizeof(T), alignof(T)));
  }

  void deallocate(T*, size_t) {
    // Arena handles deallocation in bulk
  }

  template <typename U>
  bool operator==(const ArenaAllocator<U>& other) const {
    return arena_ == other.arena_;
  }

  template <typename U>
  bool operator!=(const ArenaAllocator<U>& other) const {
    return !(*this == other);
  }

  MemoryArena* arena_;
};

}  // namespace tundradb

#endif  // MEMORY_ARENA_HPP