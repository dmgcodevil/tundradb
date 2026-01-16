#ifndef MEMORY_ARENA_HPP
#define MEMORY_ARENA_HPP

#include <memory>
#include <vector>

#include "mem_arena.hpp"
#include "mem_utils.hpp"

namespace tundradb {

/**
 * Simple arena-based memory allocation
 * Provides fast bulk allocation with automatic cleanup
 * No individual deallocation - reset/clear only
 */
class MemoryArena : public MemArena {
 public:
  explicit MemoryArena(size_t initial_size = 1024 * 1024)  // 1MB default
      : chunk_size_(initial_size), current_offset_(0) {
    allocate_new_chunk();
  }

  virtual ~MemoryArena() { MemoryArena::clear(); }

  // Non-copyable but movable
  MemoryArena(const MemoryArena&) = delete;  // Copy constructor - DISABLED
  MemoryArena& operator=(const MemoryArena&) =
      delete;  // Copy assignment - DISABLED
  MemoryArena(MemoryArena&&) = default;
  MemoryArena& operator=(MemoryArena&&) = default;

  /**
   * Allocate aligned memory from the arena.
   *
   * Ensures returned pointer is aligned to specified boundary (power of 2).
   * May skip bytes (padding) to achieve alignment.
   *
   * Example:
   *   allocate(1, 1);  // char at 0x1000
   *   allocate(8, 8);  // int64_t at 0x1008 (skips 0x1001-0x1007 for alignment)
   *
   * @param size Number of bytes to allocate
   * @param alignment Memory alignment (1, 2, 4, 8, 16...) - must be power of 2
   * @return Pointer to aligned memory, or nullptr if allocation fails
   */
  void* allocate(size_t size, size_t alignment = 8) override {
    // Calculate aligned offset: rounds current_offset_ up to next multiple of
    // alignment Example: current_offset_=5, alignment=8 → aligned_offset=8
    // (skips 3 bytes padding)
    size_t aligned_offset =
        (current_chunk_ != nullptr)
            ? calculate_aligned_offset(current_chunk_, current_offset_,
                                       alignment)
            : current_offset_;

    // Check if enough space in current chunk
    if (current_chunk_ == nullptr || aligned_offset + size > chunk_size_) {
      size_t new_chunk_size =
          std::max(chunk_size_, size) + get_alignment_overhead(alignment);
      allocate_new_chunk(new_chunk_size);
      aligned_offset = calculate_aligned_offset(current_chunk_, 0, alignment);
    }

    // Return aligned pointer: base_address + aligned_offset
    void* result = current_chunk_ + aligned_offset;
    current_offset_ = aligned_offset + size;  // Update for next allocation
    total_allocated_ += size;

    return result;
  }

  /**
   * Deallocate memory (no-op for MemoryArena - use reset/clear instead)
   * @param ptr Pointer returned by allocate()
   */
  void deallocate(void* ptr) override {
    // MemoryArena doesn't support individual deallocation
    // Use reset() or clear() instead
    (void)ptr;  // Suppress unused parameter warning
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
  void reset() override {
    current_offset_ = 0;
    if (!chunks_.empty()) {
      current_chunk_ = chunks_[0].get();
    }
    total_allocated_ = 0;
  }

  /**
   * Clear all allocated memory
   */
  void clear() override {
    chunks_.clear();
    current_chunk_ = nullptr;
    current_offset_ = 0;
    total_allocated_ = 0;
  }

  // Statistics
  size_t get_total_allocated() const override { return total_allocated_; }
  size_t get_chunk_count() const override { return chunks_.size(); }
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