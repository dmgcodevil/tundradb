#ifndef MEM_ARENA_HPP
#define MEM_ARENA_HPP

#include <cstddef>

namespace tundradb {

/**
 * Common interface for all memory arenas.
 */
class MemArena {
 public:
  virtual ~MemArena() = default;

  /**
   * Allocate aligned memory from the arena
   * @param size Number of bytes to allocate
   * @param alignment Memory alignment requirement
   * @return Pointer to allocated memory, or nullptr if allocation fails
   */
  virtual void* allocate(size_t size, size_t alignment) = 0;

  /**
   * Deallocate a block of memory (only meaningful for FreeListArena)
   * @param ptr Pointer returned by allocate()
   */
  virtual void deallocate(void* ptr) = 0;

  /**
   * Reset the arena - keeps allocated chunks but resets usage
   * Much faster than deallocating and reallocating
   */
  virtual void reset() = 0;

  /**
   * Clear all allocated memory
   */
  virtual void clear() = 0;

  // Statistics
  [[nodiscard]] virtual size_t get_total_allocated() const = 0;
  [[nodiscard]] virtual size_t get_chunk_count() const = 0;
};

}  // namespace tundradb

#endif  // MEM_ARENA_HPP