#ifndef MEM_UTILS_HPP
#define MEM_UTILS_HPP

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace tundradb {

/**
 * Align a size_t value up to the next boundary
 */
constexpr size_t align_up(size_t value, size_t alignment) {
  return (value + alignment - 1) & ~(alignment - 1);
}

/**
 * Align a memory address up to the next boundary
 */
constexpr uintptr_t align_up_address(uintptr_t value, size_t alignment) {
  return (value + alignment - 1) & ~(alignment - 1);
}

/**
 * Calculate the offset needed to align memory within a chunk
 *
 * @param chunk_start Pointer to the start of the memory chunk
 * @param current_offset Current offset within the chunk
 * @param alignment Desired alignment (power of 2)
 * @return Offset from chunk start to properly aligned position
 */
inline size_t calculate_aligned_offset(const void* chunk_start,
                                       const size_t current_offset,
                                       const size_t alignment) {
  if (alignment <= alignof(std::max_align_t)) {
    // Small alignment: use efficient offset-based calculation
    return align_up(current_offset, alignment);
  } else {
    // Large alignment: must consider actual memory address
    uintptr_t current_addr =
        reinterpret_cast<uintptr_t>(chunk_start) + current_offset;
    uintptr_t aligned_addr = align_up_address(current_addr, alignment);
    return aligned_addr - reinterpret_cast<uintptr_t>(chunk_start);
  }
}

/**
 * Calculate the extra space needed for large alignments when allocating chunks
 */
inline size_t get_alignment_overhead(size_t alignment) {
  return (alignment > alignof(std::max_align_t)) ? (alignment - 1) : 0;
}

/**
 * Check if a pointer is properly aligned
 */
inline bool is_aligned(const void* ptr, size_t alignment) {
  return (reinterpret_cast<uintptr_t>(ptr) % alignment) == 0;
}

}  // namespace tundradb

#endif  // MEM_UTILS_HPP