#ifndef STRING_REF_HPP
#define STRING_REF_HPP

#include <atomic>
#include <cstring>
#include <string>
#include <string_view>

namespace tundradb {

// Forward declaration
class StringPool;

/**
 * Intrusive reference-counted string reference.
 *
 * Memory layout in arena:
 * ┌────────────┬────────┬─────────┬─────────┬──────────────────┐
 * │ ref_count  │ length │  flags  │ padding │  string data...  │
 * │   (4B)     │  (4B)  │  (4B)   │  (4B)   │                  │
 * └────────────┴────────┴─────────┴─────────┴──────────────────┘
 *       ↑                                    ↑
 *    Header (16 bytes)                   data_ points here
 *
 * The reference count is stored IN the arena memory, BEFORE the string data.
 * This allows zero-lookup reference counting operations.
 *
 * Usage:
 *   StringArena arena;
 *   StringRef ref = arena.store_string("Hello");  // ref_count = 1
 *   StringRef copy = ref;                         // ref_count = 2 (copy ctor)
 *   // ... both refs can be used safely ...
 *   // When last StringRef is destroyed, string is deallocated
 */
class StringRef {
 private:
  const char* data_;
  uint32_t length_;
  uint32_t pool_id_;

  /**
   * String header stored in arena memory BEFORE the string data.
   * Layout: [ref_count:4][length:4][flags:4][padding:4] = 16 bytes total
   * This ensures the header is cache-line aligned and adjacent to string data.
   */
  struct StringHeader {
    std::atomic<int32_t> ref_count;  // 4 bytes - Thread-safe reference count
    uint32_t length;   // 4 bytes - String length (for dedup removal)
    uint32_t flags;    // 4 bytes - Bit 0: marked_for_deletion
    uint32_t padding;  // 4 bytes - Ensure 16-byte alignment

    /**
     * Check if string is marked for deletion.
     * When marked, the string will be deallocated when ref_count reaches 0.
     */
    bool is_marked_for_deletion() const { return (flags & 0x1) != 0; }

    /**
     * Mark string for deletion.
     * Called when a node field is updated - actual deallocation happens
     * when the last StringRef is destroyed.
     */
    void mark_for_deletion() { flags |= 0x1; }
  };

  static constexpr size_t HEADER_SIZE = sizeof(StringHeader);  // 16 bytes

  __attribute__((always_inline)) StringHeader* get_header() const {
    if (!data_) return nullptr;
    return reinterpret_cast<StringHeader*>(
        const_cast<char*>(data_ - HEADER_SIZE));
  }

 public:
  // ========================================================================
  // CONSTRUCTORS AND DESTRUCTOR
  // ========================================================================

  /**
   * Default constructor - creates a null reference.
   */
  StringRef() : data_(nullptr), length_(0), pool_id_(0) {}

  /**
   * Internal constructor used by StringArena.
   * Automatically increments the reference count.
   *
   * @param data Pointer to string data in arena (NOT to header)
   * @param length String length in bytes
   * @param pool_id ID of the pool that owns this string
   */
  StringRef(const char* data, uint32_t length, uint32_t pool_id)
      : data_(data), length_(length), pool_id_(pool_id) {
    if (data_) {
      auto* header = get_header();
      if (header) {
        // Atomic increment - lock-free and thread-safe
        header->ref_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  /**
   * Copy constructor - increments reference count.
   * Multiple StringRefs can safely point to the same string data.
   */
  StringRef(const StringRef& other)
      : data_(other.data_), length_(other.length_), pool_id_(other.pool_id_) {
    if (data_) {
      auto* header = get_header();
      if (header) {
        header->ref_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  /**
   * Move constructor - transfers ownership without changing ref count.
   * This is efficient for passing StringRef by value.
   */
  StringRef(StringRef&& other) noexcept
      : data_(other.data_), length_(other.length_), pool_id_(other.pool_id_) {
    other.data_ = nullptr;
    other.length_ = 0;
    other.pool_id_ = 0;
  }

    /**
     * Copy assignment - properly handles reference counting.
     * 
     * Example: ref4 = ref1
     * - Decrements ref4's OLD value's ref_count
     * - Increments ref1's value's ref_count
     * - ref4 now points to the same string as ref1
     */
    StringRef& operator=(const StringRef& other) {
        if (this != &other) {
            // Step 1: Release THIS object's OLD value (before overwriting)
            // Example: if ref4 pointed to "Goodbye", decrement "Goodbye" ref_count
            // Note: release() also sets this->data_ = nullptr after decrementing
            release();
            
            // Step 2: Copy OTHER object's values into THIS object
            // Example: ref4 now gets ref1's pointer to "Hello"
            data_ = other.data_;
            length_ = other.length_;
            pool_id_ = other.pool_id_;
            
            // Step 3: Increment the NEW value's ref_count
            // Important: get_header() now uses the NEW data_ pointer (from step 2)
            // Example: Increment "Hello" ref_count (not "Goodbye"!)
            if (data_) {
                auto* header = get_header();  // Uses NEW data_ pointer
                if (header) {
                    header->ref_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
        return *this;
    }

  /**
   * Move assignment - transfers ownership.
   */
  StringRef& operator=(StringRef&& other) noexcept {
    if (this != &other) {
      release();  // Decrement old reference

      data_ = other.data_;
      length_ = other.length_;
      pool_id_ = other.pool_id_;

      other.data_ = nullptr;
      other.length_ = 0;
      other.pool_id_ = 0;
    }
    return *this;
  }

  /**
   * Destructor - decrements reference count and deallocates if last reference.
   * This implements automatic memory management for strings.
   */
  ~StringRef() { release(); }

  // ========================================================================
  // PUBLIC INTERFACE
  // ========================================================================

  const char* data() const { return data_; }

  uint32_t length() const { return length_; }

  /**
   * Get pool ID (used for deallocation).
   */
  uint32_t pool_id() const { return pool_id_; }

  bool is_null() const { return data_ == nullptr; }

  std::string_view view() const {
    return data_ ? std::string_view(data_, length_) : std::string_view{};
  }

  std::string to_string() const {
    return data_ ? std::string(data_, length_) : std::string();
  }

  int32_t get_ref_count() const {
    auto* header = get_header();
    return header ? header->ref_count.load(std::memory_order_relaxed) : 0;
  }

  bool is_marked_for_deletion() const {
    auto* header = get_header();
    return header && header->is_marked_for_deletion();
  }

  // ========================================================================
  // OPERATORS
  // ========================================================================

  bool operator==(const StringRef& other) const {
    // Fast path: same pointer
    if (data_ == other.data_ && length_ == other.length_) {
      return true;
    }
    if (length_ != other.length_) {
      return false;
    }
    if (!data_ && !other.data_) {
      return true;
    }
    if (!data_ || !other.data_) {
      return false;
    }
    return std::memcmp(data_, other.data_, length_) == 0;
  }

  bool operator!=(const StringRef& other) const { return !(*this == other); }

  // ========================================================================
  // INTERNAL METHODS
  // ========================================================================

 private:
  /**
   * Release this reference.
   * Decrements ref count and deallocates if this was the last reference
   * AND the string is marked for deletion.
   *
   * This is called by the destructor and assignment operators.
   */
  void
  release();  // Implementation in string_arena.hpp to avoid circular dependency

  friend class StringPool;
  friend class StringArena;
};

}  // namespace tundradb

#endif  // STRING_REF_HPP
