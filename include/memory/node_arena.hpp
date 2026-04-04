#ifndef NODE_ARENA_HPP
#define NODE_ARENA_HPP

#include <llvm/ADT/DenseMap.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/clock.hpp"
#include "common/types.hpp"
#include "core/field_update.hpp"
#include "core/update_type.hpp"
#include "memory/array_arena.hpp"
#include "memory/free_list_arena.hpp"
#include "memory/map_arena.hpp"
#include "memory/mem_arena.hpp"
#include "memory/memory_arena.hpp"
#include "memory/schema_layout.hpp"
#include "memory/string_arena.hpp"

namespace tundradb {

struct NodeHandle;

/**
 * Temporal version metadata with field-level copy-on-write.
 *
 * Stores only changed fields; forms a linked list via prev pointer.
 * All versions share the same base node data.
 *
 * Bitemporal support:
 * - valid_from/valid_to: VALIDTIME (when the fact was true in the domain)
 * - tx_from/tx_to: TXNTIME (when the database knew about the fact)
 */
struct VersionInfo {
  // Version identifier
  uint64_t version_id = 0;

  // VALIDTIME: domain validity interval [valid_from, valid_to)
  uint64_t valid_from = 0;
  uint64_t valid_to = std::numeric_limits<uint64_t>::max();

  // TXNTIME: system knowledge interval [tx_from, tx_to)
  // When the database recorded/believed this version
  uint64_t tx_from = 0;
  uint64_t tx_to = std::numeric_limits<uint64_t>::max();

  // Linked list to previous version
  VersionInfo* prev = nullptr;

  // Changed fields: field_idx -> value pointer (nullptr = explicit NULL)
  llvm::SmallDenseMap<uint16_t, char*> updated_fields;

  // Lazy-populated cache: field_idx -> effective value pointer
  mutable llvm::SmallDenseMap<uint16_t, char*> field_cache_;
  mutable uint64_t cache_bitset_ = 0;

  VersionInfo() = default;

  VersionInfo(uint64_t vid, uint64_t ts_from, VersionInfo* prev_ver = nullptr)
      : version_id(vid),
        valid_from(ts_from),
        tx_from(ts_from),  // Initially tx_from = valid_from
        prev(prev_ver) {}

  /// Returns true if VALIDTIME \p ts lies in [valid_from, valid_to).
  bool is_valid_at(uint64_t ts) const {
    return valid_from <= ts && ts < valid_to;
  }

  /// Returns true if this version is visible at the given VALIDTIME and
  /// TXNTIME.
  bool is_visible_at(uint64_t valid_time, uint64_t tx_time) const {
    return (valid_from <= valid_time && valid_time < valid_to) &&
           (tx_from <= tx_time && tx_time < tx_to);
  }

  /// Walks the version chain and returns the version visible at the snapshot,
  /// or nullptr.
  const VersionInfo* find_version_at_snapshot(uint64_t valid_time,
                                              uint64_t tx_time) const;

  /// Walks the chain using VALIDTIME only (ignores TXNTIME); nullptr if none
  /// matches.
  const VersionInfo* find_version_at_time(uint64_t ts) const;

  /// Number of versions in this chain (including this node).
  size_t count_versions() const;

  /// True if effective value for \p field_idx is present in the lazy field
  /// cache.
  bool is_field_cached(uint16_t field_idx) const {
    if (field_idx >= 64) return field_cache_.count(field_idx) > 0;
    return (cache_bitset_ & (1ULL << field_idx)) != 0;
  }

  /// Records that the effective value for \p field_idx is cached (fast path
  /// uses a bitset).
  void mark_field_cached(uint16_t field_idx) const {
    if (field_idx < 64) cache_bitset_ |= (1ULL << field_idx);
  }

  /// Clears the lazy field cache (bitset and map).
  void clear_cache() const {
    field_cache_.clear();
    cache_bitset_ = 0;
  }
};

/**
 * Handle to a node stored in the arena.
 * Supports optional temporal versioning (version_info_ == nullptr when
 * disabled).
 */
struct NodeHandle {
  void* ptr;  // pointer to base node data
  size_t size;
  std::string schema_name;
  uint32_t schema_version;

  // optional versioning (nullptr = disabled, owned by version_arena_)
  VersionInfo* version_info_;

  // ========================================================================
  // CONSTRUCTORS
  // ========================================================================

  /**
   * Default constructor (non-versioned, null handle).
   */
  NodeHandle()
      : ptr(nullptr),
        size(0),
        schema_name(""),
        schema_version(0),
        version_info_(nullptr) {}

  /**
   * Standard constructor (non-versioned).
   * This is the backward-compatible constructor - existing code works
   * unchanged.
   */
  NodeHandle(void* p, const size_t s, std::string schema,
             const uint32_t version = 1)
      : ptr(p),
        size(s),
        schema_name(std::move(schema)),
        schema_version(version),
        version_info_(nullptr) {}  // Non-versioned by default

  /**
   * Versioned constructor (for arena allocation).
   * Note: version_info_ will be set later by the arena after allocation.
   *
   * @param p Pointer to node data
   * @param s Size of node data
   * @param schema Schema name
   * @param version Schema version
   * @param version_info Pointer to VersionInfo allocated in arena
   */
  NodeHandle(void* p, size_t s, std::string schema, uint32_t version,
             VersionInfo* version_info)
      : ptr(p),
        size(s),
        schema_name(std::move(schema)),
        schema_version(version),
        version_info_(version_info) {}

  /**
   * Destructor - does NOT delete version_info_ (owned by arena).
   */
  ~NodeHandle() {
    // version_info_ is owned by version_arena_, don't delete it here
    version_info_ = nullptr;
  }

  /// True if this handle does not reference node storage.
  bool is_null() const { return ptr == nullptr; }
  /// True if temporal versioning metadata is attached.
  bool is_versioned() const { return version_info_ != nullptr; }
  /// Attaches or replaces the version chain head (owned by the arena, not freed
  /// here).
  void set_version_info(VersionInfo* version_info) {
    version_info_ = version_info;
  }

  /// For versioned nodes, checks VALIDTIME on the current head; always true if
  /// not versioned.
  bool is_valid_at(uint64_t ts) const {
    if (!is_versioned()) return true;
    return version_info_->is_valid_at(ts);
  }

  /// Current head's version id, or 0 when not versioned.
  uint64_t get_version_id() const {
    return is_versioned() ? version_info_->version_id : 0;
  }

  /// VALIDTIME start of the current head, or 0 when not versioned.
  uint64_t get_valid_from() const {
    return is_versioned() ? version_info_->valid_from : 0;
  }

  /// VALIDTIME end of the current head, or max when not versioned.
  uint64_t get_valid_to() const {
    return is_versioned() ? version_info_->valid_to
                          : std::numeric_limits<uint64_t>::max();
  }

  /// Pointer to the version chain head, or nullptr when not versioned.
  VersionInfo* get_version_info() const { return version_info_; }

  /// Counts versions along the chain from the current head (1 if not
  /// versioned).
  size_t count_versions() const;

  /// Starting from the current head, finds the version valid at VALIDTIME \p
  /// ts.
  const VersionInfo* find_version_at_time(uint64_t ts) const;

  /// Previous version link from the current head, or nullptr.
  const VersionInfo* get_prev_version() const {
    if (!is_versioned()) return nullptr;
    return version_info_->prev;
  }

  NodeHandle(NodeHandle&& other) noexcept
      : ptr(other.ptr),
        size(other.size),
        schema_name(std::move(other.schema_name)),
        schema_version(other.schema_version),
        version_info_(other.version_info_) {
    other.ptr = nullptr;
    other.size = 0;
    other.version_info_ = nullptr;
  }

  NodeHandle& operator=(NodeHandle&& other) noexcept {
    if (this != &other) {
      ptr = other.ptr;
      size = other.size;
      schema_name = std::move(other.schema_name);
      schema_version = other.schema_version;
      version_info_ = other.version_info_;

      other.ptr = nullptr;
      other.size = 0;
      other.version_info_ = nullptr;
    }
    return *this;
  }

  NodeHandle(const NodeHandle& other) = default;
  NodeHandle& operator=(const NodeHandle& other) = default;

  /// Compares pointer, size, schema name, and schema version (not version chain
  /// identity).
  bool operator==(const NodeHandle& other) const {
    return ptr == other.ptr && size == other.size &&
           schema_name == other.schema_name &&
           schema_version == other.schema_version;
  }

  /// Negation of operator==.
  bool operator!=(const NodeHandle& other) const { return !(*this == other); }
};

/**
 * Simplified node arena that manages both node layout and string content.
 *
 * Architecture:
 * - mem_arena_: Fixed-size node data (base nodes)
 * - string_arena_: Variable-size string content
 * - version_arena_: (OPTIONAL) Version metadata and field updates
 *
 * Versioning Support:
 * When versioning is DISABLED (default):
 *   - version_arena_ is nullptr
 *   - Zero overhead
 *
 * When versioning is ENABLED:
 *   - version_arena_ stores VersionInfo and updated field data
 *   - Supports time-travel queries
 *   - Field-level copy-on-write for efficient updates
 *
 * Memory Layout:
 * ┌──────────────────────────────────────────────────────────────┐
 * │ mem_arena_ (Base Nodes - Immutable)                          │
 * │ ┌──────────┐ ┌──────────┐ ┌──────────┐                       │
 * │ │ Node 1   │ │ Node 2   │ │ Node 3   │ ...                   │
 * │ └──────────┘ └──────────┘ └──────────┘                       │
 * └──────────────────────────────────────────────────────────────┘
 *
 * ┌──────────────────────────────────────────────────────────────┐
 * │ version_arena_ (Version Metadata - Only if enabled)          │
 * │ ┌────────────┐ ┌────────────┐ ┌────────────┐                 │
 * │ │ N1_v1      │ │ N1_v2      │ │ N2_v1      │ ...             │
 * │ │ VersionInfo│ │ VersionInfo│ │ VersionInfo│                 │
 * │ │ + field    │ │ + field    │ │ + field    │                 │
 * │ └────────────┘ └────────────┘ └────────────┘                 │
 * └──────────────────────────────────────────────────────────────┘
 *
 * ┌──────────────────────────────────────────────────────────────┐
 * │ string_arena_ (String Content - Shared)                      │
 * │ ┌────────┐ ┌────────┐ ┌────────┐                             │
 * │ │ "Alice"│ │ "NYC"  │ │ "Bob"  │ ...                         │
 * │ └────────┘ └────────┘ └────────┘                             │
 * └──────────────────────────────────────────────────────────────┘
 */
class NodeArena {
 public:
  // consts
  static constexpr size_t kInitialSize = 2 * 1024 * 1024;  // 2MB default
  static constexpr size_t kMinFragmentSize = 64;  // 64 bytes minimum fragment

  /**
   * Constructor takes any MemArena implementation + StringArena for strings.
   *
   * @param mem_arena Underlying memory arena for node layouts (MemoryArena or
   * FreeListArena)
   * @param layout_registry Registry containing schema layouts
   * @param string_arena Arena for managing string content (optional, creates
   * own if null)
   * @param enable_versioning Whether to enable temporal versioning support
   * (default: false)
   */
  NodeArena(std::unique_ptr<MemArena> mem_arena,
            std::shared_ptr<LayoutRegistry> layout_registry,
            std::unique_ptr<StringArena> string_arena = nullptr,
            bool enable_versioning = false);

  ~NodeArena();

  /** Allocate new node (versioned if enabled). */
  NodeHandle allocate_node(const std::string& schema_name);

  /** Allocate new node with given layout. */
  NodeHandle allocate_node(const std::shared_ptr<SchemaLayout>& layout);

  /** Get field value pointer. */
  static const char* get_value_ptr(const NodeHandle& handle,
                                   const std::shared_ptr<SchemaLayout>& layout,
                                   const std::shared_ptr<Field>& field);

  /// Reads a field as Value, resolving version-chain overrides when versioned.
  static Value get_value(const NodeHandle& handle,
                         const std::shared_ptr<SchemaLayout>& layout,
                         const std::shared_ptr<Field>& field);

  /**
   * Prepare a Value for the APPEND operation in versioned path.
   * Reads the current ArrayRef (from version chain or base node),
   * copies it (COW), appends the new element(s), and returns the new ArrayRef.
   */
  arrow::Result<Value> prepare_append_value(
      const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout& field_layout, const Value& new_value);

  /**
   * Set field in v0 (initial population).
   * Writes to base node without creating versions.
   */
  arrow::Status set_field_value_v0(NodeHandle& handle,
                                   const std::shared_ptr<SchemaLayout>& layout,
                                   const std::shared_ptr<Field>& field,
                                   const Value& value);

  // =========================================================================
  // apply_updates — single public write entry point
  // =========================================================================

  /// Applies field updates; creates a new version when versioning is enabled.
  arrow::Result<bool> apply_updates(NodeHandle& handle,
                                    const std::shared_ptr<SchemaLayout>& layout,
                                    const std::vector<FieldUpdate>& updates);

  // =========================================================================
  // Map (properties) helpers
  // =========================================================================

  /**
   * Allocate an empty MapRef in the map arena.
   * Use this to create the initial MapRef for a MAP field.
   */
  arrow::Result<MapRef> allocate_map(
      uint32_t capacity = MapArena::DEFAULT_CAPACITY) {
    return map_arena_->allocate(capacity);
  }

  /** Reset arenas (keeps chunks). */
  void reset() {
    mem_arena_->reset();
    string_arena_->reset();
  }

  /** Clear all memory. */
  void clear() {
    mem_arena_->clear();
    string_arena_->clear();
  }

  /** Get string arena. */
  StringArena* get_string_arena() const { return string_arena_.get(); }

  /** Get array arena. */
  ArrayArena* get_array_arena() const { return array_arena_.get(); }

  /** Get map arena. */
  MapArena* get_map_arena() const { return map_arena_.get(); }

  // Statistics and getters
  /// Sum of bytes returned from the underlying node MemArena (see MemArena
  /// semantics).
  size_t get_total_allocated() const {
    return mem_arena_->get_total_allocated();
  }
  /// Number of chunks backing the node MemArena.
  size_t get_chunk_count() const { return mem_arena_->get_chunk_count(); }
  /// Underlying arena used for fixed-size node payloads.
  MemArena* get_mem_arena() const { return mem_arena_.get(); }
  /// Whether temporal versioning (version arena) is active for this NodeArena.
  bool is_versioning_enabled() const { return versioning_enabled_; }
  /// Monotonic counter used to assign unique VersionInfo::version_id values.
  uint64_t get_version_counter() const {
    return version_counter_.load(std::memory_order_relaxed);
  }

  /**
   * Get the field value pointer starting from a specific version.
   * Used by NodeView for temporal queries.
   *
   * @param handle NodeHandle (for accessing base node if needed)
   * @param version Starting version (pre-resolved by TemporalContext)
   * @param layout Schema layout
   * @param field Field to read
   * @return Pointer to field data or error if not found
   */
  static const char* get_value_ptr_at_version(
      const NodeHandle& handle, const VersionInfo* version,
      const std::shared_ptr<SchemaLayout>& layout,
      const std::shared_ptr<Field>& field);

  /**
   * Get field value starting from a specific version.
   * Used by NodeView for temporal queries.
   */
  static arrow::Result<Value> get_value_at_version(
      const NodeHandle& handle, const VersionInfo* version,
      const std::shared_ptr<SchemaLayout>& layout,
      const std::shared_ptr<Field>& field);

 private:
  static uint64_t get_current_timestamp_ns();

  // ---- apply_updates helpers ------------------------------------------------

  /** Resolve FieldUpdates to IndexedFieldUpdates using the schema layout. */
  static arrow::Result<std::vector<IndexedFieldUpdate>> resolve_field_indices(
      const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<FieldUpdate>& updates);

  /** Non-versioned path: write schema fields directly to base node memory. */
  arrow::Status apply_non_versioned_schema_updates(
      NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<IndexedFieldUpdate>& schema_updates);

  /** Allocate and construct a new VersionInfo, chained after the current. */
  arrow::Result<VersionInfo*> allocate_version(const NodeHandle& handle,
                                               uint64_t now);

  /**
   * Batch-allocate storage for schema fields and write each value into the
   * given VersionInfo's updated_fields map.
   */
  arrow::Status materialize_versioned_schema_fields(
      NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<IndexedFieldUpdate>& schema_updates,
      VersionInfo* target_vi);

  // ---- end apply_updates helpers --------------------------------------------

  /** Write field directly to node memory (handles strings/arrays). */
  arrow::Status set_field_value_internal(
      void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout* field_layout, const Value& value,
      UpdateType update_type = UpdateType::SET);

  // ---- nested-path update helpers -------------------------------------------

  /**
   * Materialise a Value suitable for storing a scalar into a MapEntry.
   * Converts std::string -> StringRef via the string arena; primitives
   * are returned unchanged.
   */
  arrow::Result<Value> materialise_map_value(const Value& value);

  /**
   * Set a single key inside an existing (or new) MapRef.
   * Handles COW growth when the map is full and string materialisation.
   */
  arrow::Status set_nested_map_key(MapRef& ref, const std::string& key,
                                   const Value& value);

  /**
   * Non-versioned nested-path update: read current composite value from the
   * base node, apply update, and write back.
   *
   * Current implementation supports MAP-backed paths (depth 1).
   */
  arrow::Status apply_nested_path_update_non_versioned(
      void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout* fl, const std::vector<std::string>& nested_path,
      const Value& value);

  /**
   * Versioned nested-path update: read current composite value from version
   * chain or base, apply update, and return the new value.
   *
   * Current implementation supports MAP-backed paths (depth 1).
   */
  arrow::Result<Value> apply_nested_path_update_versioned(
      const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout& fl, const std::vector<std::string>& nested_path,
      const Value& value);

  // ---- end nested-path update helpers ---------------------------------------

  /**
   * APPEND implementation for array fields (non-versioned path).
   *
   * Reads the current ArrayRef, copies it (COW), appends the new element(s),
   * marks the old array for deletion, and writes the new ref back.
   */
  arrow::Status append_to_array_field(
      void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout* field_layout, const Value& value);

  /**
   * How many extra slots copy() should pre-allocate so that the
   * subsequent append() calls won't trigger a second reallocation.
   * Returns 0 when the array already has enough spare capacity.
   */
  static uint32_t grow_for_append(const ArrayRef& ref, uint32_t n);

  /** Append a single Value element to an ArrayRef via the arena. */
  arrow::Status append_single_element(ArrayRef& ref,
                                      const TypeDescriptor& type_desc,
                                      const Value& elem);

  /**
   * Get field pointer from version chain.
   * Returns pair<found, ptr>:
   *   - {true, nullptr}  = field found and is explicitly NULL
   *   - {true, ptr}      = field found with value at ptr
   *   - {false, nullptr} = field not found in version chain (read from base)
   */
  static std::pair<bool, const char*> get_field_ptr_from_version_chain(
      const VersionInfo* version_info, uint16_t field_idx);

  /** Write value to memory (type-safe). */
  static bool write_value_to_memory(char* ptr, ValueType type,
                                    const Value& value);

  /**
   * Convert a raw array (std::vector<Value>) to an arena-backed ArrayRef.
   * Mirrors what string_arena_->store_string_auto() does for strings.
   *
   * @param type_desc  Field's TypeDescriptor (carries element_type)
   * @param elements   Raw element values
   * @return Ok(ArrayRef) or Error with reason (e.g. allocation failure)
   */
  arrow::Result<ArrayRef> store_raw_array(const TypeDescriptor& type_desc,
                                          const std::vector<Value>& elements);

  /**
   * Convert a raw map (std::map<std::string, Value>) to an arena-backed MapRef.
   *
   * @param entries Raw key/value pairs
   * @return Ok(MapRef) or Error with reason (e.g. allocation failure)
   */
  arrow::Result<MapRef> store_raw_map(
      const std::map<std::string, Value>& entries);

  std::unique_ptr<MemArena> mem_arena_;
  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::unique_ptr<StringArena> string_arena_;
  std::unique_ptr<ArrayArena> array_arena_;
  std::unique_ptr<MapArena> map_arena_;

  // Versioning (optional)
  bool versioning_enabled_;
  std::unique_ptr<FreeListArena> version_arena_;
  // Global (arena-wide) monotonic counter, NOT per-node.  This gives every
  // VersionInfo a unique id that establishes a total ordering of all mutations
  // across all nodes in the arena - useful for cross-node temporal comparisons
  // without relying on timestamp resolution.  Per-node ids would only give a
  // local ordering with no way to relate versions of different nodes.
  std::atomic<uint64_t> version_counter_;
  // Tracks all placement-new'd VersionInfo objects so we can call their
  // destructors (SmallDenseMap may heap-allocate on grow).
  std::vector<VersionInfo*> version_infos_;
};

/** Factory functions for creating NodeArenas. */
namespace node_arena_factory {

/** Create NodeArena with MemoryArena (fast, no individual deallocation). */
inline std::unique_ptr<NodeArena> create_simple_arena(
    const std::shared_ptr<LayoutRegistry>& layout_registry,
    size_t initial_size = NodeArena::kInitialSize,
    bool enable_versioning = false) {
  auto mem_arena = std::make_unique<MemoryArena>(initial_size);
  return std::make_unique<NodeArena>(std::move(mem_arena), layout_registry,
                                     nullptr, enable_versioning);
}

/** Create NodeArena with FreeListArena (supports individual deallocation). */
inline std::unique_ptr<NodeArena> create_free_list_arena(
    const std::shared_ptr<LayoutRegistry>& layout_registry,
    size_t initial_size = NodeArena::kInitialSize,
    size_t min_fragment_size = NodeArena::kMinFragmentSize,
    bool enable_versioning = false) {
  auto mem_arena =
      std::make_unique<FreeListArena>(initial_size, min_fragment_size);
  return std::make_unique<NodeArena>(std::move(mem_arena), layout_registry,
                                     nullptr, enable_versioning);
}

}  // namespace node_arena_factory

}  // namespace tundradb

#endif  // NODE_ARENA_HPP
