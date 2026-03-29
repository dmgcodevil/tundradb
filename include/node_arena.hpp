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

#include "array_arena.hpp"
#include "clock.hpp"
#include "field_update.hpp"
#include "free_list_arena.hpp"
#include "map_arena.hpp"
#include "mem_arena.hpp"
#include "memory_arena.hpp"
#include "schema_layout.hpp"
#include "string_arena.hpp"
#include "types.hpp"
#include "update_type.hpp"

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

  // Constructor: initializes both valid and tx times to the same value
  VersionInfo(uint64_t vid, uint64_t ts_from, VersionInfo* prev_ver = nullptr)
      : version_id(vid),
        valid_from(ts_from),
        tx_from(ts_from),  // Initially tx_from = valid_from
        prev(prev_ver) {}

  // Check if valid at a specific VALIDTIME
  bool is_valid_at(uint64_t ts) const {
    return valid_from <= ts && ts < valid_to;
  }

  // Check if visible at a bitemporal snapshot (valid_time, tx_time)
  bool is_visible_at(uint64_t valid_time, uint64_t tx_time) const {
    return (valid_from <= valid_time && valid_time < valid_to) &&
           (tx_from <= tx_time && tx_time < tx_to);
  }

  // Find version visible at bitemporal snapshot
  const VersionInfo* find_version_at_snapshot(uint64_t valid_time,
                                              uint64_t tx_time) const {
    const VersionInfo* current = this;
    while (current != nullptr) {
      if (current->is_visible_at(valid_time, tx_time)) {
        return current;
      }
      current = current->prev;
    }
    return nullptr;
  }

  // Legacy: find version at VALIDTIME only (ignores TXNTIME)
  const VersionInfo* find_version_at_time(uint64_t ts) const {
    const VersionInfo* current = this;
    while (current != nullptr) {
      if (current->is_valid_at(ts)) return current;
      current = current->prev;
    }
    return nullptr;
  }

  size_t count_versions() const {
    size_t count = 1;
    const VersionInfo* current = prev;
    while (current != nullptr) {
      count++;
      current = current->prev;
    }
    return count;
  }

  bool is_field_cached(uint16_t field_idx) const {
    if (field_idx >= 64) return field_cache_.count(field_idx) > 0;
    return (cache_bitset_ & (1ULL << field_idx)) != 0;
  }

  void mark_field_cached(uint16_t field_idx) const {
    if (field_idx < 64) cache_bitset_ |= (1ULL << field_idx);
  }

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

  bool is_null() const { return ptr == nullptr; }
  bool is_versioned() const { return version_info_ != nullptr; }
  void set_version_info(VersionInfo* version_info) {
    version_info_ = version_info;
  }

  bool is_valid_at(uint64_t ts) const {
    if (!is_versioned()) return true;
    return version_info_->is_valid_at(ts);
  }

  uint64_t get_version_id() const {
    return is_versioned() ? version_info_->version_id : 0;
  }

  uint64_t get_valid_from() const {
    return is_versioned() ? version_info_->valid_from : 0;
  }

  uint64_t get_valid_to() const {
    return is_versioned() ? version_info_->valid_to
                          : std::numeric_limits<uint64_t>::max();
  }

  VersionInfo* get_version_info() const { return version_info_; }

  size_t count_versions() const {
    if (!is_versioned()) return 1;
    return version_info_->count_versions();
  }

  const VersionInfo* find_version_at_time(uint64_t ts) const {
    if (!is_versioned()) return nullptr;
    return version_info_->find_version_at_time(ts);
  }

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

  bool operator==(const NodeHandle& other) const {
    return ptr == other.ptr && size == other.size &&
           schema_name == other.schema_name &&
           schema_version == other.schema_version;
  }

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
            bool enable_versioning = false)
      : mem_arena_(std::move(mem_arena)),
        layout_registry_(std::move(layout_registry)),
        string_arena_(string_arena ? std::move(string_arena)
                                   : std::make_unique<StringArena>()),
        array_arena_(std::make_unique<ArrayArena>()),
        map_arena_(std::make_unique<MapArena>()),
        versioning_enabled_(enable_versioning),
        version_counter_(0) {
    if (versioning_enabled_) {
      version_arena_ = std::make_unique<FreeListArena>(4 * 1024 * 1024);
    }
  }

  ~NodeArena() {
    // VersionInfo objects are placement-new'd into version_arena_ memory.
    // Their SmallDenseMap members may heap-allocate, so we must call
    // destructors before the arena frees the underlying memory.
    for (auto* vi : version_infos_) {
      vi->~VersionInfo();
    }
  }

  /** Allocate new node (versioned if enabled). */
  NodeHandle allocate_node(const std::string& schema_name) {
    const std::shared_ptr<SchemaLayout> layout =
        layout_registry_->get_layout(schema_name);
    if (!layout) {
      return NodeHandle{};  // null handle for unknown schema
    }

    return allocate_node(layout);
  }

  /** Allocate new node with given layout. */
  NodeHandle allocate_node(const std::shared_ptr<SchemaLayout>& layout) {
    size_t node_size = layout->get_total_size_with_bitset();
    size_t alignment = layout->get_alignment();

    void* node_data = mem_arena_->allocate(node_size, alignment);
    if (!node_data) {
      return NodeHandle{};  // allocation failed
    }

    // Initialize the node data with default values
    layout->initialize_node_data(static_cast<char*>(node_data));

    // Create versioned or non-versioned handle based on configuration
    if (versioning_enabled_) {
      // Allocate VersionInfo (v0) in version_arena_
      void* version_info_memory =
          version_arena_->allocate(sizeof(VersionInfo), alignof(VersionInfo));
      if (!version_info_memory) {
        return NodeHandle{};  // Allocation failed
      }

      // Construct base version (v0)
      uint64_t now = get_current_timestamp_ns();
      auto* version_info = new (version_info_memory) VersionInfo();
      version_infos_.push_back(version_info);
      version_info->version_id = 0;
      version_info->valid_from = now;
      version_info->valid_to = std::numeric_limits<uint64_t>::max();
      version_info->prev = nullptr;

      return {node_data, node_size, layout->get_schema_name(), 1, version_info};
    }
    return {node_data, node_size, layout->get_schema_name()};
  }

  /** Get field value pointer. */
  static const char* get_value_ptr(const NodeHandle& handle,
                                   const std::shared_ptr<SchemaLayout>& layout,
                                   const std::shared_ptr<Field>& field) {
    // Logger::get_instance().debug("get_field_value: {}.{}", schema_name,
    //                              field_name);
    if (handle.is_null()) {
      // Logger::get_instance().error("null value for invalid handle");
      return nullptr;  // null value for invalid handle
    }

    return layout->get_value_ptr(static_cast<const char*>(handle.ptr), field);
  }

  static Value get_value(const NodeHandle& handle,
                         const std::shared_ptr<SchemaLayout>& layout,
                         const std::shared_ptr<Field>& field) {
    if (handle.is_null()) {
      return Value{};  // null value for invalid handle
    }

    // For versioned nodes, check version chain
    if (handle.is_versioned()) {
      const FieldLayout* field_layout = layout->get_field_layout(field);
      if (!field_layout) {
        return Value{};  // Invalid field
      }

      uint16_t field_idx = field_layout->index;

      // Traverse version chain to find the field
      const VersionInfo* current = handle.version_info_;
      while (current != nullptr) {
        auto it = current->updated_fields.find(field_idx);
        if (it != current->updated_fields.end()) {
          // Found in version chain
          // Check if it's nullptr (explicit NULL sentinel)
          if (it->second == nullptr) {
            return Value{};  // Explicitly set to NULL
          }
          // Read actual value from version_arena_
          return Value::read_value_from_memory(it->second, field_layout->type);
        }
        current = current->prev;
      }

      // Not found in version chain, read from base node
      return layout->get_value(static_cast<const char*>(handle.ptr), field);
    }

    // Non-versioned: direct read from base node
    return layout->get_value(static_cast<const char*>(handle.ptr), field);
  }

  /**
   * Prepare a Value for the APPEND operation in versioned path.
   * Reads the current ArrayRef (from version chain or base node),
   * copies it (COW), appends the new element(s), and returns the new ArrayRef.
   */
  arrow::Result<Value> prepare_append_value(
      const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout& field_layout, const Value& new_value) {
    if (!is_array_type(field_layout.type)) {
      return arrow::Status::TypeError(
          "APPEND is only valid for array fields, got: ",
          tundradb::to_string(field_layout.type));
    }

    // Read current ArrayRef from the version chain or base node
    ArrayRef current_ref;
    if (handle.is_versioned()) {
      auto [found, ptr] = get_field_ptr_from_version_chain(handle.version_info_,
                                                           field_layout.index);
      if (found && ptr) {
        current_ref = *reinterpret_cast<const ArrayRef*>(ptr);
      } else if (!found) {
        const char* base_ptr = layout->get_value_ptr(
            static_cast<const char*>(handle.ptr), field_layout.index);
        if (base_ptr) {
          current_ref = *reinterpret_cast<const ArrayRef*>(base_ptr);
        }
      }
    }

    if (new_value.holds_raw_array()) {
      const auto& elems = new_value.as_raw_array();
      if (elems.empty()) {
        if (current_ref.is_null()) return Value{ArrayRef{}};
        ARROW_ASSIGN_OR_RAISE(ArrayRef copy, array_arena_->copy(current_ref));
        return Value{std::move(copy)};
      }
      if (current_ref.is_null()) {
        ARROW_ASSIGN_OR_RAISE(ArrayRef arr_ref,
                              store_raw_array(field_layout.type_desc, elems));
        return Value{std::move(arr_ref)};
      }
      const auto n = static_cast<uint32_t>(elems.size());
      ARROW_ASSIGN_OR_RAISE(
          ArrayRef new_ref,
          array_arena_->copy(current_ref, grow_for_append(current_ref, n)));
      for (const auto& elem : elems) {
        ARROW_RETURN_NOT_OK(
            append_single_element(new_ref, field_layout.type_desc, elem));
      }
      return Value{std::move(new_ref)};
    }

    // Single element
    if (current_ref.is_null()) {
      const std::vector<Value> elems = {new_value};
      ARROW_ASSIGN_OR_RAISE(ArrayRef arr_ref,
                            store_raw_array(field_layout.type_desc, elems));
      return Value{std::move(arr_ref)};
    }
    ARROW_ASSIGN_OR_RAISE(
        ArrayRef new_ref,
        array_arena_->copy(current_ref, grow_for_append(current_ref, 1)));
    ARROW_RETURN_NOT_OK(
        append_single_element(new_ref, field_layout.type_desc, new_value));
    return Value{std::move(new_ref)};
  }

  /**
   * Set field in v0 (initial population).
   * Writes to base node without creating versions.
   */
  arrow::Status set_field_value_v0(NodeHandle& handle,
                                   const std::shared_ptr<SchemaLayout>& layout,
                                   const std::shared_ptr<Field>& field,
                                   const Value& value) {
    assert(!handle.is_null());

    const FieldLayout* field_layout = layout->get_field_layout(field);
    if (!field_layout) {
      return arrow::Status::Invalid(
          "set_field_value_v0: field not found in layout");
    }

    // Write directly to base node
    return set_field_value_internal(handle.ptr, layout, field_layout, value);
  }

  // =========================================================================
  // apply_updates — single public write entry point
  // =========================================================================

  arrow::Result<bool> apply_updates(NodeHandle& handle,
                                    const std::shared_ptr<SchemaLayout>& layout,
                                    const std::vector<FieldUpdate>& updates) {
    ARROW_ASSIGN_OR_RAISE(auto schema_updates,
                          resolve_field_indices(layout, updates));

    if (!versioning_enabled_ || !handle.is_versioned()) {
      ARROW_RETURN_NOT_OK(
          apply_non_versioned_schema_updates(handle, layout, schema_updates));
      return true;
    }

    if (schema_updates.empty()) {
      return true;
    }

    const uint64_t now = get_current_timestamp_ns();
    ARROW_ASSIGN_OR_RAISE(auto* new_vi, allocate_version(handle, now));

    ARROW_RETURN_NOT_OK(materialize_versioned_schema_fields(
        handle, layout, schema_updates, new_vi));

    handle.version_info_->valid_to = now;
    handle.version_info_ = new_vi;
    return true;
  }

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
  size_t get_total_allocated() const {
    return mem_arena_->get_total_allocated();
  }
  size_t get_chunk_count() const { return mem_arena_->get_chunk_count(); }
  MemArena* get_mem_arena() const { return mem_arena_.get(); }
  bool is_versioning_enabled() const { return versioning_enabled_; }
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
      const std::shared_ptr<Field>& field) {
    const FieldLayout* field_layout = layout->get_field_layout(field);
    if (!field_layout) {
      return nullptr;
    }

    auto [found, field_ptr] =
        get_field_ptr_from_version_chain(version, field_layout->index);

    if (found) {
      return field_ptr;
    }

    // Not in version chain, read from base node
    return layout->get_value_ptr(static_cast<const char*>(handle.ptr),
                                 field_layout->index);
  }

  /**
   * Get field value starting from a specific version.
   * Used by NodeView for temporal queries.
   */
  static arrow::Result<Value> get_value_at_version(
      const NodeHandle& handle, const VersionInfo* version,
      const std::shared_ptr<SchemaLayout>& layout,
      const std::shared_ptr<Field>& field) {
    const FieldLayout* field_layout = layout->get_field_layout(field);
    if (!field_layout) {
      return arrow::Status::KeyError("Field not found in layout");
    }

    // Try to find in version chain first
    auto [found, field_ptr] =
        get_field_ptr_from_version_chain(version, field_layout->index);

    if (found) {
      if (field_ptr == nullptr) {
        // Explicit NULL value
        return Value{};
      }
      // Read value from version chain
      return layout->get_value_from_ptr(field_ptr, *field_layout);
    }

    // Not in version chain, read from base node
    return layout->get_value(static_cast<const char*>(handle.ptr),
                             *field_layout);
  }

 private:
  static uint64_t get_current_timestamp_ns() {
    return Clock::instance().now_nanos();
  }

  // ---- apply_updates helpers ------------------------------------------------

  /** Resolve FieldUpdates to IndexedFieldUpdates using the schema layout. */
  static arrow::Result<std::vector<IndexedFieldUpdate>> resolve_field_indices(
      const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<FieldUpdate>& updates) {
    std::vector<IndexedFieldUpdate> result;
    result.reserve(updates.size());
    for (const auto& upd : updates) {
      const FieldLayout* fl = layout->get_field_layout(upd.field);
      if (!fl) {
        return arrow::Status::Invalid("Invalid field in apply_updates: ",
                                      upd.field->name());
      }
      result.push_back({static_cast<uint16_t>(fl->index), upd.value, upd.op,
                        upd.nested_path});
    }
    return result;
  }

  /** Non-versioned path: write schema fields directly to base node memory. */
  arrow::Status apply_non_versioned_schema_updates(
      NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<IndexedFieldUpdate>& schema_updates) {
    for (const auto& upd : schema_updates) {
      if (upd.field_idx >= layout->get_fields().size()) {
        return arrow::Status::IndexError("Field index out of bounds");
      }
      const FieldLayout& fl = layout->get_fields()[upd.field_idx];

      if (!upd.nested_path.empty()) {
        ARROW_RETURN_NOT_OK(apply_nested_path_update_non_versioned(
            handle.ptr, layout, &fl, upd.nested_path, upd.value));
        continue;
      }

      ARROW_RETURN_NOT_OK(
          set_field_value_internal(handle.ptr, layout, &fl, upd.value, upd.op));
    }
    return arrow::Status::OK();
  }

  /** Allocate and construct a new VersionInfo, chained after the current. */
  arrow::Result<VersionInfo*> allocate_version(const NodeHandle& handle,
                                               const uint64_t now) {
    void* vi_mem =
        version_arena_->allocate(sizeof(VersionInfo), alignof(VersionInfo));
    if (!vi_mem) {
      return arrow::Status::OutOfMemory("Failed to allocate VersionInfo");
    }
    const uint64_t vid =
        version_counter_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto* new_vi = new (vi_mem) VersionInfo(vid, now, handle.version_info_);
    version_infos_.push_back(new_vi);
    return new_vi;
  }

  /**
   * Batch-allocate storage for schema fields and write each value into the
   * given VersionInfo's updated_fields map.
   */
  arrow::Status materialize_versioned_schema_fields(
      NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<IndexedFieldUpdate>& schema_updates,
      VersionInfo* target_vi) {
    size_t total_size = 0;
    size_t max_alignment = 1;
    for (const auto& upd : schema_updates) {
      const FieldLayout& fl = layout->get_fields()[upd.field_idx];
      if (upd.op == UpdateType::APPEND || !upd.value.is_null()) {
        total_size += fl.size;
        max_alignment = std::max(max_alignment, fl.alignment);
      }
    }

    char* batch_memory = nullptr;
    if (total_size > 0) {
      batch_memory = static_cast<char*>(
          version_arena_->allocate(total_size, max_alignment));
      if (!batch_memory) {
        return arrow::Status::OutOfMemory(
            "Failed to batch allocate field storage");
      }
      std::memset(batch_memory, 0, total_size);
    }

    size_t offset = 0;
    for (const auto& upd : schema_updates) {
      const FieldLayout& fl = layout->get_fields()[upd.field_idx];

      if (!upd.nested_path.empty()) {
        ARROW_ASSIGN_OR_RAISE(
            Value map_val, apply_nested_path_update_versioned(
                               handle, layout, fl, upd.nested_path, upd.value));
        assert(batch_memory != nullptr);
        char* field_storage = batch_memory + offset;
        offset += fl.size;
        if (!write_value_to_memory(field_storage, fl.type, map_val)) {
          return arrow::Status::TypeError("Type mismatch writing MAP field");
        }
        target_vi->updated_fields[upd.field_idx] = field_storage;
        continue;
      }

      if (upd.op == UpdateType::SET && upd.value.is_null()) {
        target_vi->updated_fields[upd.field_idx] = nullptr;
        continue;
      }

      assert(batch_memory != nullptr);
      Value storage_value = upd.value;

      if (upd.op == UpdateType::APPEND) {
        ARROW_ASSIGN_OR_RAISE(
            storage_value, prepare_append_value(handle, layout, fl, upd.value));
      } else {
        if (upd.value.type() == ValueType::STRING &&
            upd.value.holds_std_string()) {
          ARROW_ASSIGN_OR_RAISE(
              StringRef str_ref,
              string_arena_->store_string_auto(upd.value.as_string()));
          storage_value = Value{str_ref, fl.type};
        } else if (upd.value.type() == ValueType::ARRAY &&
                   upd.value.holds_raw_array()) {
          ARROW_ASSIGN_OR_RAISE(
              ArrayRef arr_ref,
              store_raw_array(fl.type_desc, upd.value.as_raw_array()));
          storage_value = Value{std::move(arr_ref)};
        }
      }

      char* field_storage = batch_memory + offset;
      offset += fl.size;

      if (!write_value_to_memory(field_storage, fl.type, storage_value)) {
        return arrow::Status::TypeError("Type mismatch writing field value");
      }
      target_vi->updated_fields[upd.field_idx] = field_storage;
    }
    return arrow::Status::OK();
  }

  // ---- end apply_updates helpers --------------------------------------------

  /** Write field directly to node memory (handles strings/arrays). */
  arrow::Status set_field_value_internal(
      void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout* field_layout, const Value& value,
      UpdateType update_type = UpdateType::SET) {
    if (update_type == UpdateType::APPEND) {
      return append_to_array_field(node_ptr, layout, field_layout, value);
    }

    // If the field currently contains a string, deallocate it first
    if (is_string_type(field_layout->type) &&
        is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
      Value old_value =
          layout->get_value(static_cast<char*>(node_ptr), *field_layout);
      if (!old_value.is_null() && old_value.type() != ValueType::NA) {
        try {
          const StringRef& old_str_ref = old_value.as_string_ref();
          if (!old_str_ref.is_null()) {
            string_arena_->mark_for_deletion(old_str_ref);
          }
        } catch (...) {
          // Old value wasn't a StringRef, ignore
        }
      }
    }

    // If the field currently contains an array, mark for deletion
    if (is_array_type(field_layout->type) &&
        is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
      Value old_value =
          layout->get_value(static_cast<char*>(node_ptr), *field_layout);
      if (!old_value.is_null() && old_value.holds_array_ref()) {
        const ArrayRef& old_arr_ref = old_value.as_array_ref();
        if (!old_arr_ref.is_null()) {
          array_arena_->mark_for_deletion(old_arr_ref);
        }
      }
    }

    // If the field currently contains a map, mark for deletion
    if (is_map_type(field_layout->type) &&
        is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
      Value old_value =
          layout->get_value(static_cast<char*>(node_ptr), *field_layout);
      if (!old_value.is_null() && old_value.holds_map_ref()) {
        const MapRef& old_map_ref = old_value.as_map_ref();
        if (!old_map_ref.is_null()) {
          map_arena_->mark_for_deletion(old_map_ref);
        }
      }
    }

    // Handle string storage: std::string -> StringRef via arena
    if (value.type() == ValueType::STRING && value.holds_std_string()) {
      const std::string& str_content = value.as_string();
      ARROW_ASSIGN_OR_RAISE(StringRef str_ref,
                            string_arena_->store_string_auto(str_content));
      if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                   Value{str_ref, field_layout->type})) {
        return arrow::Status::Invalid("Failed to write string field value");
      }
      return arrow::Status::OK();
    }

    // Handle array storage: std::vector<Value> -> ArrayRef via arena
    if (value.type() == ValueType::ARRAY && value.holds_raw_array()) {
      ARROW_ASSIGN_OR_RAISE(
          ArrayRef arr_ref,
          store_raw_array(field_layout->type_desc, value.as_raw_array()));
      if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                   Value{std::move(arr_ref)})) {
        return arrow::Status::Invalid("Failed to write array field value");
      }
      return arrow::Status::OK();
    }

    // Value already holds arena-backed ref (StringRef / ArrayRef) or primitive
    if (!layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                 value)) {
      return arrow::Status::Invalid("Failed to write field value");
    }
    return arrow::Status::OK();
  }

  // ---- nested-path update helpers -------------------------------------------

  /**
   * Materialise a Value suitable for storing a scalar into a MapEntry.
   * Converts std::string → StringRef via the string arena; primitives
   * are returned unchanged.
   */
  arrow::Result<Value> materialise_map_value(const Value& value) {
    if (value.type() == ValueType::STRING && value.holds_std_string()) {
      ARROW_ASSIGN_OR_RAISE(
          StringRef sr, string_arena_->store_string_auto(value.as_string()));
      return Value{sr, ValueType::STRING};
    }
    return value;
  }

  /**
   * Set a single key inside an existing (or new) MapRef.
   * Handles COW growth when the map is full and string materialisation.
   */
  arrow::Status set_nested_map_key(MapRef& ref, const std::string& key,
                                   const Value& value) {
    if (ref.is_null()) {
      ARROW_ASSIGN_OR_RAISE(ref, map_arena_->allocate());
    }

    ARROW_ASSIGN_OR_RAISE(Value mat, materialise_map_value(value));
    ARROW_ASSIGN_OR_RAISE(StringRef key_ref,
                          string_arena_->store_string_auto(key));

    ValueType vtype = mat.type();
    // For string-like types stored inside maps the entry type is STRING.
    if (is_string_type(vtype)) vtype = ValueType::STRING;

    const void* vptr = nullptr;
    int32_t i32;
    int64_t i64;
    double d;
    float f;
    bool b;
    StringRef sr;
    ArrayRef ar;
    MapRef mr;

    switch (vtype) {
      case ValueType::INT32:
        i32 = mat.as_int32();
        vptr = &i32;
        break;
      case ValueType::INT64:
        i64 = mat.as_int64();
        vptr = &i64;
        break;
      case ValueType::DOUBLE:
        d = mat.as_double();
        vptr = &d;
        break;
      case ValueType::FLOAT:
        f = mat.as_float();
        vptr = &f;
        break;
      case ValueType::BOOL:
        b = mat.as_bool();
        vptr = &b;
        break;
      case ValueType::STRING:
        sr = mat.as_string_ref();
        vptr = &sr;
        break;
      case ValueType::ARRAY:
        if (!mat.holds_array_ref())
          return arrow::Status::Invalid(
              "nested_path update: raw arrays not supported");
        ar = mat.as_array_ref();
        vptr = &ar;
        break;
      case ValueType::MAP:
        if (!mat.holds_map_ref())
          return arrow::Status::Invalid(
              "nested_path update: raw maps not supported");
        mr = mat.as_map_ref();
        vptr = &mr;
        break;
      default:
        return arrow::Status::Invalid(
            "nested_path update: unsupported value type");
    }

    auto status = MapArena::set_entry(ref, key_ref, vtype, vptr);
    if (status.IsCapacityError()) {
      ARROW_ASSIGN_OR_RAISE(MapRef grown,
                            map_arena_->copy(ref, ref.capacity()));
      map_arena_->mark_for_deletion(ref);
      ref = std::move(grown);
      return MapArena::set_entry(ref, key_ref, vtype, vptr);
    }
    return status;
  }

  /**
   * Non-versioned nested-path update: read current composite value from the
   * base node, apply update, and write back.
   *
   * Current implementation supports MAP-backed paths (depth 1).
   */
  arrow::Status apply_nested_path_update_non_versioned(
      void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout* fl, const std::vector<std::string>& nested_path,
      const Value& value) {
    if (nested_path.empty()) {
      return arrow::Status::Invalid(
          "nested_path update requires at least one path segment");
    }
    if (nested_path.size() > 1) {
      return arrow::Status::NotImplemented(
          "nested_path update depth > 1 is not implemented yet");
    }
    const std::string& key = nested_path.front();
    if (!is_map_type(fl->type)) {
      return arrow::Status::TypeError("nested_path update on non-map field: ",
                                      tundradb::to_string(fl->type));
    }

    auto* base = static_cast<char*>(node_ptr);
    MapRef current;
    if (is_field_set(base, fl->index)) {
      Value old = layout->get_value(base, *fl);
      if (!old.is_null() && old.holds_map_ref()) current = old.as_map_ref();
    }

    MapRef copy;
    if (current.is_null()) {
      ARROW_ASSIGN_OR_RAISE(copy, map_arena_->allocate());
    } else {
      ARROW_ASSIGN_OR_RAISE(copy, map_arena_->copy(current));
      map_arena_->mark_for_deletion(current);
    }

    ARROW_RETURN_NOT_OK(set_nested_map_key(copy, key, value));

    if (!layout->set_field_value(base, *fl, Value{std::move(copy)})) {
      return arrow::Status::Invalid(
          "Failed to write map field after nested_path update");
    }
    return arrow::Status::OK();
  }

  /**
   * Versioned nested-path update: read current composite value from version
   * chain or base, apply update, and return the new value.
   *
   * Current implementation supports MAP-backed paths (depth 1).
   */
  arrow::Result<Value> apply_nested_path_update_versioned(
      const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout& fl, const std::vector<std::string>& nested_path,
      const Value& value) {
    if (nested_path.empty()) {
      return arrow::Status::Invalid(
          "nested_path update requires at least one path segment");
    }
    if (nested_path.size() > 1) {
      return arrow::Status::NotImplemented(
          "nested_path update depth > 1 is not implemented yet");
    }
    const std::string& key = nested_path.front();
    if (!is_map_type(fl.type)) {
      return arrow::Status::TypeError("nested_path update on non-map field: ",
                                      tundradb::to_string(fl.type));
    }

    MapRef current;
    if (handle.is_versioned()) {
      auto [found, ptr] =
          get_field_ptr_from_version_chain(handle.version_info_, fl.index);
      if (found && ptr) {
        current = *reinterpret_cast<const MapRef*>(ptr);
      } else if (!found) {
        const char* base_ptr = layout->get_value_ptr(
            static_cast<const char*>(handle.ptr), fl.index);
        if (base_ptr) {
          current = *reinterpret_cast<const MapRef*>(base_ptr);
        }
      }
    }

    MapRef copy;
    if (current.is_null()) {
      ARROW_ASSIGN_OR_RAISE(copy, map_arena_->allocate());
    } else {
      ARROW_ASSIGN_OR_RAISE(copy, map_arena_->copy(current));
    }

    ARROW_RETURN_NOT_OK(set_nested_map_key(copy, key, value));
    return Value{std::move(copy)};
  }

  // ---- end nested-path update helpers ---------------------------------------

  /**
   * APPEND implementation for array fields (non-versioned path).
   *
   * Reads the current ArrayRef, copies it (COW), appends the new element(s),
   * marks the old array for deletion, and writes the new ref back.
   */
  arrow::Status append_to_array_field(
      void* node_ptr, const std::shared_ptr<SchemaLayout>& layout,
      const FieldLayout* field_layout, const Value& value) {
    if (!is_array_type(field_layout->type)) {
      return arrow::Status::TypeError(
          "APPEND is only valid for array fields, got: ",
          tundradb::to_string(field_layout->type));
    }

    auto* base = static_cast<char*>(node_ptr);
    const bool field_is_set = is_field_set(base, field_layout->index);

    ArrayRef current_ref;
    if (field_is_set) {
      Value old_value = layout->get_value(base, *field_layout);
      if (!old_value.is_null() && old_value.holds_array_ref()) {
        current_ref = old_value.as_array_ref();
      }
    }

    if (value.holds_raw_array()) {
      const auto& elems = value.as_raw_array();
      if (elems.empty()) return arrow::Status::OK();

      ArrayRef new_ref;
      if (current_ref.is_null()) {
        ARROW_ASSIGN_OR_RAISE(new_ref,
                              store_raw_array(field_layout->type_desc, elems));
      } else {
        const auto n = static_cast<uint32_t>(elems.size());
        ARROW_ASSIGN_OR_RAISE(
            new_ref,
            array_arena_->copy(current_ref, grow_for_append(current_ref, n)));
        for (const auto& elem : elems) {
          ARROW_RETURN_NOT_OK(
              append_single_element(new_ref, field_layout->type_desc, elem));
        }
        array_arena_->mark_for_deletion(current_ref);
      }

      if (!layout->set_field_value(base, *field_layout,
                                   Value{std::move(new_ref)})) {
        return arrow::Status::Invalid(
            "Failed to write array field after APPEND");
      }
      return arrow::Status::OK();
    }

    // Single element append
    if (current_ref.is_null()) {
      const std::vector<Value> elems = {value};
      ARROW_ASSIGN_OR_RAISE(ArrayRef new_ref,
                            store_raw_array(field_layout->type_desc, elems));
      if (!layout->set_field_value(base, *field_layout,
                                   Value{std::move(new_ref)})) {
        return arrow::Status::Invalid(
            "Failed to write array field after APPEND");
      }
      return arrow::Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(
        ArrayRef new_ref,
        array_arena_->copy(current_ref, grow_for_append(current_ref, 1)));
    ARROW_RETURN_NOT_OK(
        append_single_element(new_ref, field_layout->type_desc, value));
    array_arena_->mark_for_deletion(current_ref);

    if (!layout->set_field_value(base, *field_layout,
                                 Value{std::move(new_ref)})) {
      return arrow::Status::Invalid("Failed to write array field after APPEND");
    }
    return arrow::Status::OK();
  }

  /**
   * How many extra slots copy() should pre-allocate so that the
   * subsequent append() calls won't trigger a second reallocation.
   * Returns 0 when the array already has enough spare capacity.
   */
  static uint32_t grow_for_append(const ArrayRef& ref, uint32_t n) {
    const uint32_t spare = ref.capacity() - ref.length();
    if (spare >= n) return 0;
    return n - spare;
  }

  /** Append a single Value element to an ArrayRef via the arena. */
  arrow::Status append_single_element(ArrayRef& ref,
                                      const TypeDescriptor& type_desc,
                                      const Value& elem) {
    switch (type_desc.element_type) {
      case ValueType::INT32: {
        int32_t v = elem.as_int32();
        return array_arena_->append(ref, &v);
      }
      case ValueType::INT64: {
        int64_t v = elem.as_int64();
        return array_arena_->append(ref, &v);
      }
      case ValueType::DOUBLE: {
        double v = elem.as_double();
        return array_arena_->append(ref, &v);
      }
      case ValueType::BOOL: {
        bool v = elem.as_bool();
        return array_arena_->append(ref, &v);
      }
      case ValueType::STRING: {
        ARROW_ASSIGN_OR_RAISE(
            StringRef sr, string_arena_->store_string_auto(elem.as_string()));
        return array_arena_->append(ref, &sr);
      }
      default:
        return arrow::Status::NotImplemented(
            "APPEND: unsupported element type: ",
            tundradb::to_string(type_desc.element_type));
    }
  }

  /** Traverse the version chain to find field pointer. */
  /**
   * Get field pointer from version chain.
   * Returns pair<found, ptr>:
   *   - {true, nullptr}  = field found and is explicitly NULL
   *   - {true, ptr}      = field found with value at ptr
   *   - {false, nullptr} = field not found in version chain (read from base)
   */
  static std::pair<bool, const char*> get_field_ptr_from_version_chain(
      const VersionInfo* version_info, uint16_t field_idx) {
    const VersionInfo* current = version_info;
    while (current != nullptr) {
      // Check if this version has an override for this field
      if (auto it = current->updated_fields.find(field_idx);
          it != current->updated_fields.end()) {
        return {true, it->second};  // Found (value or nullptr for NULL)
      }
      current = current->prev;
    }

    // Not found in any version - read from base node
    return {false, nullptr};
  }

  /** Write value to memory (type-safe). */
  static bool write_value_to_memory(char* ptr, ValueType type,
                                    const Value& value) {
    switch (type) {
      case ValueType::INT64:
        if (value.type() != ValueType::INT64) return false;
        *reinterpret_cast<int64_t*>(ptr) = value.as_int64();
        return true;

      case ValueType::INT32:
        if (value.type() != ValueType::INT32) return false;
        *reinterpret_cast<int32_t*>(ptr) = value.as_int32();
        return true;

      case ValueType::DOUBLE:
        if (value.type() != ValueType::DOUBLE) return false;
        *reinterpret_cast<double*>(ptr) = value.as_double();
        return true;

      case ValueType::BOOL:
        if (value.type() != ValueType::BOOL) return false;
        *reinterpret_cast<bool*>(ptr) = value.as_bool();
        return true;

      case ValueType::STRING:
      case ValueType::FIXED_STRING16:
      case ValueType::FIXED_STRING32:
      case ValueType::FIXED_STRING64:
        if (!is_string_type(value.type())) return false;
        *reinterpret_cast<StringRef*>(ptr) = value.as_string_ref();
        return true;

      case ValueType::ARRAY:
        if (value.type() != ValueType::ARRAY) return false;
        *reinterpret_cast<ArrayRef*>(ptr) = value.as_array_ref();
        return true;

      case ValueType::MAP:
        if (value.type() != ValueType::MAP) return false;
        *reinterpret_cast<MapRef*>(ptr) = value.as_map_ref();
        return true;

      default:
        return false;
    }
  }

  /**
   * Convert a raw array (std::vector<Value>) to an arena-backed ArrayRef.
   * Mirrors what string_arena_->store_string_auto() does for strings.
   *
   * @param type_desc  Field's TypeDescriptor (carries element_type)
   * @param elements   Raw element values
   * @return Ok(ArrayRef) or Error with reason (e.g. allocation failure)
   */
  arrow::Result<ArrayRef> store_raw_array(const TypeDescriptor& type_desc,
                                          const std::vector<Value>& elements) {
    const ValueType elem_type = type_desc.element_type;
    const auto count = static_cast<uint32_t>(elements.size());

    uint32_t capacity = count;
    if (type_desc.is_fixed_size_array() && type_desc.fixed_size > count) {
      capacity = type_desc.fixed_size;
    }

    ARROW_ASSIGN_OR_RAISE(ArrayRef ref,
                          array_arena_->allocate(elem_type, capacity));

    // Empty array: allocate(0) returns null ArrayRef; nothing to fill
    if (ref.is_null()) {
      return ref;
    }

    const size_t elem_sz = get_type_size(elem_type);
    auto* header = reinterpret_cast<ArrayRef::ArrayHeader*>(
        ref.data() - ArrayRef::HEADER_SIZE);

    for (uint32_t i = 0; i < count; ++i) {
      char* dest = ref.mutable_element_ptr(i);
      const Value& elem = elements[i];

      // For string elements, store via string arena first
      if (is_string_type(elem_type) && elem.holds_std_string()) {
        ARROW_ASSIGN_OR_RAISE(
            StringRef str_ref,
            string_arena_->store_string_auto(elem.as_string()));
        *reinterpret_cast<StringRef*>(dest) = std::move(str_ref);
      } else {
        // Write primitive or pre-allocated ref directly
        write_value_to_memory(dest, elem_type, elem);
      }
    }

    header->length = count;
    return ref;
  }

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