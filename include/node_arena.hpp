#ifndef NODE_ARENA_HPP
#define NODE_ARENA_HPP

#include <llvm/ADT/DenseMap.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <limits>
#include <memory>
#include <string>

#include "clock.hpp"
#include "free_list_arena.hpp"
#include "mem_arena.hpp"
#include "memory_arena.hpp"
#include "schema_layout.hpp"
#include "string_arena.hpp"
#include "types.hpp"

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
        versioning_enabled_(enable_versioning),
        version_counter_(0) {
    // Only allocate version arena if versioning is enabled
    if (versioning_enabled_) {
      // Use FreeListArena for versions (supports individual deallocation)
      // Default 4MB - expect more versions than base nodes
      version_arena_ = std::make_unique<FreeListArena>(4 * 1024 * 1024);
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
      version_info->version_id = 0;
      version_info->valid_from = now;
      version_info->valid_to = std::numeric_limits<uint64_t>::max();
      version_info->prev = nullptr;

      return {node_data, node_size, layout->get_schema_name(), 1, version_info};
    }
    return {node_data, node_size, layout->get_schema_name()};
  }

  /** Deallocate node and its strings. */
  void deallocate_node(const NodeHandle& handle) const {
    if (handle.is_null()) {
      return;
    }

    // First, deallocate all string references from the node
    if (!handle.schema_name.empty()) {
      const std::shared_ptr<SchemaLayout> layout =
          layout_registry_->get_layout(handle.schema_name);
      if (layout) {
        const char* data_start =
            static_cast<const char*>(handle.ptr) + layout->get_data_offset();
        for (const auto& field : layout->get_fields()) {
          if (is_string_type(field.type)) {
            // Read the StringRef from the node memory (after data offset)
            const char* field_ptr = data_start + field.offset;
            const auto* str_ref = reinterpret_cast<const StringRef*>(field_ptr);

            // Deallocate the string if it's not null
            if (!str_ref->is_null()) {
              string_arena_->mark_for_deletion(*str_ref);
            }
          }
        }
      }
    }

    // Then deallocate the node memory itself
    mem_arena_->deallocate(handle.ptr);
  }

  /** Get field value pointer. */
  static const char* get_field_value_ptr(
      const NodeHandle& handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::shared_ptr<Field>& field) {
    // Logger::get_instance().debug("get_field_value: {}.{}", schema_name,
    //                              field_name);
    if (handle.is_null()) {
      // Logger::get_instance().error("null value for invalid handle");
      return nullptr;  // null value for invalid handle
    }

    return layout->get_field_value_ptr(static_cast<const char*>(handle.ptr),
                                       field);
  }

  static Value get_field_value(const NodeHandle& handle,
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
      return layout->get_field_value(static_cast<const char*>(handle.ptr),
                                     field);
    }

    // Non-versioned: direct read from base node
    return layout->get_field_value(static_cast<const char*>(handle.ptr), field);
  }

  /** Update multiple fields atomically (creates one version). */
  arrow::Result<bool> update_fields(
      NodeHandle& current_handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<std::pair<std::shared_ptr<Field>, Value>>&
          field_updates) {
    // Convert Field pointers to indices
    std::vector<std::pair<uint16_t, Value>> indexed_updates;
    indexed_updates.reserve(field_updates.size());

    for (const auto& [field, value] : field_updates) {
      const FieldLayout* field_layout = layout->get_field_layout(field);
      if (!field_layout) {
        return arrow::Status::Invalid("Invalid field in update_fields");
      }
      indexed_updates.emplace_back(field_layout->index, value);
    }

    return update_fields_by_index(current_handle, layout, indexed_updates);
  }

  /**
   * Create new version by updating a single field.
   * For multiple fields, use update_fields() instead.
   */
  arrow::Result<bool> create_new_version(
      NodeHandle& current_handle, const std::shared_ptr<SchemaLayout>& layout,
      uint16_t field_idx, const Value& new_value) {
    if (field_idx >= layout->get_fields().size()) {
      return arrow::Status::IndexError("Field index out of bounds");
    }
    const std::vector<std::pair<uint16_t, Value>> updates = {
        {field_idx, new_value}};
    return update_fields_by_index(current_handle, layout, updates);
  }

  /** Update multiple fields by index (internal, more efficient). */
  arrow::Result<bool> update_fields_by_index(
      NodeHandle& current_handle, const std::shared_ptr<SchemaLayout>& layout,
      const std::vector<std::pair<uint16_t, Value>>& field_updates) {
    if (field_updates.empty()) return true;

    // Non-versioned: write directly to base node
    if (!versioning_enabled_ || !current_handle.is_versioned()) {
      for (const auto& [field_idx, value] : field_updates) {
        if (field_idx >= layout->get_fields().size()) {
          return arrow::Status::IndexError("Field index out of bounds");
        }
        const FieldLayout& field_layout = layout->get_fields()[field_idx];
        if (!set_field_value_internal(current_handle.ptr, layout, &field_layout,
                                      value)) {
          return arrow::Status::Invalid("Failed to set field value");
        }
      }
      return true;
    }

    const uint64_t now = get_current_timestamp_ns();

    // Allocate new VersionInfo
    void* version_info_memory =
        version_arena_->allocate(sizeof(VersionInfo), alignof(VersionInfo));
    if (!version_info_memory) {
      return arrow::Status::OutOfMemory("Failed to allocate VersionInfo");
    }

    uint64_t new_version_id =
        version_counter_.fetch_add(1, std::memory_order_relaxed) + 1;
    VersionInfo* old_version_info = current_handle.version_info_;
    VersionInfo* new_version_info = new (version_info_memory)
        VersionInfo(new_version_id, now, old_version_info);

    // ========================================================================
    // BATCH ALLOCATION: Calculate total memory needed for all fields
    // ========================================================================
    size_t total_size = 0;
    size_t max_alignment = 1;

    // First pass: calculate total size and max alignment
    for (const auto& [field_idx, new_value] : field_updates) {
      if (field_idx >= layout->get_fields().size()) {
        return arrow::Status::IndexError("Field index out of bounds");
      }

      if (!new_value.is_null()) {  // NULL uses nullptr sentinel (no allocation)
        const FieldLayout& field_layout = layout->get_fields()[field_idx];
        total_size += field_layout.size;
        max_alignment = std::max(max_alignment, field_layout.alignment);
      }
    }

    // Batch allocate memory for all non-null fields
    char* batch_memory = nullptr;
    if (total_size > 0) {
      batch_memory = static_cast<char*>(
          version_arena_->allocate(total_size, max_alignment));
      if (!batch_memory) {
        return arrow::Status::OutOfMemory(
            "Failed to batch allocate field storage");
      }
    }

    // Second pass: write values and assign pointers
    size_t offset = 0;
    for (const auto& [field_idx, new_value] : field_updates) {
      const FieldLayout& field_layout = layout->get_fields()[field_idx];

      // Handle NULL: use nullptr sentinel
      if (new_value.is_null()) {
        new_version_info->updated_fields[field_idx] = nullptr;
        continue;  // Skip batch_memory usage for NULL fields
      }

      // At this point, field is non-NULL, so batch_memory must be allocated
      // (because total_size > 0 when any field is non-NULL)
      assert(batch_memory != nullptr &&
             "Batch memory must be allocated for non-null fields");

      // Prepare value (convert strings to StringRef)
      Value storage_value = new_value;
      if (new_value.type() == ValueType::STRING) {
        const StringRef str_ref =
            string_arena_->store_string_auto(new_value.as_string());
        storage_value = Value{str_ref, field_layout.type};
      }

      // Use batch-allocated memory (safe because batch_memory != nullptr here)
      char* field_storage = batch_memory + offset;
      offset += field_layout.size;

      if (!write_value_to_memory(field_storage, field_layout.type,
                                 storage_value)) {
        return arrow::Status::TypeError("Type mismatch writing field value");
      }

      new_version_info->updated_fields[field_idx] = field_storage;
    }

    old_version_info->valid_to = now;
    current_handle.version_info_ = new_version_info;

    return true;
  }

  /**
   * Set field in v0 (initial population).
   * Writes to base node without creating versions.
   */
  bool set_field_value_v0(NodeHandle& handle,
                          const std::shared_ptr<SchemaLayout>& layout,
                          const std::shared_ptr<Field>& field,
                          const Value& value) {
    assert(!handle.is_null());

    const FieldLayout* field_layout = layout->get_field_layout(field);
    if (!field_layout) {
      return false;
    }

    // Write directly to base node
    return set_field_value_internal(handle.ptr, layout, field_layout, value);
  }

  /**
   * Set field value.
   * Creates new version if versioning enabled, direct write otherwise.
   */
  bool set_field_value(NodeHandle& handle,
                       const std::shared_ptr<SchemaLayout>& layout,
                       const std::shared_ptr<Field>& field,
                       const Value& value) {
    assert(!handle.is_null());

    const FieldLayout* field_layout = layout->get_field_layout(field);
    if (!field_layout) {
      return false;  // Invalid field
    }

    // VERSIONED PATH: Create a new version
    if (versioning_enabled_ && handle.is_versioned()) {
      auto result =
          create_new_version(handle, layout, field_layout->index, value);
      return result.ok() && result.ValueOrDie();
    }

    // ========================================================================
    // NON-VERSIONED PATH: Direct write (in-place update)
    // ========================================================================

    // If the field currently contains a string, deallocate it first
    if (is_string_type(field_layout->type) &&
        is_field_set(static_cast<char*>(handle.ptr), field_layout->index)) {
      const Value old_value = layout->get_field_value(
          static_cast<char*>(handle.ptr), *field_layout);
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

    // Handle string storage
    if (value.type() == ValueType::STRING) {
      const std::string& str_content = value.as_string();
      const StringRef str_ref = string_arena_->store_string_auto(str_content);
      return layout->set_field_value(static_cast<char*>(handle.ptr),
                                     *field_layout,
                                     Value{str_ref, field_layout->type});
    } else {
      return layout->set_field_value(static_cast<char*>(handle.ptr),
                                     *field_layout, value);
    }
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
  static const char* get_field_value_ptr_from_version(
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
    return layout->get_field_value_ptr(static_cast<const char*>(handle.ptr),
                                       field_layout->index);
  }

  /**
   * Get field value starting from a specific version.
   * Used by NodeView for temporal queries.
   */
  static arrow::Result<Value> get_field_value_from_version(
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
      return layout->get_field_value_from_ptr(field_ptr, *field_layout);
    }

    // Not in version chain, read from base node
    return layout->get_field_value(static_cast<const char*>(handle.ptr),
                                   *field_layout);
  }

 private:
  static uint64_t get_current_timestamp_ns() {
    return Clock::instance().now_nanos();
  }

  /** Write field directly to node memory (handles strings). */
  bool set_field_value_internal(void* node_ptr,
                                const std::shared_ptr<SchemaLayout>& layout,
                                const FieldLayout* field_layout,
                                const Value& value) const {
    // If the field currently contains a string, deallocate it first
    if (is_string_type(field_layout->type) &&
        is_field_set(static_cast<char*>(node_ptr), field_layout->index)) {
      Value old_value =
          layout->get_field_value(static_cast<char*>(node_ptr), *field_layout);
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

    // Handle string storage
    if (value.type() == ValueType::STRING) {
      const std::string& str_content = value.as_string();
      const StringRef str_ref = string_arena_->store_string_auto(str_content);
      return layout->set_field_value(static_cast<char*>(node_ptr),
                                     *field_layout,
                                     Value{str_ref, field_layout->type});
    }
    return layout->set_field_value(static_cast<char*>(node_ptr), *field_layout,
                                   value);
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

      default:
        return false;
    }
  }

  std::unique_ptr<MemArena> mem_arena_;
  std::shared_ptr<LayoutRegistry> layout_registry_;
  std::unique_ptr<StringArena> string_arena_;

  // Versioning (optional)
  bool versioning_enabled_;
  std::unique_ptr<FreeListArena> version_arena_;
  std::atomic<uint64_t> version_counter_;
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