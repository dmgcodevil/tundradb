#ifndef NODE_ARENA_HPP
#define NODE_ARENA_HPP

#include <memory>
#include <string>

#include "free_list_arena.hpp"
#include "mem_arena.hpp"
#include "memory_arena.hpp"
#include "schema_layout.hpp"
#include "string_arena.hpp"
#include "types.hpp"

namespace tundradb {

/**
 * Handle to a node stored in the arena
 * Lightweight reference that can be passed around efficiently
 */
struct NodeHandle {
  void* ptr;    // Direct pointer to node data
  size_t size;  // Size of the node data

  NodeHandle() : ptr(nullptr), size(0) {}
  NodeHandle(void* p, size_t s) : ptr(p), size(s) {}

  bool is_null() const { return ptr == nullptr; }

  bool operator==(const NodeHandle& other) const {
    return ptr == other.ptr && size == other.size;
  }

  bool operator!=(const NodeHandle& other) const { return !(*this == other); }
};

/**
 * Simplified node arena that manages both node layout and string content
 * Uses TWO arenas: one for fixed-size node data, one for variable-size strings
 */
class NodeArena {
 public:
  /**
   * Constructor takes any MemArena implementation + StringArena for strings
   * @param mem_arena Underlying memory arena for node layouts (MemoryArena or
   * FreeListArena)
   * @param layout_registry Registry containing schema layouts
   * @param string_arena Arena for managing string content (optional, creates
   * own if null)
   */
  NodeArena(std::unique_ptr<MemArena> mem_arena,
            LayoutRegistry* layout_registry,
            std::unique_ptr<StringArena> string_arena = nullptr)
      : mem_arena_(std::move(mem_arena)),
        layout_registry_(layout_registry),
        string_arena_(string_arena ? std::move(string_arena)
                                   : std::make_unique<StringArena>()) {}

  /**
   * Allocate space for a new node of the given schema type
   * Returns a handle to the allocated node memory
   */
  NodeHandle allocate_node(const std::string& schema_name) {
    const SchemaLayout* layout = layout_registry_->get_layout(schema_name);
    if (!layout) {
      return NodeHandle{};  // null handle for unknown schema
    }

    size_t node_size = layout->get_total_size();
    size_t alignment = layout->get_alignment();

    void* node_data = mem_arena_->allocate(node_size, alignment);
    if (!node_data) {
      return NodeHandle{};  // allocation failed
    }

    // Initialize the node data with default values
    layout->initialize_node_data(static_cast<char*>(node_data));

    return NodeHandle(node_data, node_size);
  }

  /**
   * Deallocate a node (only works with FreeListArena)
   * For MemoryArena, this is a no-op
   */
  void deallocate_node(const NodeHandle& handle) {
    if (!handle.is_null()) {
      mem_arena_->deallocate(handle.ptr);
    }
  }

  /**
   * Get field value from a node using its handle
   */
  Value get_field_value(const NodeHandle& handle,
                        const std::string& schema_name,
                        const std::string& field_name) const {
    if (handle.is_null()) {
      return Value{};  // null value for invalid handle
    }

    const SchemaLayout* layout = layout_registry_->get_layout(schema_name);
    if (!layout) {
      return Value{};  // null value for unknown schema
    }

    return layout->get_field_value(static_cast<const char*>(handle.ptr),
                                   field_name);
  }

  /**
   * Set field value in a node using its handle
   * Automatically stores strings in the string arena and creates StringRef
   */
  bool set_field_value(const NodeHandle& handle, const std::string& schema_name,
                       const std::string& field_name, const Value& value) {
    if (handle.is_null()) {
      return false;  // invalid handle
    }

    SchemaLayout* layout = layout_registry_->get_layout(schema_name);
    if (!layout) {
      return false;  // unknown schema
    }

    // For string values, store in string arena and convert to StringRef
    Value storage_value = value;
    const FieldLayout* field_layout = layout->get_field_layout(field_name);
    if (field_layout && is_string_type(field_layout->type)) {
      if (value.type() == ValueType::String) {
        // Check if it's a temporary std::string that needs to be stored in
        // arena
        try {
          const std::string& str_content = value.as_string();
          StringRef str_ref = string_arena_->store_string_auto(str_content);
          storage_value = Value{str_ref};
        } catch (...) {
          // Already a StringRef, use as-is
          storage_value = value;
        }
      }
    }

    return layout->set_field_value(static_cast<char*>(handle.ptr), field_name,
                                   storage_value);
  }

  /**
   * Reset the arena - keeps allocated chunks but resets usage
   */
  void reset() {
    mem_arena_->reset();
    string_arena_->reset();
  }

  /**
   * Clear all allocated memory
   */
  void clear() {
    mem_arena_->clear();
    string_arena_->clear();
  }

  /**
   * Get the string arena for direct string management
   */
  StringArena* get_string_arena() const { return string_arena_.get(); }

  // Statistics
  size_t get_total_allocated() const {
    return mem_arena_->get_total_allocated();
  }

  size_t get_chunk_count() const { return mem_arena_->get_chunk_count(); }

  // Get the underlying arena (for advanced usage)
  MemArena* get_mem_arena() const { return mem_arena_.get(); }

 private:
  std::unique_ptr<MemArena> mem_arena_;  // For fixed-size node layouts
  LayoutRegistry* layout_registry_;
  std::unique_ptr<StringArena>
      string_arena_;  // For variable-size string content
};

/**
 * Factory functions for creating NodeArenas with different underlying
 * implementations
 */
namespace node_arena_factory {

/**
 * Create a NodeArena using MemoryArena (fast allocation, reset/clear only)
 * Creates its own StringArena for string management
 */
inline std::unique_ptr<NodeArena> create_simple_arena(
    LayoutRegistry* layout_registry,
    size_t initial_size = 2 * 1024 * 1024) {  // 2MB default
  auto mem_arena = std::make_unique<MemoryArena>(initial_size);
  return std::make_unique<NodeArena>(std::move(mem_arena), layout_registry);
}

/**
 * Create a NodeArena using FreeListArena (individual deallocation supported)
 * Creates its own StringArena for string management
 */
inline std::unique_ptr<NodeArena> create_free_list_arena(
    LayoutRegistry* layout_registry,
    size_t initial_size = 2 * 1024 * 1024,  // 2MB default
    size_t min_fragment_size = 64) {        // 64 bytes minimum fragment
  auto mem_arena =
      std::make_unique<FreeListArena>(initial_size, min_fragment_size);
  return std::make_unique<NodeArena>(std::move(mem_arena), layout_registry);
}

}  // namespace node_arena_factory

}  // namespace tundradb

#endif  // NODE_ARENA_HPP