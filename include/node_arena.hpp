#ifndef NODE_ARENA_HPP
#define NODE_ARENA_HPP

#include <cassert>
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
 * Includes schema version for evolution support
 */
struct NodeHandle {
  void* ptr;                // Direct pointer to node data
  size_t size;              // Size of the node data
  std::string schema_name;  // Schema name for proper string cleanup
  uint32_t schema_version;  // Schema version for evolution support

  NodeHandle() : ptr(nullptr), size(0), schema_name(""), schema_version(0) {}
  NodeHandle(void* p, size_t s, std::string schema, uint32_t version = 1)
      : ptr(p),
        size(s),
        schema_name(std::move(schema)),
        schema_version(version) {}

  bool is_null() const { return ptr == nullptr; }

  bool operator==(const NodeHandle& other) const {
    return ptr == other.ptr && size == other.size &&
           schema_name == other.schema_name &&
           schema_version == other.schema_version;
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
            std::shared_ptr<LayoutRegistry> layout_registry,
            std::unique_ptr<StringArena> string_arena = nullptr)
      : mem_arena_(std::move(mem_arena)),
        layout_registry_(std::move(layout_registry)),
        string_arena_(string_arena ? std::move(string_arena)
                                   : std::make_unique<StringArena>()) {}

  /**
   * Allocate space for a new node of the given schema type
   * Returns a handle to the allocated node memory
   */
  NodeHandle allocate_node(const std::string& schema_name) {
    const std::shared_ptr<SchemaLayout> layout =
        layout_registry_->get_layout(schema_name);
    if (!layout) {
      return NodeHandle{};  // null handle for unknown schema
    }

    return allocate_node(layout);
  }
  NodeHandle allocate_node(const std::shared_ptr<SchemaLayout>& layout) {
    size_t node_size = layout->get_total_size_with_bitset();
    size_t alignment = layout->get_alignment();

    void* node_data = mem_arena_->allocate(node_size, alignment);
    if (!node_data) {
      return NodeHandle{};  // allocation failed
    }

    // Initialize the node data with default values
    layout->initialize_node_data(static_cast<char*>(node_data));
    return NodeHandle(node_data, node_size, layout->get_schema_name());
  }

  /**
   * Deallocate a node and all its string references
   * Uses schema_name from the NodeHandle for proper cleanup
   */
  void deallocate_node(const NodeHandle& handle) {
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
            const StringRef* str_ref =
                reinterpret_cast<const StringRef*>(field_ptr);

            // Deallocate the string if it's not null
            if (!str_ref->is_null()) {
              string_arena_->deallocate_string(*str_ref);
            }
          }
        }
      }
    }

    // Then deallocate the node memory itself
    mem_arena_->deallocate(handle.ptr);
  }

  /**
   * Get field value from a node using its handle
   */
  const char* get_field_value_ptr(const NodeHandle& handle,
                                  const std::shared_ptr<SchemaLayout>& layout,
                                  const std::string& field_name) const {
    // Logger::get_instance().debug("get_field_value: {}.{}", schema_name,
    //                              field_name);
    if (handle.is_null()) {
      // Logger::get_instance().error("null value for invalid handle");
      return nullptr;  // null value for invalid handle
    }

    return layout->get_field_value_ptr(static_cast<const char*>(handle.ptr),
                                       field_name);
  }

  Value get_field_value(const NodeHandle& handle,
                        const std::shared_ptr<SchemaLayout>& layout,
                        const std::string& field_name) const {
    // Logger::get_instance().debug("get_field_value: {}.{}", schema_name,
    //                              field_name);
    if (handle.is_null()) {
      // Logger::get_instance().error("null value for invalid handle");
      return nullptr;  // null value for invalid handle
    }

    return layout->get_field_value(static_cast<const char*>(handle.ptr),
                                   field_name);
  }

  /**
   * Set field value in a node using its handle
   * Automatically stores strings in the string arena and creates StringRef
   */
  bool set_field_value(const NodeHandle& handle,
                       const std::shared_ptr<SchemaLayout>& layout,
                       const std::string& field_name, const Value& value) {
    // Logger::get_instance().debug("set_field_value: {}.{} = {}", schema_name,
    //                              field_name, value.to_string());
    if (handle.is_null()) {
      return false;  // invalid handle
    }

    // Handle string deallocation for any field that might contain strings
    // Value storage_value = value;
    const FieldLayout* field_layout = layout->get_field_layout(field_name);
    assert(field_layout != nullptr);

    // If the field currently contains a string, deallocate it first
    if (is_string_type(field_layout->type) &&
        is_field_set(static_cast<char*>(handle.ptr), field_layout->index)) {
      Value old_value = layout->get_field_value(static_cast<char*>(handle.ptr),
                                                *field_layout);
      if (!old_value.is_null() && old_value.type() != ValueType::NA) {
        try {
          StringRef old_str_ref = old_value.as_string_ref();
          // Logger::get_instance().debug("deallocate old string: {}",
          //                              old_str_ref.to_string());
          if (!old_str_ref.is_null()) {
            string_arena_->deallocate_string(old_str_ref);
          }
        } catch (...) {
          // Old value wasn't a StringRef, ignore
        }
      }
    }
    // Value storage_value;
    if (value.type() == ValueType::STRING) {
      // Check if it's a temporary std::string that needs to be stored in
      // arena
      const std::string& str_content = value.as_string();
      StringRef str_ref = string_arena_->store_string_auto(str_content);
      // Logger::get_instance().debug("store string: {}",
      // str_ref.to_string());

      return layout->set_field_value(static_cast<char*>(handle.ptr),
                                     *field_layout,
                                     Value{str_ref, field_layout->type});
    } else {
      return layout->set_field_value(static_cast<char*>(handle.ptr),
                                     *field_layout, value);
    }

    // Logger::get_instance().debug("storage_value: {}",
    //                              storage_value.to_string());
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
  std::shared_ptr<LayoutRegistry> layout_registry_;
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
    const std::shared_ptr<LayoutRegistry>& layout_registry,
    size_t initial_size = 2 * 1024 * 1024) {  // 2MB default
  auto mem_arena = std::make_unique<MemoryArena>(initial_size);
  return std::make_unique<NodeArena>(std::move(mem_arena), layout_registry);
}

/**
 * Create a NodeArena using FreeListArena (individual deallocation supported)
 * Creates its own StringArena for string management
 */
inline std::unique_ptr<NodeArena> create_free_list_arena(
    const std::shared_ptr<LayoutRegistry>& layout_registry,
    size_t initial_size = 2 * 1024 * 1024,  // 2MB default
    size_t min_fragment_size = 64) {        // 64 bytes minimum fragment
  auto mem_arena =
      std::make_unique<FreeListArena>(initial_size, min_fragment_size);
  return std::make_unique<NodeArena>(std::move(mem_arena), layout_registry);
}

}  // namespace node_arena_factory

}  // namespace tundradb

#endif  // NODE_ARENA_HPP