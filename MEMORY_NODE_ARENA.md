# TundraDB Memory Node Arena System

## Overview

The **Memory Node Arena System** is TundraDB's high-performance memory management solution designed specifically for gaming workloads and real-time data processing. It provides fast, cache-friendly node allocation with automatic memory layout optimization and string management.

## Key Benefits

- **🚀 Fast Allocation**: Bulk memory allocation with minimal system calls
- **🔧 Zero Fragmentation**: Arena-based allocation eliminates memory fragmentation
- **📊 Cache Efficiency**: Contiguous memory layout optimizes CPU cache usage
- **🎯 Type Safety**: Schema-based layouts ensure data integrity
- **🧵 String Optimization**: Automatic string pooling and deduplication
- **⚡ Gaming Optimized**: Designed for low-latency, high-throughput gaming scenarios

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    NodeArena System                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   NodeArena     │    │        StringArena              │ │
│  │                 │    │  ┌─────────────────────────────┐ │ │
│  │ ┌─────────────┐ │    │  │    String Pools             │ │ │
│  │ │ MemArena    │ │    │  │ ┌─────┐ ┌─────┐ ┌─────────┐ │ │ │
│  │ │             │ │    │  │ │≤16  │ │≤32  │ │Unlimited│ │ │ │
│  │ │ - Memory    │ │    │  │ │chars│ │chars│ │  chars  │ │ │ │
│  │ │ - FreeList  │ │    │  │ └─────┘ └─────┘ └─────────┘ │ │ │
│  │ └─────────────┘ │    │  └─────────────────────────────┘ │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. NodeArena - The Main Interface

The `NodeArena` is the primary interface for node memory management. It coordinates between:
- **Memory allocation** for fixed-size node data
- **String management** for variable-size string content
- **Schema layout** enforcement for type safety

```cpp
class NodeArena {
public:
    // Allocate a new node of the specified schema
    NodeHandle allocate_node(const std::string& schema_name);
    
    // Deallocate node and all its string references
    void deallocate_node(const NodeHandle& handle);
    
    // Field operations using schema layout
    bool set_field_value(const NodeHandle& handle,
                        const std::shared_ptr<SchemaLayout>& layout,
                        const std::string& field_name, 
                        const Value& value);
    
    Value get_field_value(const NodeHandle& handle,
                         const std::shared_ptr<SchemaLayout>& layout,
                         const std::string& field_name) const;
};
```

### 2. NodeHandle - Lightweight Node Reference

A `NodeHandle` is a lightweight reference to allocated node memory:

```cpp
struct NodeHandle {
    void* ptr;                // Direct pointer to node data
    size_t size;              // Size of the node data
    std::string schema_name;  // Schema name for proper cleanup
    uint32_t schema_version;  // Schema version for evolution support
    
    bool is_null() const { return ptr == nullptr; }
};
```

### 3. Memory Arena Implementations

#### MemoryArena - Fast Bulk Allocation
- **Use Case**: High-throughput scenarios where nodes are allocated in bulk
- **Allocation**: O(1) bump pointer allocation
- **Deallocation**: Reset/clear entire arena only
- **Memory Pattern**: Contiguous allocation, no fragmentation

```cpp
class MemoryArena : public MemArena {
    // Fast bump-pointer allocation
    void* allocate(size_t size, size_t alignment = 8) override;
    
    // Reset arena for reuse (keeps chunks allocated)
    void reset() override;
    
    // Clear all memory
    void clear() override;
};
```

#### FreeListArena - Individual Deallocation
- **Use Case**: Dynamic scenarios requiring individual node deallocation
- **Allocation**: O(1) free list allocation with size classes
- **Deallocation**: O(1) individual node deallocation
- **Memory Pattern**: Efficient reuse of deallocated blocks

```cpp
class FreeListArena : public MemArena {
    // Allocate with individual deallocation support
    void* allocate(size_t size, size_t alignment = 8) override;
    
    // Deallocate individual blocks
    void deallocate(void* ptr) override;
};
```

### 4. Schema Layout System

#### SchemaLayout - Memory Layout Definition

The `SchemaLayout` defines how fields are arranged in memory:

```cpp
class SchemaLayout {
    // Memory layout:
    // [Bitset][Padding][Field1][Field2][Field3]...
    //    ^        ^        ^
    //    |        |        └─ Data fields (aligned)
    //    |        └─ Alignment padding
    //    └─ Null/set flags for each field
    
    void add_field(const std::string& name, ValueType type, bool nullable = true);
    void finalize(); // Must call after adding all fields
    
    // Field access
    Value get_field_value(const char* node_data, const std::string& field_name) const;
    bool set_field_value(char* node_data, const std::string& field_name, const Value& value);
};
```

#### Field Layout Optimization

Fields are automatically arranged for optimal memory usage:

1. **Alignment**: Fields aligned to their natural boundaries
2. **Packing**: Minimal padding between fields
3. **Bitset**: Efficient null/set tracking (1 bit per field)
4. **StringRef**: All strings stored as 12-byte references

**Example Layout:**
```
User Schema: id(int64), name(string), age(int32), active(bool)

Memory Layout (40 bytes total):
┌─────┬─────┬─────────┬──────────────┬─────┬────┬─────┐
│Bits │ Pad │   id    │     name     │ age │act │ Pad │
│ 1   │  7  │    8    │      12      │  4  │ 1  │  7  │
└─────┴─────┴─────────┴──────────────┴─────┴────┴─────┘
```

### 5. String Management System

#### StringArena - Multi-Pool String Management

The `StringArena` manages strings of different sizes using specialized pools:

```cpp
class StringArena {
    // Pools for different string sizes
    std::unordered_map<ValueType, std::unique_ptr<StringPool>> pools_;
    
    // Pool Types:
    // - FIXED_STRING16: ≤16 characters
    // - FIXED_STRING32: ≤32 characters  
    // - FIXED_STRING64: ≤64 characters
    // - STRING: Unlimited size
};
```

#### StringPool - Size-Specific String Storage

Each `StringPool` handles strings of a specific maximum size:

```cpp
class StringPool {
    // Features:
    // - Individual string deallocation
    // - Optional deduplication with reference counting
    // - FreeListArena backing for efficiency
    
    StringRef store_string(const std::string& str, uint32_t pool_id = 0);
    void deallocate_string(const StringRef& ref);
    void enable_deduplication(bool enable = true);
};
```

#### StringRef - Efficient String Reference

All strings are stored as `StringRef` objects (12 bytes):

```cpp
struct StringRef {
    const char* data;     // Pointer to string data
    uint32_t length;      // String length
    uint32_t arena_id;    // Pool identifier
    
    std::string_view to_string_view() const;
    bool is_null() const { return data == nullptr; }
};
```

## Usage Examples

### Basic Node Operations

```cpp
// 1. Set up the system
auto layout_registry = std::make_shared<LayoutRegistry>();
auto node_arena = node_arena_factory::create_free_list_arena(layout_registry);

// 2. Define schema
auto user_layout = std::make_unique<SchemaLayout>("User");
user_layout->add_field("id", ValueType::INT64);
user_layout->add_field("name", ValueType::FIXED_STRING32);
user_layout->add_field("age", ValueType::INT32);
user_layout->add_field("active", ValueType::BOOL);
user_layout->finalize();

auto layout = user_layout.get();
layout_registry->register_layout(std::move(user_layout));

// 3. Allocate and use nodes
NodeHandle user_node = node_arena->allocate_node("User");

// Set field values
node_arena->set_field_value(user_node, layout, "id", Value{12345L});
node_arena->set_field_value(user_node, layout, "name", Value{"John Doe"});
node_arena->set_field_value(user_node, layout, "age", Value{30});
node_arena->set_field_value(user_node, layout, "active", Value{true});

// Get field values
Value name = node_arena->get_field_value(user_node, layout, "name");
std::cout << "User name: " << name.to_string() << std::endl;

// Clean up
node_arena->deallocate_node(user_node);
```

### Factory Functions

The system provides convenient factory functions:

```cpp
// Create NodeArena with MemoryArena (bulk allocation)
auto memory_arena = node_arena_factory::create_memory_arena(layout_registry);

// Create NodeArena with FreeListArena (individual deallocation)
auto freelist_arena = node_arena_factory::create_free_list_arena(layout_registry);

// Create with custom string arena
auto string_arena = std::make_unique<StringArena>();
string_arena->enable_deduplication(true);
auto custom_arena = node_arena_factory::create_with_string_arena(
    layout_registry, std::move(string_arena));
```

## Performance Characteristics

### Memory Allocation

| Operation | MemoryArena | FreeListArena | std::malloc |
|-----------|-------------|---------------|-------------|
| Allocate  | O(1)        | O(1)          | O(log n)    |
| Deallocate| N/A         | O(1)          | O(log n)    |
| Fragmentation | None    | Minimal       | High        |

### String Management

| String Size | Pool Used | Deduplication | Allocation |
|-------------|-----------|---------------|------------|
| ≤16 chars   | FIXED_STRING16 | Optional | O(1) |
| ≤32 chars   | FIXED_STRING32 | Optional | O(1) |
| ≤64 chars   | FIXED_STRING64 | Optional | O(1) |
| >64 chars   | STRING         | Optional | O(1) |

### Cache Efficiency

- **Node Data**: Contiguous allocation improves cache locality
- **Field Access**: Direct memory access without indirection
- **String Pools**: Size-based pooling reduces cache misses
- **Schema Layout**: Optimal field alignment minimizes padding

## Gaming Workload Optimizations

### Real-Time Requirements
- **Predictable Allocation**: O(1) allocation times
- **No GC Pauses**: Manual memory management
- **Low Latency**: Direct memory access patterns

### High Throughput
- **Bulk Operations**: Arena-based allocation
- **String Pooling**: Reduces allocation overhead
- **Cache Optimization**: Contiguous memory layout

### Memory Efficiency
- **Zero Fragmentation**: Arena allocation
- **String Deduplication**: Optional reference counting
- **Optimal Packing**: Automatic field alignment

## Best Practices

### Schema Design
1. **Order fields by size** (largest to smallest) to minimize padding
2. **Use appropriate string types** (FIXED_STRING16/32/64 vs STRING)
3. **Finalize schemas** before use for optimal layout
4. **Consider nullable fields** carefully for memory efficiency

### Memory Management
1. **Choose the right arena type**:
   - `MemoryArena` for bulk allocation scenarios
   - `FreeListArena` for dynamic allocation/deallocation
2. **Use string deduplication** for repeated string values
3. **Reset arenas** instead of clearing when possible
4. **Monitor memory statistics** for optimization opportunities

### Performance Tips
1. **Batch operations** when possible
2. **Reuse NodeHandles** to avoid repeated lookups
3. **Profile string pool usage** to optimize pool sizes
4. **Use schema caching** in hot paths

## Monitoring and Statistics

The system provides comprehensive statistics for monitoring:

```cpp
// Arena statistics
size_t total_allocated = node_arena->get_total_allocated();
size_t used_bytes = node_arena->get_used_bytes();
size_t chunk_count = node_arena->get_chunk_count();

// String arena statistics
StringArena* string_arena = node_arena->get_string_arena();
string_arena->print_statistics();

// Per-pool statistics
StringPool* pool16 = string_arena->get_pool(ValueType::FIXED_STRING16);
size_t pool_allocated = pool16->get_total_allocated();
size_t string_count = pool16->get_string_count();
```

## Thread Safety

⚠️ **Important**: The current implementation is **not thread-safe**. For multi-threaded usage:

1. **Use external synchronization** (mutexes, etc.)
2. **Create separate arenas per thread** for lock-free operation
3. **Consider thread-local storage** for high-performance scenarios

## Schema Evolution Support

The system includes basic support for schema evolution:

- **Version tracking** in NodeHandle
- **Backward compatibility** through careful field addition
- **Migration support** (planned feature)

## Integration with TundraDB

The Memory Node Arena integrates seamlessly with TundraDB's other systems:

- **Query Engine**: Direct field access for filtering
- **Storage Layer**: Efficient serialization/deserialization
- **Index System**: Cache-friendly data layout
- **Arrow Integration**: Compatible with Arrow memory format

## Future Enhancements

- **Thread-safe implementation** with lock-free data structures
- **NUMA awareness** for multi-socket systems
- **Compression support** for string pools
- **Advanced schema migration** tools
- **Memory-mapped file support** for persistence
- **Custom allocators** for specialized workloads

---

*This documentation covers TundraDB's Memory Node Arena System v1.0. For implementation details, see the source code in `include/node_arena.hpp` and related files.*