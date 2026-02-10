# NodeView Design - Complete Implementation

## Overview

This document describes the complete `NodeView` design for time-travel queries in TundraDB.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Query Execution                           │
├──────────────────────────────────────────────────────────────────┤
│  1. Create TemporalContext with snapshot (valid_time, tx_time)   │
│  2. For each node, call node.view(&ctx)                          │
│  3. NodeView resolves version once (cached in TemporalContext)   │
│  4. Read fields from NodeView (no per-field lookups)             │
└──────────────────────────────────────────────────────────────────┘
```

## Components

### 1. TemporalSnapshot (temporal_context.hpp)

Represents a point in bitemporal space:

```cpp
struct TemporalSnapshot {
  uint64_t valid_time;  // VALIDTIME: when fact was true in domain
  uint64_t tx_time;     // TXNTIME: when DB knew about the fact

  // UINT64_MAX means "current time"
  static TemporalSnapshot as_of_valid(uint64_t vt);  // VALIDTIME only
  static TemporalSnapshot as_of_tx(uint64_t tt);     // TXNTIME only
  bool is_current() const;  // Both times = UINT64_MAX
};
```

**Usage:**
```cpp
// Time-travel to specific VALIDTIME
auto snapshot = TemporalSnapshot::as_of_valid(1704067200000);

// Bitemporal: specific valid & tx times
auto snapshot = TemporalSnapshot(1704067200000, 1706745600000);
```

---

### 2. TemporalContext (temporal_context.hpp)

Per-query state for temporal queries:

```cpp
class TemporalContext {
  TemporalSnapshot snapshot_;
  std::unordered_map<int64_t, VersionInfo*> version_cache_;  // node_id -> version

public:
  explicit TemporalContext(TemporalSnapshot snapshot);
  
  // Resolve visible version for a node (with caching)
  VersionInfo* resolve_version(int64_t node_id, const NodeHandle& handle);
  
  void clear_cache();
};
```

**Key Features:**
- **Version resolution**: Finds the visible version at the snapshot
- **Caching**: Avoids repeated version chain traversals
- **Bitemporal visibility**: Checks both VALIDTIME and TXNTIME

**Visibility Rule:**
```cpp
A version is visible if:
  (version.valid_from <= snapshot.valid_time < version.valid_to)
  AND
  (version.tx_from <= snapshot.tx_time < version.tx_to)
```

---

### 3. NodeView (node_view.hpp)

Lightweight view of a Node at a specific temporal snapshot:

```cpp
class NodeView {
  Node* node_;
  VersionInfo* resolved_version_;  // Resolved once at construction
  NodeArena* arena_;
  SchemaLayout* layout_;

public:
  NodeView(Node* node, VersionInfo* resolved_version, 
           NodeArena* arena, SchemaLayout* layout);
  
  // Same interface as Node
  arrow::Result<const char*> get_value_ptr(const Field& field) const;
  arrow::Result<ValueRef> get_value_ref(const Field& field) const;
  arrow::Result<Value> get_value(const Field& field) const;
  
  bool is_visible() const;  // false if node didn't exist at snapshot
  const VersionInfo* get_resolved_version() const;
  Node* get_node() const;
};
```

**Key Features:**
- **Lightweight**: Only 24 bytes (3 pointers)
- **No per-field lookups**: Version resolved once at construction
- **Same interface as Node**: Drop-in replacement for field access
- **Handles non-existence**: `is_visible() == false` if node didn't exist at snapshot

---

### 4. Node::view() Method (node.hpp)

Factory method on Node to create NodeView:

```cpp
class Node {
public:
  NodeView view(TemporalContext* ctx = nullptr) {
    if (!ctx || !handle_) {
      // No temporal context -> use current version
      return NodeView(this, handle_->version_info_, arena_.get(), layout_.get());
    }

    // Resolve version using TemporalContext (cached)
    VersionInfo* resolved = ctx->resolve_version(id, *handle_);
    return NodeView(this, resolved, arena_.get(), layout_.get());
  }
};
```

---

### 5. VersionInfo Extension (node_arena.hpp)

Extended with TXNTIME support:

```cpp
struct VersionInfo {
  uint64_t version_id;
  
  // VALIDTIME (domain)
  uint64_t valid_from;
  uint64_t valid_to;  // UINT64_MAX = INF
  
  // TXNTIME (system knowledge) - NEW
  uint64_t tx_from;
  uint64_t tx_to;     // UINT64_MAX = INF
  
  VersionInfo* prev;
  llvm::SmallDenseMap<uint16_t, char*> updated_fields;
  
  // Bitemporal visibility check
  bool is_visible_at(uint64_t valid_time, uint64_t tx_time) const;
  
  // Find version visible at snapshot
  const VersionInfo* find_version_at_snapshot(uint64_t vt, uint64_t tt) const;
};
```

**Constructor:**
```cpp
VersionInfo(uint64_t vid, uint64_t ts_from, VersionInfo* prev_ver = nullptr)
    : version_id(vid), 
      valid_from(ts_from), 
      tx_from(ts_from),  // Initially tx_from = valid_from
      prev(prev_ver) {}
```

---

### 6. NodeArena Helper Methods (node_arena.hpp)

Added methods for NodeView to resolve fields:

```cpp
class NodeArena {
public:
  // Get field value pointer starting from a specific version
  const char* get_field_value_ptr_from_version(
      const VersionInfo* version,
      const SchemaLayout& layout,
      const Field& field) const;
  
  // Get field value starting from a specific version
  arrow::Result<Value> get_field_value_from_version(
      const VersionInfo* version,
      const SchemaLayout& layout,
      const Field& field) const;
};
```

---

## Usage Examples

### Example 1: Current Version (No Time-Travel)

```cpp
Node node = ...;

// Create view without temporal context (current version)
NodeView view = node.view(nullptr);

// Read fields
auto age = view.get_value(age_field);
auto dept = view.get_value(department_field);
```

---

### Example 2: Time-Travel Query (VALIDTIME)

```cpp
// Scenario: Node has 3 versions at times t0, t1, t2
// v0 (t0): age=25, dept="Engineering"
// v1 (t1): age=26, dept="Engineering"
// v2 (t2): age=26, dept="Sales"

// Query at t1 (between v1 and v2)
TemporalContext ctx(TemporalSnapshot::as_of_valid(t1));
NodeView view = node.view(&ctx);

auto age = view.get_value(age_field);    // Returns 26 (from v1)
auto dept = view.get_value(dept_field);  // Returns "Engineering" (from v1)
```

---

### Example 3: Bitemporal Query (VALIDTIME + TXNTIME)

```cpp
// Query what the DB knew at tx_time=100 about valid_time=90
TemporalContext ctx(TemporalSnapshot(90, 100));
NodeView view = node.view(&ctx);

// Returns values from the version visible at (vt=90, tt=100)
auto age = view.get_value(age_field);
```

---

### Example 4: Multiple Field Reads (No Repeated Lookups)

```cpp
TemporalContext ctx(TemporalSnapshot::as_of_valid(timestamp));
NodeView view = node.view(&ctx);  // Version resolved once here

// All subsequent field reads use the cached resolved version
auto name = view.get_value(name_field);
auto age = view.get_value(age_field);
auto dept = view.get_value(department_field);
// No map lookups, no repeated version chain traversals!
```

---

### Example 5: Node Doesn't Exist at Snapshot

```cpp
uint64_t before_creation = node_creation_time - 100000;

TemporalContext ctx(TemporalSnapshot::as_of_valid(before_creation));
NodeView view = node.view(&ctx);

if (!view.is_visible()) {
  // Node didn't exist at this time
  std::cout << "Node not visible\n";
}

// Accessing fields returns KeyError
auto age_result = view.get_value(age_field);
assert(!age_result.ok() && age_result.status().IsKeyError());
```

---

## Performance Characteristics

### Memory

| Component | Size | Notes |
|-----------|------|-------|
| `TemporalSnapshot` | 16 bytes | 2 × uint64_t |
| `TemporalContext` | 40+ bytes | Snapshot + version cache map |
| `NodeView` | 24 bytes | 3 pointers |
| `VersionInfo` extension | +16 bytes | Added tx_from/tx_to |

### Time Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| `TemporalContext::resolve_version` | O(N) first time, O(1) cached | N = version chain length |
| `NodeView` construction | O(1) | Just pointer assignment |
| `NodeView::get_value` | O(M) | M = field chain depth (usually 1-2) |

**Key Optimization**: Version resolution happens **once per node per query**, not per field access.

---

## Benefits

1. **No Per-Field Lookups**: Version resolved once, cached in `TemporalContext`
2. **Unified Interface**: `NodeView` has same API as `Node` (drop-in replacement)
3. **Bitemporal Support**: Ready for VALIDTIME + TXNTIME queries
4. **Efficient**: Minimal overhead (24 bytes per view)
5. **Handles Non-Existence**: Gracefully handles nodes that didn't exist at snapshot
6. **Thread-Safe Reads**: Multiple queries can use separate `TemporalContext` instances

---

## Tests

See `tests/node_view_test.cpp` for comprehensive tests:

- ✅ `CurrentVersionView`: No temporal context (current version)
- ✅ `TimeTravelValidTime`: Query at different points in VALIDTIME
- ✅ `TemporalContextCaching`: Version resolution caching
- ✅ `NodeNotVisibleAtSnapshot`: Node didn't exist yet
- ✅ `MultipleFieldReadsFromSameView`: No repeated lookups

Run tests:
```bash
cd build && ctest -R NodeViewTest -V
```

---

## Next Steps

### Phase 2: Query Syntax (Pending)

Add temporal query syntax to TundraQL grammar:

```sql
-- Time-travel query
AS OF VALIDTIME 1704067200000
AS OF TXNTIME   1706745600000
MATCH (u:User) WHERE u.name = "Alice"
SELECT u.department;

-- Retroactive correction
MATCH (u:User) WHERE u.name = "Alice"
FOR VALIDTIME [1704067200000, INF)
SET u.department = "Sales";
```

### Phase 3: Interval Surgery (Pending)

Implement retroactive corrections with interval splitting/coalescing.

### Phase 4: DIFF Operator (Pending)

Implement audit trail queries:

```sql
DIFF FROM (VALIDTIME 1704067200000, TXNTIME 1706745600000)
     TO   (VALIDTIME 1706745600000, TXNTIME now)
MATCH (u:User) WHERE u.name = "Alice";
```

---

## Summary

The NodeView design provides:
- ✅ Efficient time-travel queries (version resolved once per node)
- ✅ Bitemporal support (VALIDTIME + TXNTIME)
- ✅ Clean API (same as Node, drop-in replacement)
- ✅ Comprehensive tests
- ✅ Ready for query syntax integration

**Current Status**: Phase 1 Complete (Infrastructure + Tests) ✅

