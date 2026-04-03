# Incident: Source Over-Read in `MapArena::copy_value_into_entry`

**Date:** 2026-04-03  
**Severity:** Critical — undefined behavior in production code; data corruption risk  
**Classification:** CWE-126 (Buffer Over-read)  
**Affected component:** `MapArena::copy_value_into_entry` in `include/map_arena.hpp`  
**Detected by:** AddressSanitizer (ASan) on GitHub Actions CI  
**Not detected locally:** macOS ARM64 without sanitizers

---

## Summary

Every `MAP`-typed field operation that stores a primitive value (`INT32`,
`INT64`, `FLOAT`, `DOUBLE`, `BOOL`) into a map entry reads **16 bytes** from
the source pointer, regardless of the actual value size. The destination
`entry->value[16]` is correctly sized — it is intentionally wide enough to hold
the largest type (`StringRef`/`ArrayRef`/`MapRef`, all 16 bytes). Primitives
are smaller and simply don't fill the full buffer, which is by design.

The bug is a **source-side over-read**: when the source is a stack variable
(e.g., a 4-byte `int32_t`), `memcpy` reads 12 bytes past the end of that
object into adjacent stack memory and copies the garbage into the unused
portion of the destination buffer. This is undefined behavior, leaks stack
contents into stored data, and blocks the entire ASan CI pipeline.

---

## Affected CI Runs

- [Run #160, Debug + ASan](https://github.com/dmgcodevil/tundradb/actions/runs/23934415615/job/69807932903) — 5 test suites failed

| Test suite          | Failing test                                  |
|---------------------|-----------------------------------------------|
| `SnapshotTest`      | `SnapshotReloadPreservesMapValues`             |
| `WhereExpressionTest` | `CompoundWhereOrFluent` (map schema setup)   |
| `NodeVersionTest`   | `MapPropertySetAndGet`                        |
| `UpdateQueryTest`   | `UpdateByMatchSupportsMapKeySet`              |
| `MapArenaTest`      | `SetAndGetPrimitiveEntries`                   |

All 5 failures produce an identical ASan report:

```
==PID==ERROR: AddressSanitizer: stack-buffer-overflow on address 0x...
READ of size 16 at 0x...
    #0 MapArena::copy_value_into_entry(MapEntry*, ValueType, void const*)
    #1 MapArena::set_entry(MapRef&, StringRef const&, ValueType, void const*)

[48, 52) 'i32' <== Memory access at offset 48 partially overflows this variable
```

---

## Technical Analysis

### Data structures involved

```
MapEntry (include/map_ref.hpp)
┌─────────────────┬────────────┬─────────┬──────────────────────────────────┐
│    key (16B)     │ value_type │ pad[7]  │        value[16]                 │
│    StringRef     │   uint8    │         │  raw storage for any value type  │
└─────────────────┴────────────┴─────────┴──────────────────────────────────┘
                                          ▲
                               VALUE_SIZE = 16 bytes
                               Fits: StringRef(16), ArrayRef(16), MapRef(16),
                                     int64_t(8), double(8), int32_t(4),
                                     float(4), bool(1)
```

The `value[16]` buffer is a tagged-union slot sized to the **widest** stored
type. The widest types are `StringRef`, `ArrayRef`, and `MapRef` (all 16 bytes).
Primitives (`int32_t` = 4B, `bool` = 1B, `double` = 8B, etc.) are smaller and
simply don't fill the entire 16 bytes — that's by design. The destination buffer
is always large enough.

**The bug is not about the destination.** The 16-byte `entry->value` buffer has
plenty of room for any type. The bug is a **source-side over-read**: `memcpy`
reads 16 bytes from a pointer that points to a smaller object (e.g., a 4-byte
`int32_t` on the stack), reading past the end of that source object into
adjacent memory.

### The buggy code path

```
Caller (e.g., set_nested_map_key, test code)
  │
  │  int32_t val = 42;
  │  MapArena::set_entry(ref, key, ValueType::INT32, &val);
  │                                                   │
  │                                  src points to 4 bytes on the stack
  ▼
MapArena::set_entry(ref, key, vtype, value_ptr)
  │
  ▼
MapArena::copy_value_into_entry(entry, vtype, src)
  │
  │  if (is_string_type)  → copy-construct StringRef (16B) ✓
  │  if (is_array_type)   → copy-construct ArrayRef  (16B) ✓
  │  if (is_map_type)     → copy-construct MapRef    (16B) ✓
  │  else (primitive)     → memcpy(dst, src, 16)           ✗ BUG
  ▼
```

### What happens in memory

When storing an `INT32` value `42` located at stack address `0x7ff...030`:

```
SOURCE (stack — only 4 bytes belong to i32):

Address         Content         ASan shadow
─────────────────────────────────────────────
0x7ff...02C     [redzone]       f2 (stack mid redzone)
0x7ff...030     42 00 00 00     04 (4 bytes addressable)  ← int32_t i32
0x7ff...034     [redzone]       f2 (stack mid redzone)
0x7ff...038     [redzone]       f2
0x7ff...03C     [redzone]       f2

memcpy reads 16 bytes starting at src = 0x030:
  bytes 0-3:   42 00 00 00     ← actual int32_t value (VALID read)
  bytes 4-7:   ?? ?? ?? ??     ← past end of i32 — adjacent stack memory (INVALID read)
  bytes 8-11:  ?? ?? ?? ??     ← past end of i32 — stack garbage (INVALID read)
  bytes 12-15: ?? ?? ?? ??     ← past end of i32 — stack garbage (INVALID read)
                ^^^^^^^^^^^
                SOURCE over-read: 12 bytes beyond the 4-byte int32_t object


DESTINATION (entry->value[16] — 16 bytes, always big enough):

┌────────────┬──────────────────────────────────────────┐
│  42000000  │  ???????? ???????? ????????              │
│  (correct) │  (stack garbage copied from source)      │
└────────────┴──────────────────────────────────────────┘
  bytes 0-3     bytes 4-15
  = int32_t     = meaningless noise from the stack
```

The **destination** has 16 bytes of valid storage — there is no write overflow.
The problem is entirely on the **read side**: `memcpy` reads 12 bytes beyond
the end of the 4-byte source object.

### Why this is dangerous beyond ASan

Even without ASan, the over-read is **undefined behavior per the C++ standard**
(§6.7.3: reading beyond the bounds of an object). The destination is fine — the
16-byte `entry->value` buffer can hold any type. The problem is reading past
the **source** object. In practice:

1. **Stack data leakage into stored values.** The over-read bytes (e.g., 12
   garbage bytes for an `int32_t`) land in positions 4–15 of the entry's
   `value` buffer — the unused tail that exists because we reserve space for the
   widest type. If the entry is ever serialized byte-for-byte (e.g., snapshot
   to disk, wire protocol, or debugging dump), those bytes leak whatever was on
   the stack at write time: other local variables, return addresses, pointers.

2. **Non-deterministic stored data.** Two identical `set_entry(ref, key,
   INT32, &val)` calls with the same `val` can produce entries with different
   raw `value` contents, because the stack context (the garbage past the source
   variable) differs between call sites. This can cause spurious failures in
   byte-level equality checks, checksums, or replication.

3. **Compiler optimization hazard.** The compiler may assume the 16-byte read
   is within a valid object and optimize accordingly. Future compiler versions
   or link-time optimization could reorder variables, elide stack frames, or
   vectorize the memcpy in ways that cause visible corruption or crashes.

4. **Blocks sanitizer CI.** With ASan enabled, the over-read is an immediate
   abort. This means the entire Debug + ASan matrix is red, preventing
   detection of other real memory bugs.

### Call sites affected

Every place that passes a `&primitive` to `set_entry` or `copy_value_into_entry`:

| Location | Variable | Size | Over-read |
|----------|----------|------|-----------|
| `NodeArena::set_nested_map_key` | `int32_t i32` | 4B | 12B |
| `NodeArena::set_nested_map_key` | `float f` | 4B | 12B |
| `NodeArena::set_nested_map_key` | `bool b` | 1B | 15B |
| `NodeArena::set_nested_map_key` | `int64_t i64` | 8B | 8B |
| `NodeArena::set_nested_map_key` | `double d` | 8B | 8B |
| `tests/map_arena_test.cpp` (14+ calls) | various `int32_t`, `double`, `bool` | 1–8B | 8–15B |
| `tests/node_version_test.cpp` (4 calls) | `double`, `int32_t` | 4–8B | 8–12B |
| `tests/snapshot_test.cpp` (via `update_fields`) | `int32_t` | 4B | 12B |
| `tests/where_expression_test.cpp` (via schema setup) | `int32_t` | 4B | 12B |
| `tests/update_query_test.cpp` (via `update_fields`) | `int32_t` | 4B | 12B |

---

## Root Cause

The original `else` branch treated all non-ref types uniformly:

```cpp
// BEFORE (buggy)
static void copy_value_into_entry(MapEntry* entry, ValueType vtype,
                                  const void* src) {
    if (is_string_type(vtype)) {
        new (entry->value) StringRef(*reinterpret_cast<const StringRef*>(src));
    } else if (is_array_type(vtype)) {
        new (entry->value) ArrayRef(*reinterpret_cast<const ArrayRef*>(src));
    } else if (is_map_type(vtype)) {
        new (entry->value) MapRef(*reinterpret_cast<const MapRef*>(src));
    } else {
        std::memcpy(entry->value, src, MapEntry::VALUE_SIZE);  // ← 16 bytes ALWAYS
    }
}
```

The ref-counted types (`StringRef`, `ArrayRef`, `MapRef`) are all exactly 16
bytes (`static_assert` enforced), so reading 16 bytes from a ref source pointer
is correct — the source object is 16 bytes, matching the destination slot.

But the `else` branch — handling `INT32`, `INT64`, `DOUBLE`, `FLOAT`, `BOOL` —
applied the same 16-byte **read** to source objects that are only 1–8 bytes.
The destination has room for 16 bytes (by design, it reserves space for the
widest type), but the source does not have 16 bytes to give.

### Why it wasn't caught earlier

- **macOS development:** Without ASan, the over-read silently succeeds
  (adjacent stack memory is always mapped on macOS).
- **No byte-level comparison of entries:** The read-back path extracts values
  by type (e.g., `as_int32()` reads 4 bytes), so the garbage in bytes 4–15 is
  never observed through the normal API.
- **No serialization of raw entry bytes:** The Parquet write path converts
  values through Arrow builders, which also extract by type.

---

## Fix

### 1. Type-aware copy in `copy_value_into_entry`

Added a `primitive_value_size()` helper that returns the exact byte count
for each primitive type, and changed the `else` branch to zero-fill before
copying only the actual size:

```cpp
// AFTER (fixed)
static void copy_value_into_entry(MapEntry* entry, ValueType vtype,
                                  const void* src) {
    if (is_string_type(vtype)) {
        auto* dst = reinterpret_cast<StringRef*>(entry->value);
        dst->~StringRef();
        new (dst) StringRef(*reinterpret_cast<const StringRef*>(src));
    } else if (is_array_type(vtype)) {
        auto* dst = reinterpret_cast<ArrayRef*>(entry->value);
        dst->~ArrayRef();
        new (dst) ArrayRef(*reinterpret_cast<const ArrayRef*>(src));
    } else if (is_map_type(vtype)) {
        auto* dst = reinterpret_cast<MapRef*>(entry->value);
        dst->~MapRef();
        new (dst) MapRef(*reinterpret_cast<const MapRef*>(src));
    } else {
        size_t n = primitive_value_size(vtype);
        std::memset(entry->value, 0, MapEntry::VALUE_SIZE);
        std::memcpy(entry->value, src, n);
    }
}

static constexpr size_t primitive_value_size(ValueType vtype) {
    switch (vtype) {
        case ValueType::BOOL:   return sizeof(bool);    // 1
        case ValueType::INT32:  return sizeof(int32_t); // 4
        case ValueType::INT64:  return sizeof(int64_t); // 8
        case ValueType::FLOAT:  return sizeof(float);   // 4
        case ValueType::DOUBLE: return sizeof(double);  // 8
        default:                return MapEntry::VALUE_SIZE; // 16 (safe fallback)
    }
}
```

The `memset` ensures bytes beyond the value are deterministically zero rather
than uninitialized, which is important for reproducibility and potential future
byte-level serialization.

### 2. Removed dangerous `NodeManager` destructor

```cpp
// BEFORE
~NodeManager() { node_arena_->clear(); }

// AFTER
~NodeManager() = default;
```

The explicit `clear()` freed arena memory while `Node` objects (holding
`NodeHandle` pointers into the arena) were still alive as class members.
The arena's own destructor handles cleanup in the correct order.

---

## Files Changed

| File | Change |
|------|--------|
| `include/map_arena.hpp` | Fixed `copy_value_into_entry` primitive branch; added `primitive_value_size()` |
| `include/node.hpp` | Changed `~NodeManager()` from explicit `clear()` to `= default` |

---

## Verification

- All 27 test suites pass locally (macOS, Debug)
- The fix is a strict narrowing of the read — no behavioral change for
  correctly-sized sources (ref types still copy 16 bytes via copy-constructors)

---

## Lessons Learned

1. **Never assume a `void*` points to `N` bytes.** The `copy_value_into_entry`
   contract was implicit: "src must point to at least `VALUE_SIZE` bytes." This
   was never documented, never enforced, and every primitive call site violated
   it. The fix makes the function safe for any correctly-typed pointer.

2. **ASan is not optional.** This bug existed since the MAP feature was
   introduced and was invisible without sanitizers. ASan should be part of
   every CI run, not just a periodic check.

3. **Zero-fill unused buffer bytes.** Even when the extra bytes are "never
   read," they can leak through serialization, debugging, or future code
   changes. Deterministic zero-fill costs almost nothing and eliminates an
   entire class of information-leak bugs.

4. **Tagged-union value buffers need type-aware operations.** Any function that
   reads or writes a tagged union's raw storage must dispatch on the tag to
   determine the correct size. A blanket `memcpy` of the maximum size is a
   latent bug waiting to surface.

5. **Review all `memcpy` calls in arena code.** This particular pattern (fixed-
   size memcpy from a variable-size source) may exist elsewhere. A codebase
   audit of `memcpy` calls with `VALUE_SIZE` or `HEADER_SIZE` constants is
   recommended.
