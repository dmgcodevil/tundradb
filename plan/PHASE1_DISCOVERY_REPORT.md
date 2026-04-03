# Phase 1: Baseline and Discovery — Report

**Date:** 2026-04-03

---

## Overview

Scanned all `include/*.hpp` (44 files) and `src/*.cpp` (19 files) for:
duplicate logic, dead code, large/complex methods, memory-sensitive patterns,
and magic numbers. Coverage script exists at `scripts/run_tests_coverage.sh`
(baseline run deferred to Phase 5 validation).

---

## 1. Duplicate Logic Candidates (16 findings)

| # | What | Where | Duplicates | Action |
|---|------|-------|------------|--------|
| 1 | `decode_map_item_union` | `src/query_execution.cpp:98–147` | `map_item_to_value` in `src/arrow_utils.cpp:218–267` | Delete, call `map_item_to_value` |
| 2 | `value_type_to_arrow` (anon ns) | `src/edge_store.cpp:352–371` | `scalar_vt_to_arrow` in `include/metadata.hpp:198–217` | Delete, use `scalar_vt_to_arrow` |
| 3 | Array element type switch | `src/schema.cpp:90–118` | `scalar_vt_to_arrow` | Replace inner switch |
| 4 | Arrow→string (shell) | `src/tundra_shell.cpp:1805–1854` | `stringify_arrow_scalar` in `src/utils.cpp:10–61` | Unify into one function |
| 5 | `print_row` | `include/utils.hpp:453–494` | `stringify_arrow_scalar` | Factor shared `format_arrow_cell` |
| 6 | Builder-from-type (3 copies) | `utils.hpp:237–274`, `arrow_utils.cpp:585–627`, `edge_store.cpp:407–426` | Each other | Single `make_array_builder_for_type` |
| 7 | Raw→builder append (2 copies) | `utils.hpp:300–368`, `edge_store.cpp:380–404` | `value_ptr_to_arrow_scalar` in `arrow_utils.cpp:194–215` | Shared `append_raw_to_builder` |
| 8 | `decode_cell_value` scalars | `src/storage.cpp:207–239` | `array_element_to_value` in `arrow_utils.cpp:270–297` | Call `array_element_to_value` for scalars |
| 9 | Binary MAP sizeof usage | `src/storage.cpp:74–106` | `get_type_size` in `value_type.hpp` | Use `get_type_size` |
| 10 | `Value::to_string` vs `ValueRef::ToString` | `include/types.hpp` (two ~65-line blocks) | Each other | Single `format_value_for_display` |
| 11 | `ValueRef::as_scalar` | `include/types.hpp:331–355` | `value_ptr_to_arrow_scalar` in `arrow_utils.cpp:194–215` | Implement one via the other |
| 12 | `value_to_arrow_scalar` missing FLOAT | `src/arrow_utils.cpp:172–191` | `ValueRef::as_scalar` (also missing FLOAT) | Unify, add FLOAT |
| 13 | Two `apply_comparison` overloads | `src/query.cpp:165–201, 220–257` | Each other | One overload calling the other |
| 14 | `extract_map_values_for_key` builder loop | `src/query_execution.cpp:223–297` | `append_value_to_builder` pattern | Shared `values_to_arrow_array` |
| 15 | Repeated `ValueType` switches (6+ files) | types.hpp, schema_layout.hpp, node_arena.hpp, row.hpp, core.cpp, query.cpp | Each other | Consider centralized dispatch table |
| 16 | Arrow type ID switches (4 files) | metadata.hpp, schema.cpp, storage.cpp, utils.hpp | Each other | Single `arrow_type_to_value_type` |

---

## 2. Dead Code Candidates (18 findings)

### High confidence — no callers in src/

| # | Symbol | File | Notes |
|---|--------|------|-------|
| 1 | `is_aligned` | `include/mem_utils.hpp` | Zero references |
| 2 | `get_column_as_array` | `include/utils.hpp` | Zero references |
| 3 | `get_first_value_from_array`, `get_first_int64`, `get_first_string` | `include/utils.hpp` | Zero references |
| 4 | `filter_nodes_by_where` | `include/utils.hpp` | Zero references |
| 5 | `ValueOrDieWithContext` / `VALUE_OR_DIE_CTX` | `include/utils.hpp` | Zero references |
| 6 | `value_to_arrow_scalar` | `arrow_utils.hpp/cpp` | Zero callers |
| 7 | `create_int64_array`, `create_str_array`, `create_null_array` | `arrow_utils.hpp/cpp` | Zero callers |
| 8 | `Node::update_fields` (pair overload) | `include/node.hpp` | Legacy, zero callers |
| 9 | `write_to_file`, `read_from_file`, `file_exists` | `file_utils.hpp/cpp` | Zero callers |
| 10 | `container_concepts.hpp` | entire header | Zero includes |
| 11 | `schema_utils.hpp` | entire header | Only included from test |
| 12 | `src/main.cpp` | orphan file | Not in CMakeLists.txt |
| 13 | `src/utils.cpp` (`stringify_arrow_scalar`) | entire TU | Zero callers (shell reimplements) |

### Medium confidence — test-only or deprecated

| # | Symbol | File | Notes |
|---|--------|------|-------|
| 14 | `MockClock::advance_millis` | `include/clock.hpp` | No callers |
| 15 | `print_row`, `print_table` | `include/utils.hpp` | Test-only |
| 16 | `Node::get_value(string)`, `update(string,...)`, `set_value(string,...)` | `include/node.hpp` | `[[deprecated]]`, test-only |
| 17 | `prepend_id_field` `[[deprecated]]` | `arrow_utils.hpp` | Still called from schema.cpp |
| 18 | `SchemaRegistry::get_arrow` `[[deprecated]]` | `schema.hpp/cpp` | Still called from snapshot_manager |

---

## 3. Large/Complex Methods (17 findings)

| # | Function | File | Lines | Why complex |
|---|----------|------|-------|-------------|
| 1 | `Database::query` | `src/core.cpp:524–994` | ~471 | Orchestrates entire query pipeline; 4+ nesting in TRAVERSE |
| 2 | `visitUpdateStatement` | `src/tundra_shell.cpp:972–1218` | ~247 | Parse, validate, coerce, execute |
| 3 | `visitDeleteStatement` | `src/tundra_shell.cpp:775–971` | ~197 | Multiple delete shapes |
| 4 | `visitCreateNodeStatement` | `src/tundra_shell.cpp:204–381` | ~178 | Per-type parse/validate/coerce loop |
| 5 | `SnapshotManager::commit` | `src/snapshot_manager.cpp:170–346` | ~177 | Linear script, many sequential concerns |
| 6 | `main` (shell) | `src/tundra_shell.cpp:2051–2212` | ~162 | CLI parsing, DB init, script mode, REPL |
| 7 | `populate_rows` | `src/core.cpp:259–397` | ~139 | Join bookkeeping + serial/parallel paths |
| 8 | `Database::update_by_match` | `src/core.cpp:1076–1215` | ~140 | Parse + plan + execute + apply |
| 9 | `extract_map_values_for_key` | `src/query_execution.cpp:166–298` | ~133 | Chunk/row/key loops + type switch |
| 10 | `create_table_from_rows` | `src/core.cpp:399–522` | ~124 | Double loop + ValueType switch |
| 11 | `EdgeStore::generate_table` | `src/edge_store.cpp:479–591` | ~113 | Collect + schema + fill + finish |
| 12 | `buildComparisonExpression` | `src/tundra_shell.cpp:1609–1701` | ~93 | Long if/else for ops and literals |
| 13 | `Storage::read_edges` | `src/storage.cpp:526–614` | ~89 | Fixed columns + property loop |
| 14 | `materialize_versioned_schema_fields` | `include/node_arena.hpp:755–839` | ~85 | Allocation planning + per-field materialization |
| 15 | `set_field_value_internal` | `include/node_arena.hpp:844–935` | ~92 | APPEND vs SET + old/new value paths |
| 16 | `set_nested_map_key` | `include/node_arena.hpp:957–1034` | ~78 | Type switch + capacity retry |
| 17 | `append_to_array_field` | `include/node_arena.hpp:1142–1215` | ~74 | Raw-array vs single + COW |

---

## 4. Memory-Sensitive Patterns (14 findings)

### High risk

| # | Pattern | File | Issue |
|---|---------|------|-------|
| 1 | `materialize_versioned_schema_fields` batch size vs offset | `node_arena.hpp:759–795` | `total_size` may not reserve for nested-path updates that consume `offset += fl.size` |

### Medium risk

| # | Pattern | File | Issue |
|---|---------|------|-------|
| 2 | `size_t` multiplication overflow | map_arena.hpp:53, array_arena.hpp:66–108, map_ref.hpp:197, memory_arena.hpp:154 | `elem_sz * capacity` can wrap |
| 3 | `FreeListArena` block walk | free_list_arena.hpp:295–307, 431–435 | Corrupted `size` → runaway loop |
| 4 | `reinterpret_cast` alignment | types.hpp, schema_layout.hpp, node_arena.hpp, utils.hpp | Unaligned load is UB on strict archs |
| 5 | `ValueRef` accessors null guard | types.hpp:297–328 | No null check before deref |
| 6 | `MemoryArena::allocate` overflow | memory_arena.hpp:58, 66–67 | `aligned_offset + size` can overflow |
| 7 | `MapRef::operator==` memcmp bounds | map_ref.hpp:197 | `count > capacity` → OOB read |

### Low risk (audited, appears correct)

| # | Pattern | File |
|---|---------|------|
| 8 | `StringPool::store_string` memcpy | string_arena.hpp:91–106 |
| 9 | `copy_value_into_entry` (fixed) | map_arena.hpp:313–316 |
| 10 | `move_entry` full-slot memcpy | map_arena.hpp:376 |
| 11 | Legacy MAP binary decode | storage.cpp:63–101 |
| 12 | `SchemaLayout::initialize_node_data` | schema_layout.hpp:201–206 |
| 13 | `edge.hpp` reinterpret_cast | edge.hpp:97–104 |
| 14 | Placement new in arenas | map/array/node arena, memory_arena |

---

## 5. Magic Numbers (13 categories, ~50+ instances)

| Category | Count | Examples |
|----------|-------|---------|
| Arena/buffer sizes (`2*1024*1024`, `1024*1024`, `10*1024*1024`) | 8 | node_arena, map_arena, array_arena, string_arena, memory_arena, free_list_arena, shard.hpp |
| String pool thresholds (`16`, `32`, `64`, pool index `3`) | 5 | string_arena.hpp, value_type.hpp, type_descriptor.hpp |
| Field cache / bitset (`64`, `>> 6`, `& 63`) | 3 | node_arena.hpp, schema_layout.hpp |
| Hash constants (`2166136261u`, `16777619u`, `48`, `0xFFFF`) | 5 | utils.hpp, row.cpp |
| Snapshot ID layout (`32`, `0xFFFFFFFF`) | 2 | utils.hpp |
| Inline capacities (`64`, `+32`, `4`, `2`) | 5 | row.hpp, query_execution.hpp |
| Flag bits (`0x1`) | 3 | map_ref.hpp, array_ref.hpp, string_ref.hpp |
| Repeated string identifiers (`"id"`, `"source_id"`, `"target_id"`, `"created_ts"`, `"_edge_id"`) | 10+ files | row.hpp, edge.hpp, node.hpp, core.cpp, edge_store.cpp, tundra_shell.cpp, query_execution.cpp |
| Debug truncation limits (`3`, `5`) | 1 | query_execution.cpp |
| UUID buffer (`37`) | 2 | utils.hpp, storage.cpp |
| Reserve heuristic (`32`) | 1 | storage.cpp |
| Growth factor (`2`) | 1 | query_execution.hpp |
| Min fragment size (`64`) | 2 | node_arena.hpp, free_list_arena.hpp |

---

## Summary Statistics

| Category | Findings |
|----------|----------|
| Duplicate logic | 16 |
| Dead code | 18 |
| Large/complex methods | 17 |
| Memory-sensitive patterns | 14 (1 high, 6 medium, 7 low) |
| Magic numbers | ~50+ instances in 13 categories |

---

## Next Steps (Phase 2: Safe Cleanup)

Priority order:
1. Remove high-confidence dead code (items 1–13)
2. Replace duplicate logic with existing helpers (safe, behavior-identical swaps)
3. Replace obvious magic numbers with named constants
