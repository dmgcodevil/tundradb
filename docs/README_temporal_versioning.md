# TundraDB Temporal Versioning Spec (VALIDTIME + TXNTIME)

This README is a developer-focused spec for implementing temporal queries and temporal updates (including retroactive corrections) using a `VersionInfo` chain per logical record (node or edge).

The goal is to make the model *click*:
- **VALIDTIME** = when a fact is true in the domain/world
- **TXNTIME** = when the database recorded/“knew” that fact (system time)

> **TL;DR:** You do not rewrite history by editing old versions. You create new versions with new (valid/txn) intervals. The only safe in-place mutation is closing `tx_to` on previously-current versions.

---

## 1) Data Model

### 1.1 Version record

```cpp
struct VersionInfo {
  uint64_t version_id;

  // Domain validity (world time): [valid_from, valid_to)
  uint64_t valid_from;
  uint64_t valid_to;   // default = INF

  // System validity (knowledge/commit time): [tx_from, tx_to)
  uint64_t tx_from;    // commit timestamp or monotonic commit id
  uint64_t tx_to;      // default = INF

  // Sparse deltas: field_idx -> value pointer (nullptr means explicit NULL)
  map<uint16_t, value> updated_fields;

  // Newest -> older
  VersionInfo* prev;
};
```

### 1.2 Logical record vs versions

A **logical record** (a node id or edge id) is stable. It has a chain of versions:

```
logical_id: 42
  head -> v9 -> v8 -> v7 -> ...
```

Each `VersionInfo` represents a set of *field deltas* and an interval in **valid-time** and **txn-time**.

### 1.3 Optional: edge identity choices

You must choose how to identify edges:

**A) Stable `edge_id` (recommended)**
- An edge has a stable id, and versions can change edge properties (including dst) over time.
- DIFF can show `UPDATED`.

**B) Tuple identity**
- Edge identity is `(src, label, dst, key?)`.
- Changing `dst` is modeled as `REMOVED+ADDED`.

Both are acceptable; A is better for bitemporal editing and auditing.

---

## 2) Time Semantics

### 2.1 VALIDTIME (domain time)

- Answers: **“When was it true in the world?”**
- Used for: temporal graphs, historical queries (friendship at date), truth as-of time.

### 2.2 TXNTIME (system/knowledge time)

- Answers: **“When did the DB record/believe it?”**
- Used for: audit/replay (“what did we know then?”), backtesting, preventing future leakage, late-arriving data, corrections.

### 2.3 Visibility rule (bitemporal)

Given query snapshot `(vtime, txtime)`, a version `v` is visible if:

```
v.valid_from <= vtime < v.valid_to
AND
v.tx_from    <= txtime < v.tx_to
```

If you only implement VALIDTIME initially, treat `tx_from=0, tx_to=INF` for all versions (or ignore tx constraints).

---

## 3) Immutability / Mutability Rules (Critical)

**After commit:**

- `valid_from`, `valid_to` are **immutable** (do not rewrite domain history).
- `tx_from` is **immutable** (it is commit time).
- `tx_to` is **mutable only to close old versions** when a newer belief is written.

> The only allowed in-place mutation is:
>
> **`old_version.tx_to = tx_now`**
>
> This preserves reproducible snapshots: old versions remain visible for earlier `txtime`.

---

## 4) Field Materialization with `updated_fields`

`updated_fields` is a sparse delta map. Resolving a field `f` at a visible version:

```
value resolve(logical_id, field_idx, snapshot):
  v = visible_version(logical_id, snapshot)
  while v != null:
    if v.updated_fields contains field_idx:
       return v.updated_fields[field_idx]   // may be NULL explicitly
    v = v.prev
  return DEFAULT/ABSENT (schema default)
```

### 4.1 Performance note

Avoid full materialization:
- resolve only fields referenced by the query
- cache `(logical_id, snapshot, field_idx)` during query execution

---

## 5) Snapshot Selection in Queries

### 5.1 Read queries

A query can specify one or two time axes:

```sql
AS OF VALIDTIME @v
AS OF TXNTIME   @t
MATCH ...
SELECT ...
```

If `AS OF TXNTIME` is omitted, treat it as `now` (or latest commit) for “current belief”.

### 5.2 Pattern matching

`AS OF` applies to:
- nodes (their visible versions)
- edges (their visible versions)
- edge existence (edges can appear/disappear by valid/txn windows)

---

## 6) Updates: Concepts

There are two distinct intents:

### 6.1 “Current update” (world changes now)

Example: “Alice moved departments today.”
- `valid_from = now`
- this is not a retroactive correction; it is a new fact from now onward.

### 6.2 “Correction” (retroactive fix)

Example: “We learned Alice actually moved on Jan 14.”
- `valid_from = Jan14` (back in the past)
- `tx_from = commit_time` (when we learned and recorded it)

This is why VALIDTIME != TXNTIME.

---

## 7) Recommended Update Syntax (Minimal Extensions)

Your current syntax:

```sql
MATCH (u:User) WHERE u.name = "Alice"
SET u.age = 26, u.department = "Sales";
```

### 7.1 Add a valid-time target interval

To make corrections explicit:

```sql
MATCH (u:User) WHERE u.name = "Alice"
FOR VALIDTIME [@vf, @vt)
SET u.department = "Sales";
```

- `FOR VALIDTIME [...]` defines the **domain interval** to patch.
- `tx_from` is automatically set to `tx_now` at commit.

### 7.2 Optional: `AS OF` for target selection

This controls how MATCH is evaluated:

```sql
MATCH (u:User)-[:MEMBER_OF]->(t:Team)
AS OF VALIDTIME @v_sel
AS OF TXNTIME   @t_sel
WHERE t.name = "Risk"
FOR VALIDTIME [@vf, INF)
SET u.department = "Sales";
```

This helps avoid “future knowledge” when running repair jobs.

---

## 8) Core Algorithms

### 8.1 Helper: interval overlap

Intervals are half-open: `[from, to)`.

```
overlaps([a_from, a_to), [b_from, b_to)) = (a_from < b_to) && (b_from < a_to)
```

### 8.2 Finding currently-believed versions at commit time

When writing at `tx_now`, find versions visible in TX at `tx_now`:

```
tx_from <= tx_now < tx_to
```

(Optionally also filtered by logical id and other constraints.)

### 8.3 Interval surgery for corrections

Operation:
- Patch fields for valid interval `[corr_from, corr_to)` (corr_to may be INF)
- Assign `tx_from = tx_now`

**High-level:**
1. Find versions currently-believed at `tx_now`
2. For each version overlapping `[corr_from, corr_to)`:
   - close old belief: `old.tx_to = tx_now`
   - insert up to 3 new versions at `tx_now`:
     - left remainder
     - corrected middle
     - right remainder
3. Coalesce adjacent segments with identical values

---

## 9) Worked Examples (Version Chains)

All times below are simplified integers.

### 9.1 Example A: current update (not correction)

Alice department becomes Sales at time 100, recorded at tx=100.

Existing belief:
- dept=Eng for `[1, INF)` at tx `[1, INF)`

Write: `SET dept=Sales` for `[100, INF)` at `tx_now=100`.

Result versions:

```
v1: dept=Eng
    valid [1, INF)
    tx    [1, 100)        // closed at tx=100

v2: dept=Eng
    valid [1, 100)
    tx    [100, INF)      // reassert unchanged left remainder (optional; see notes)

v3: dept=Sales
    valid [100, INF)
    tx    [100, INF)
```

**Optimization:** You can avoid writing `v2` by keeping an unchanged segment represented implicitly,
but many implementations keep explicit segments for simplicity and coalescing.

### 9.2 Example B: retroactive correction

Reality: Alice moved to Sales at valid time 80, but the DB learned this only at tx=120.

Initial belief (at tx=1):
- dept=Eng valid `[1, INF)` tx `[1, INF)`

Correction write at `tx_now=120`:
- patch dept=Sales for valid `[80, INF)`

Result:

```
v1: dept=Eng
    valid [1, INF)
    tx    [1, 120)     // old belief still visible for txtime<120

v2: dept=Eng
    valid [1, 80)
    tx    [120, INF)

v3: dept=Sales
    valid [80, INF)
    tx    [120, INF)
```

Now:
- Query `(vtime=90, txtime=100)` -> sees **Eng** (what we believed then)
- Query `(vtime=90, txtime=130)` -> sees **Sales** (corrected belief)

---

## 10) Corner Cases (Must Handle)

### 10.1 No-op update

If the patch does not change any field values over `[vf, vt)`:
- Do **not** create new versions
- Return “0 changed” (and DIFF empty)

### 10.2 Boundary already exists

If the chain already has a segment boundary at `vf`:
- only patch the overlapping segments
- avoid extra splitting

### 10.3 Patch overlaps multiple segments

Example segments:
- `[1,50) Eng`, `[50,120) Ops`, `[120, INF) Eng`
Patch `[100, INF) -> Sales`

You must:
- split `[50,120)` into `[50,100) Ops` + `[100,120) Sales`
- patch `[120, INF) Eng -> Sales`
- coalesce adjacent Sales segments if produced

### 10.4 Gaps in valid-time

If you allow gaps (no version covers some `[t1,t2)`), define behavior:
- treat as “unknown / absent” (no visible row)
- `FOR VALIDTIME` may *create* versions in gaps (new facts)

### 10.5 Explicit NULL vs inherited

In `updated_fields`, a `NULL` value means “explicitly NULL”.
Absence of field in map means “inherit from prev”.

DIFF must distinguish:
- field becomes NULL
- field not present / unchanged

### 10.6 Deleting a property vs deleting an edge

Two approaches:
- **tombstone**: an edge version with a deleted flag
- **interval end**: set valid_to to deletion time (via new versions; do not mutate old valid_to)

Recommended: represent deletion as versioning:
- close old belief in TX (`tx_to = tx_now`)
- write new version(s) that remove existence for a valid interval
  - for edges, this may mean the edge simply has no visible version for that interval
  - or a tombstone version that suppresses older versions in that interval

### 10.7 Multiple corrections over time

If you correct the same logical record multiple times:
- you create multiple tx-time “belief layers”
- queries at different `txtime` see different beliefs
- coalesce within the same tx_from layer if desired

### 10.8 Concurrent writers

If concurrent commits can write the same logical record:
- you must serialize updates per logical id, or
- ensure deterministic conflict resolution (e.g., commit id ordering)

---

## 11) DIFF Operator (Audit-Ready)

DIFF compares two snapshots:

Snapshot A: `(vA, tA)`
Snapshot B: `(vB, tB)`

### 11.1 Entity diff (nodes/edges)

```sql
DIFF
FROM (VALIDTIME @vA, TXNTIME @tA)
TO   (VALIDTIME @vB, TXNTIME @tB)
MATCH (u:User {id:"Alice"})-[e:WORKS_AT]->(org)
RETURN u, e, org;
```

Output shape (Arrow-friendly):
- `diff_type` in {`ADDED`, `REMOVED`, `UPDATED`}
- `entity_kind` in {`node`, `edge`}
- identifiers (`label`, `id` / `src,dst,type`)
- optional field-level details (`field`, `old_value`, `new_value`)

### 11.2 Result diff (diff a query’s output)

```sql
DIFF RESULT
FROM (...) TO (...)
MATCH ...
SELECT ...
KEY BY <stable columns>;
```

This is ideal for “why did this decision change?” audits.

---

## 12) Implementation Checklist

### 12.1 Required primitives

- `visible_version(logical_id, vtime, txtime)`
- `resolve_field(logical_id, field_idx, snapshot)`
- `find_versions_visible_in_tx(logical_id, tx_now)` (for writers)
- `insert_version(logical_id, VersionInfo*)`
- `close_version_tx(VersionInfo*, tx_now)` (mutate `tx_to`)
- `coalesce_adjacent_versions(logical_id, tx_from_layer)` (optional but recommended)

### 12.2 Writer invariants to maintain

- For a fixed `(logical_id, tx_layer)` the valid-time segments should not overlap.
- For a fixed `(logical_id, snapshot)` there should be at most one visible version per valid-time point.

---

## 13) Recommended Defaults

- Use half-open intervals `[from, to)`.
- Represent INF as `UINT64_MAX`.
- Use commit id or monotonic clock for `tx_from`.
- Default read behavior:
  - If `AS OF TXNTIME` is not specified, treat as “latest” (now).
  - If `AS OF VALIDTIME` is not specified, treat as “now” (or required).

---

## 14) Appendix: ASCII Diagram Cheat Sheet

### 14.1 Single correction (retroactive)

```
Time axis (VALIDTIME):      1 -------- 80 --------------- INF
Old belief (tx<120):       Eng===============================>
New belief (tx>=120):      Eng==========| Sales==============>
                                 ^ split at 80
TX axis: tx<120 sees old; tx>=120 sees new
```

---

## 15) Notes on Storage Minimization (Optional)

You can reduce version explosion by:
- writing only the patched middle versions
- leaving unchanged parts represented by older versions plus careful tx windows
…but this complicates invariants.

For a first implementation, prefer:
- explicit left/mid/right segments
- coalescing adjacent identical segments

---

## 16) Examples you can paste into docs

### Simple correction update

```sql
MATCH (u:User) WHERE u.name = "Alice"
FOR VALIDTIME [80, INF)
SET u.department = "Sales";
```

### Safe replay query

```sql
AS OF VALIDTIME 90
AS OF TXNTIME   100
MATCH (u:User) WHERE u.name="Alice"
SELECT u.department;
```

---

If you want, I can also generate a short `spec_tests.md` with:
- input version chains
- update operations
- expected resulting chains
- expected query outputs at various snapshots
