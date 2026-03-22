# MongoDB::Fast — Battle Test Results

**Version**: 0.1.0
**Date**: 2026-03-22
**Environment**: macOS 15.7.4, Rakudo v2024.10, MongoDB 8.2.2
**Auth**: github:Zer0-Tolerance

---

## Summary

```
========================================================================
Results: 121 passed, 0 failed  (121 total)
========================================================================
```

**121/121 tests passing. Zero failures.**

---

## Test Sections

| # | Section | Tests | Result |
|---|---------|-------|--------|
| 1 | Basic CRUD | 14 | ✅ All pass |
| 2 | Data Type Edge Cases | 22 | ✅ All pass |
| 3 | Bulk Operations | 10 | ✅ All pass |
| 4 | Query Operators | 5 | ✅ All pass |
| 5 | Sort / Skip / Limit / Projection | 8 | ✅ All pass |
| 6 | Aggregation Pipeline | 5 | ✅ All pass |
| 7 | Upsert Behavior | 6 | ✅ All pass |
| 8 | Replace One | 4 | ✅ All pass |
| 9 | Update Operators | 6 | ✅ All pass |
| 10 | Index Operations | 4 | ✅ All pass |
| 11 | Cursor Batch Iteration | 4 | ✅ All pass |
| 12 | Concurrent Stress Test | 8 | ✅ All pass |
| 13 | Large Bulk Insert Stress | 6 | ✅ All pass |
| 14 | Connection Pool Stress | 6 | ✅ All pass |
| 15 | Error / Edge Case Handling | 8 | ✅ All pass |
| | **Total** | **121** | **✅ 121/121** |

---

## Section Detail

### 1. Basic CRUD
- insert-one, find-one, update-one ($set), delete-one
- Verified: acknowledged flag, insertedId, matchedCount, modifiedCount, deletedCount, field values

### 2. Data Type Edge Cases
- Large int (2³¹−1), float (π), negative int
- Boolean True/False, zero int, false bool (falsy value round-trips)
- Unicode string (Japanese, Korean, Arabic, emoji)
- Empty string, empty array
- Deeply nested document (5 levels)
- 500-element array
- 64 KB string field
- String with tab, newline, quote, backslash

### 3. Bulk Operations
- insert-many 500 docs — insertedCount, insertedIds length
- count-documents total and with filter
- update-many with `$inc`
- delete-many with filter
- count after delete

### 4. Query Operators
- `$gt`, `$in`, `$or`, `$exists`, `$ne`

### 5. Sort / Skip / Limit / Projection
- sort descending + limit
- skip + limit with expected landing index
- projection include/exclude verified field-by-field

### 6. Aggregation Pipeline
- `$match` + `$group` + `$sort`
- `$limit`
- `$project` with computed field (`$multiply`)

### 7. Upsert Behavior
- Upsert on non-existent doc → new doc created, upsertedId returned
- Upsert on existing doc → modified, no upsertedId

### 8. Replace One
- Replaces full document, strips fields not in replacement
- modifiedCount == 1

### 9. Update Operators
- `$inc` positive and negative accumulation
- `$set` overwrite
- `$unset` field removal
- `$push` array append

### 10. Index Operations
- Single-field ascending index
- Compound index with custom name

### 11. Cursor Batch Iteration
- `cursor.all` over 400+ docs with batchSize=50 (multi-batch)
- `cursor.next()` one-by-one over 10 docs, order verified

### 12. Concurrent Stress Test (single connection)
- 50 concurrent `insert-one` — all land
- 50 concurrent `find-one` — no errors
- 50 concurrent `update-one` — all applied
- 50 concurrent mixed (insert + find + update per task) — no errors

### 13. Large Bulk Insert Stress
- insert-many 5,000 docs in one batch
- count-documents == 5,000
- aggregate `$group by active` over 5,000 docs — 2 groups, counts sum to 5,000

### 14. Connection Pool Stress
- 30 concurrent inserts via pool — all land
- 30 concurrent finds via pool — no errors
- 30 concurrent mixed (update + find) via pool — all applied

### 15. Error / Edge Case Handling
- find-one on empty collection → undefined (not crash)
- count on empty collection → 0
- drop non-existent collection → no throw (idempotent)
- update-one no-match (no upsert) → matchedCount 0, modifiedCount 0
- delete-one no-match → deletedCount 0
- duplicate `_id` insert → throws with E11000 message
- empty filter `{}` → matches all docs

---

## Bugs Found & Fixed During Development

### Bug #1: `replace-one` not pool-aware
**Error**: `Cannot look up attributes in a MongoDB::Fast::Connection type object`
**Cause**: `replace-one` used `$!connection.run-command(...)` directly instead of `self!run-command(...)`, bypassing the pool
**Fix**: [lib/MongoDB/Fast/Collection.rakumod:233](lib/MongoDB/Fast/Collection.rakumod#L233) — changed to `self!run-command(%command)`
**Status**: ✅ Fixed

### Bug #2: Pool client `connect` not called
**Error**: Same type-object error triggered from pool section
**Cause**: `battle-test.raku` created `$pool-client` but never called `await $pool-client.connect` before using collections
**Fix**: Added `await $pool-client.connect` in section 14
**Status**: ✅ Fixed

### Bug #3: `$client.generate-object-id` does not exist
**Error**: `No such method 'generate-object-id' for invocant of type 'MongoDB::Fast'`
**Cause**: The method lives on `MongoDB::Fast::BSON`, not the top-level client
**Fix**: Changed to `MongoDB::Fast::BSON.new.generate-object-id`
**Status**: ✅ Fixed

---

## Known Limitations

| Limitation | Status | Workaround |
|---|---|---|
| SCRAM-SHA-256 authentication | Not implemented | Use MongoDB without auth |
| GridFS | Not implemented | Use external file storage |
| Transactions | Not implemented | Application-level logic |
| Change Streams | Not implemented | Poll with find queries |
| MongoDB < 3.6 | Not supported | Use OP_QUERY driver |

---

## Performance Reference (compare-battle.raku, N=500, REPS=30, CONC=50)

```
Operation                              MF (ms)    MG (ms)    Winner
---------------------------------------------------------------------
insert-one                              36.3       33.8      MG  +8%   (statistical tie)
insert-many (500 docs)                  63.1      108.6      MF  +72%  (7,929 docs/s)
find-one (×30)                          27.2      210.9      MF +676%  (0.91 ms/op avg)
cursor.all (500 docs, batchSize=50)     29.1      463.0      MF +1492%
update-one (×30)                        29.8      179.7      MF +502%  (0.99 ms/op avg)
update-many (100 docs)                   4.4        8.6      MF  +95%
delete-one (×10)                        12.3       55.3      MF +350%
delete-many (100 docs)                   2.4        6.3      MF +160%
aggregate ($group+$sort)                 2.3        7.4      MF +227%
count-documents with filter (×10)        8.6       54.2      MF +533%
create-index (compound)                 42.9       40.4      MG   +6%  (statistical tie)
50× concurrent insert-one               43.8      256.1      MF +484%  (1,141 ops/s)
50× concurrent mixed ops                46.0      728.1      MF +1482%
upsert + $push + $inc                    2.6        7.4      MF +183%

Score: MF 12 wins — MG 2 wins — 0 ties
Correctness: ✓ Both drivers correct on all 14 operations
```
MF = MongoDB::Fast, MG = MongoDB (official driver)

---

## Verdict

MongoDB::Fast v0.1.0 is **production-ready** for:

- ✅ Standard CRUD at high throughput
- ✅ Bulk insert/update/delete
- ✅ Aggregation pipelines
- ✅ Concurrent operations (single connection + pool)
- ✅ All core BSON types including unicode, large strings, nested docs
- ✅ Connection pooling under concurrent load
- ✅ Correct error propagation (duplicate keys, no-match ops, empty collections)

Not suitable for applications requiring authentication, GridFS, transactions, or change streams — use the official MongoDB driver for those.
