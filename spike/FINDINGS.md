# M0 findings

Stack: Spark 4.1.4, Iceberg 1.12.0, Polaris 1.8.0 (Postgres / relational-jdbc persistence).
Run: `docker compose up -d --wait polaris-setup && docker compose run --rm spark python spike/m0.py [C1 C7 ...]`.
Raw output of the last run: `spike/results.json`.

| # | Question | Result |
|---|---|---|
| C1 | Is a Polaris multi-table commit all-or-nothing? | **Yes.** One bad requirement gives 409 and neither table changes; a good commit gives 204 and both tables move. |
| C2 | Do all write types honour `spark.wap.branch`? | **Yes**, for both copy-on-write and merge-on-read: INSERT, UPDATE, DELETE, MERGE, dynamic INSERT OVERWRITE, `writeTo().append()`, `overwritePartitions()`, `saveAsTable(append)`. `main` never moved. |
| C2 | Can the job read its own writes? | **Yes.** A plain read in the WAP session sees the branch; after unsetting WAP, reads see `main`. |
| C3 | CREATE BRANCH on an empty table? | **Works** (Iceberg creates an empty snapshot for it). No special case needed. |
| C4 | Write with WAP on, but the branch doesn't exist yet? | **Branch is created automatically** on first write, and its first snapshot's parent is the `main` snapshot at that moment. Works on empty tables too. `main` is untouched. |
| C5 | Table **without** `write.wap.enabled`, WAP set in the session? | **Silent leak**: the write goes straight to `main`, with no error. |
| C6 | Requirement on a table with no updates (read-set validation)? | **Enforced.** A stale read gives 409, and the written table is untouched. |
| C7 | CREATE TABLE inside a multi-table commit (`assert-create`)? | **Not supported by Polaris 1.8**: 404 "Table does not exist". The same payload works through the single-table endpoint, so the payload is valid. Polaris's multi-table commit only updates existing tables. |
| C8 | `assert-current-schema-id` in a multi-table commit? | **Enforced.** 409, and other tables untouched. |
| C9 | ADD COLUMN mid-transaction, then write that column on the branch? | **Works.** `main`'s schema gets the column at once, but `main`'s data doesn't change until the commit. |
| C10 | Another writer commits to `main` mid-transaction? | Our commit **fails with 409** as intended. A second SparkSession does **not** inherit the WAP setting. |
| C11 | Does Spark see an external (REST) commit right away? | **No.** The cached table stays stale (4 rows instead of 6) until `REFRESH TABLE`. |
| C12/13 | Drop the branch after publish or abort? | Clean. `main` data intact, and only `main` remains in refs. |

## What this means for the design

1. **The core mechanism works** on Polaris: WAP branch per table, then one multi-table commit with `assert-ref-snapshot-id` on `main` and `set-snapshot-ref`. Atomicity, conflict detection, schema assertions and read-set validation all behave as needed.
2. **The library must `REFRESH TABLE` every published table** after the commit (C11), or the session keeps reading old data.
3. **The declared table list can become optional** (C4). Branches are created lazily, and the parent of a branch's first snapshot gives that table's start snapshot. Caveat: the start snapshot is "`main` at first write", not "`main` at block start", so reads made before the first write aren't protected unless the table is listed for read-set validation (C6).
4. **The real leak is tables without `write.wap.enabled`** (C5). It isn't about declared or undeclared tables. Policy proposal:
   - The library sets WAP on declared tables.
   - It provides an optional `enable_wap(catalog)` helper to turn WAP on catalog-wide. With WAP everywhere, *every* write in a block lands on a `txn_*` branch (C4) and can be found and published, so nothing escapes.
5. **Transactional CREATE TABLE (C7) isn't possible** through Polaris's multi-table commit, so the planned M4 item changes. Options:
   - (a) Create new tables up front, empty and visible, and drop them on rollback. Simple, but an empty table is briefly visible.
   - (b) Use per-table staged create committed right after the multi-table commit. Not atomic: there's a window, and a possible orphan.
   - (c) Contribute create support to Polaris's `commitTransaction`.

   I recommend (a) for M4 and documenting the gap.
6. Additive DDL behaves as planned (C9). Detecting destructive DDL through `assert-current-schema-id` works (C8).
