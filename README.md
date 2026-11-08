# atomic-spark

Atomic blocks for PySpark on Iceberg. A block can write, MERGE or DELETE across several tables, and
either every change is published or none is. If a stage fails, you don't roll anything back by hand.

```python
from atomic_spark import atomic

with atomic(spark, "polaris", tables=["db.orders", "db.customers", "db.audit"]):
    spark.sql("MERGE INTO polaris.db.orders t USING updates s ON t.id = s.id ...")
    spark.sql("DELETE FROM polaris.db.customers WHERE ...")
    audit_df.writeTo("polaris.db.audit").append()
# block finished normally  -> all three tables published in one atomic catalog commit
# an exception in the block -> nothing published, the exception propagates
```

Requirements:
- an **Iceberg REST catalog that supports multi-table commits** (`POST /v1/{prefix}/transactions/commit`),
  e.g. Apache Polaris;
- PySpark 4.x with the Iceberg Spark runtime and SQL extensions.

The connection settings are read from your Spark catalog config (`uri`, `warehouse`, `credential` or
`token`, `scope`, `header.*`).

## How it works
1. **Begin:** a branch `txn_<id>` is created on every listed table, in one catalog commit, at the
   table's current `main`. Then `spark.wap.branch` is set, so every write in the session goes to that
   branch. Reads in the session also see the branch, so later stages see earlier stages' writes.
2. **Commit:** one multi-table commit moves each changed table's `main` to its branch head. It is
   guarded by checks that `main` hasn't moved, that the table wasn't dropped or recreated, and that
   its schema is unchanged.
3. **Rollback:** `main` is never touched; the branches are dropped.

## Usage
```python
# Retry the whole job when another writer got there first (decorator form only):
@atomic(spark, "polaris", tables=["db.orders"], retries=3, retry_wait=2.0)
def nightly_load():
    ...

# Also fail if tables the job only *reads* changed meanwhile:
with atomic(spark, "polaris", tables=["db.report"], read_tables=["db.orders"]):
    ...

# Tables need write.wap.enabled=true (otherwise WapNotEnabledError); or let atomic-spark set it:
with atomic(spark, "polaris", tables=["db.new_table"], enable_wap=True):
    ...
```

| Exception | Meaning |
|---|---|
| `ConflictError` | Another writer changed a table (or a `read_tables` entry). Nothing published; safe to re-run. |
| `DestructiveSchemaChangeError` | A column was dropped, renamed or narrowed during the block. Nothing published, but the DDL itself stays applied. |
| `CommitFailedError` | The catalog rejected the commit. Nothing published. |
| `CommitStateUnknownError` | The commit's outcome couldn't be determined. Branches are kept for inspection. |
| `WapNotEnabledError` / `TransactionUsageError` | Setup problems, raised before anything runs. |

## Limits
- **List every table the block writes to.** A write to an unlisted table that has
  `write.wap.enabled` creates its own `txn_*` branch, and that write is lost. A write to an unlisted
  table *without* WAP goes straight to `main`, outside the transaction.
- **One open transaction per SparkSession.** Sequential blocks are fine; nesting is rejected.
- **DDL is not transactional.** Iceberg schemas are shared across branches. Additive changes
  (ADD COLUMN) are allowed; destructive ones fail the commit but are not undone. CREATE TABLE inside a
  block is not covered.
- **Conflicts are detected per table, not per row.** Any commit to a listed table's `main` during
  the block counts as a conflict.
- **Only catalog changes are covered.** External effects (APIs, files written outside the catalog)
  are not rolled back.
- **DataFrames cached inside a block** keep their data after a rollback.

## Development
```sh
pip install -e ".[test]" && pytest                      # unit tests (no Spark needed)
docker compose up -d --wait polaris-setup               # Postgres + Polaris 1.8 + "atomic" catalog
docker compose run --rm spark pytest tests/integration  # end-to-end against Polaris + Spark 4.1 + Iceberg 1.12
docker compose down -v
```
On Git Bash for Windows, prefix `docker compose run` with `MSYS_NO_PATHCONV=1`.
