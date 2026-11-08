# atomic-spark: progress and handoff

Goal: Oracle-style atomic blocks for multi-stage PySpark jobs on Iceberg tables. Either every
write, merge and delete in a block is published, or none is. This removes manual rollbacks.

## Design
1. **Begin:** for each table, check that `write.wap.enabled=true`, record the `main` snapshot,
   and create the branch `txn_<id>`. Then set `spark.wap.branch=txn_<id>` in the session.
2. **Body:** the user's stages run unchanged. Writes go to the branch, and reads in the session
   see the branch.
3. **Commit:** send one Polaris `POST /v1/{prefix}/transactions/commit`. Per table it carries
   `assert-ref-snapshot-id(main == start)` and `set-snapshot-ref(main -> branch head)`.
   It also asserts the schema ID and, optionally, the start snapshots of tables the job only reads.
4. **Unknown outcome** (timeout): re-read the refs. If every table points at its branch head, the
   commit happened; if every table is still at its start snapshot, it didn't. Anything else: raise.
5. **Cleanup (always):** unset WAP, drop the `txn_*` branches, `REFRESH TABLE` each published table.
6. **Rollback:** an exception skips step 3. `main` is never touched.

Target API:
```python
with atomic(spark, catalog="polaris", tables=["db.a", "db.b"]):
    spark.sql("MERGE INTO polaris.db.a ...")
# plus a decorator form with retries=N, which re-runs the job on conflict
```

## Decisions made
- **Environment:** catalog is Iceberg REST on **Polaris**, client is **PySpark**, newest versions
  (Spark 4.1.4, Iceberg 1.12.0, Polaris 1.8.0).
- **Concurrency:** **sequential blocks only**. Nesting and threads are rejected, and there is no `tx.spark`.
- **Other writers do write the same tables at the same time.** So conflicts matter: commits fail
  with `ConflictError`, and a decorator with `retries=` re-runs the job. Rebasing onto the other
  writer's changes is M4.
- **DDL:**
  - additive changes (ADD COLUMN, set property) are allowed and take effect immediately;
  - destructive changes are detected through the schema ID and fail the commit;
  - CREATE TABLE is handled in M4.
- **Integration tests use Docker.**

## Milestones
- [x] **M0 spike.** Done 2026-11-08. See [spike/FINDINGS.md](spike/FINDINGS.md). 12 of 13 checks
  behave as the design needs.
- [x] **M1 core library.** Done. See the M1 section below.
  - 60 unit tests (`tests/unit`, run without Spark against a fake catalog);
  - 10 integration tests (`tests/integration`) pass against real Polaris.
- [ ] **M2 integration tests** (Docker). Some are already covered by M1: failure mid-job,
  conflicting writer, decorator retry, empty tables, DDL. Still to do: commit timeout or lost
  response against the real server, and a driver killed mid-block (orphaned branches).
- [ ] **M3 operations:** `gc` for orphaned `txn_*` branches, logging, transaction ID in snapshot summaries.
- [ ] **M4 extras:**
  - transactional CREATE TABLE: create up front, drop on rollback;
  - conflict rebase;
  - catalog-wide `enable_wap()` helper;
  - optional JVM extension that blocks DDL outright.

## Key M0 findings that shape M1
1. A table **without** `write.wap.enabled` leaks: its writes go to `main` silently. The library
   must check or set WAP on its tables.
2. Branches are **created automatically on first write**, with parent = the `main` snapshot at that
   moment. This means the declared table list *could* become optional.
3. Spark's cache stays stale after the REST commit, so `REFRESH TABLE` is required.
4. Polaris's multi-table commit **cannot create tables** (404). Transactional CREATE TABLE needs
   another route (M4).

## M1: what was built (2026-11-08)
- **Decision:** `tables=[...]` stays **required**. `read_tables=[...]` is optional, for validating
  tables the job only reads.
- **Modules** in `atomic_spark/`:
  - `api.py`: `atomic()`, used as a context manager or as a decorator with `retries=`;
  - `transaction.py`: the state machine;
  - `rest.py`: REST client, OAuth token caching, payload helpers;
  - `config.py`: reads the Spark catalog conf;
  - `schema.py`: classifies schema changes as additive or destructive;
  - `model.py`: table identifiers and per-table state;
  - `spark_ops.py`: the Spark-session seam, so tests can fake it;
  - `errors.py`: exception types.
- **Begin creates branches over REST, not Spark SQL.** All tables get their branch in one
  multi-table commit, guarded by `main == recorded start`, then each table is refreshed in Spark.
  Spark SQL would branch off Spark's cached (possibly stale) `main` and could lose another writer's
  update. Begin retries up to 3 times if `main` moves in the meantime.
- **Empty tables** (no snapshot) get no branch at begin; the first write creates it (M0 C4).
  The commit then asserts that `main` still doesn't exist.
- **Other details:**
  - the commit preserves `main`'s retention settings;
  - transaction branches get `max-ref-age-ms` (3 days), so snapshot expiry removes orphaned ones;
  - unknown outcomes (5xx or network error) are settled by re-reading `main`: if every table moved,
    it's committed; if none moved, it's `CommitFailedError`; if mixed, it's
    `CommitStateUnknownError` and the branches are kept.
- **Bug found by the integration tests and fixed:** branches weren't dropped when every declared
  table started empty. A regression unit test was added.

## Next steps
- **M2:** lost-response and timeout tests against real Polaris (for example a proxy that drops the
  response), and a killed driver.
- **M3:** a `gc` command for orphaned `txn_*` branches. These come from killed drivers, and from
  writes to unlisted WAP tables, which create their own `txn_*` branch that is never published.
  Also: transaction ID in snapshot summaries, and structured logging.
- **Known race (document or fix):** after a timeout, the library may see "nothing moved" and drop
  the branches while the server is still applying the commit. `main` would still be valid, since
  snapshots outlive refs, but the job would report failure for a commit that succeeded.

## How to run
```sh
docker compose up -d --wait polaris-setup           # Postgres + Polaris + "atomic" catalog
docker compose run --rm spark python spike/m0.py    # all checks; or pass IDs: C1 C7
docker compose down -v                              # stop and wipe
```
On Git Bash, prefix the `run` command with `MSYS_NO_PATHCONV=1`.

Tests: `pytest` runs the unit tests (needs no Spark).
`docker compose run --rm spark pytest tests/integration` runs the integration tests (stack must be up).

## Files
- `atomic_spark/`: the library. `README.md` has usage and limits.
- `tests/unit/`: unit tests with an in-memory fake catalog (`fakes.py`).
- `tests/integration/`: end-to-end tests against Polaris.
- `docker-compose.yml`, `docker/`: test stack (Polaris uses FILE storage on a shared `/warehouse` volume)
- `spike/m0.py`: M0 checks; `spike/polaris_rest.py`: minimal REST client;
  `spike/FINDINGS.md`: results; `spike/results.json`: raw output
