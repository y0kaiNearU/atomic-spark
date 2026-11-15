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
- [x] **M2 integration tests** (Docker). Done 2026-11-15. See the M2 section below.
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

## Auth-independent catalog access (2026-11-09)
- **New default `client="auto"`:** atomic-spark builds Iceberg's Java `RESTCatalog` inside Spark's
  JVM (via py4j, with `CatalogUtil.buildIcebergCatalog`) from the same Spark catalog options. Auth is
  then whatever Iceberg itself does, so Keycloak and the other login setups need no extra config.
  - Code: `atomic_spark/jvm.py`, with the client choice in `atomic_spark/clients.py`.
- **Fallback:** the Python HTTP client stays, for Spark Connect or by explicit choice.
  `ATOMIC_SPARK_CLIENT` overrides the choice.
- **Iceberg 1.12 detail:** `TableCommit.create(...)` takes two metadata objects, so each commit is
  built with `ImmutableTableCommit.builder()`.
- **Hardening:** a 409 now checks whether the commit was already applied, which can happen when the
  Java client retries internally, before reporting a `ConflictError`.
- **Tests:**
  - 71 unit tests;
  - the integration suite runs every scenario with both clients (22 passed).

## Keycloak auth tested (2026-11-15)
- **Setup:** `docker-compose.keycloak.yml` is an override that adds Keycloak 26.7.4 and a second
  Polaris realm, `EXTERNAL`, using `polaris.authentication."EXTERNAL".type: external`. That realm
  accepts only Keycloak tokens.
  - Keycloak realm `iceberg` (`docker/keycloak/iceberg-realm.json`): its client `client1:s3cr3t` maps
    to Polaris's root principal.
  - `docker/polaris/setup.sh` creates the `atomic` catalog in every realm that is enabled.
  - `POLARIS_AUTH=keycloak` switches the integration tests to `oauth2-server-uri` (Keycloak) plus
    `header.Polaris-Realm=EXTERNAL`.
- **Results:** 22/22 pass with Keycloak auth, for both the JVM and the HTTP client. They also pass
  with Polaris auth, both on the Keycloak stack and on the plain stack.
- **Negative check:** in `EXTERNAL`, a Polaris-issued token gets 401 and a Keycloak token gets 200.
  Polaris's own token endpoint returns 501 there. So the passing runs really went through Keycloak.

## M2: failure injection, and the in-flight race fixed (2026-11-15)
- **`tests/integration/faultproxy.py`:** an HTTP proxy that sits between every catalog client and
  Polaris. Spark's own client, both atomic-spark clients and the killed driver all go through it.
  It only touches publish commits (`transactions/commit` that set `main`), in one of four modes:
  - `drop`: not forwarded;
  - `apply_then_drop`: applied, response discarded;
  - `apply_then_504`: applied, 504 returned;
  - `hold`: forwarded only after the test releases it.
- **`tests/integration/test_failures.py`**, for both clients:
  - commit never arrives: `CommitFailedError`, tables untouched and usable;
  - applied but response lost, or 504: reported as committed;
  - client times out while the request is held: `CommitFailedError`. Once released, Polaris rejects
    the late request with 409.
  - driver SIGKILLed mid-block (`killed_driver.py`): `main` untouched. The orphaned branch has
    `max-ref-age-ms` set and doesn't block later blocks.
- **Race fixed, not just documented:**
  - **Before:** after a timeout with "nothing moved", the branches were dropped and failure reported.
    The held request could still land later. Confirmed on real Polaris: the late request got 204 and
    `main` moved.
  - **Now:** the publish commit also asserts `txn branch == head`. On "nothing moved", the library
    first drops the branches (the fence), then re-reads, then reports failure. If the fence itself
    fails, it raises `CommitStateUnknownError` and keeps the branches.
- **Observed:** neither client retries a publish whose request may have been sent. The proxy saw
  exactly 1 publish request in every fault mode.
- **`rest.client.socket-timeout-ms`** (Iceberg's option) now also sets the HTTP client's timeout.
- **Test layout:** fixtures moved to `tests/integration/conftest.py`, helpers to `helpers.py`.
- **Results:**
  - 79 unit tests;
  - 32 integration tests with Polaris auth;
  - 32 integration tests with Keycloak auth.

## Next steps
- **M3:** a `gc` command for orphaned `txn_*` branches. These come from killed drivers, and from
  writes to unlisted WAP tables, which create their own `txn_*` branch that is never published.
  - It must not remove the branch of a transaction that is still running. Options: an age threshold
    based on the branch's snapshot timestamps, or a transaction-ID or heartbeat property.
  - Also: transaction ID in snapshot summaries, and structured logging.

## How to run
```sh
docker compose up -d --wait polaris-setup           # Postgres + Polaris + "atomic" catalog
docker compose run --rm spark python spike/m0.py    # all checks; or pass IDs: C1 C7
docker compose down -v                              # stop and wipe
```
On Git Bash, prefix the `run` command with `MSYS_NO_PATHCONV=1`.

Tests: `pytest` runs the unit tests (needs no Spark).
`docker compose run --rm spark pytest tests/integration` runs the integration tests (stack must be up).

Keycloak variant (start from fresh volumes, because realms are bootstrapped only once):
```sh
docker compose down -v
docker compose -f docker-compose.yml -f docker-compose.keycloak.yml up -d --wait polaris-setup
docker compose -f docker-compose.yml -f docker-compose.keycloak.yml run --rm -e POLARIS_AUTH=keycloak spark pytest tests/integration
```

## Files
- `atomic_spark/`: the library. `README.md` has usage and limits.
- `tests/unit/`: unit tests with an in-memory fake catalog (`fakes.py`).
- `tests/integration/`: end-to-end tests against Polaris.
- `docker-compose.yml`, `docker/`: test stack (Polaris uses FILE storage on a shared `/warehouse` volume)
- `spike/m0.py`: M0 checks; `spike/polaris_rest.py`: minimal REST client;
  `spike/FINDINGS.md`: results; `spike/results.json`: raw output
