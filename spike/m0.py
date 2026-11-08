"""M0 spike: verify the assumptions atomic-spark is built on.

Run inside the compose stack:
    docker compose run --rm spark python spike/m0.py

Every check runs in its own fresh tables inside a fresh namespace, records
PASS / FAIL / INFO plus the observed facts, and never stops the run.
Results go to stdout and spike/results.json.
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
import uuid

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession  # noqa: E402

from polaris_rest import IcebergRest, RestError, assert_ref, change, set_ref  # noqa: E402

URI = os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog")
CRED = os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t")
WAREHOUSE = os.environ.get("POLARIS_WAREHOUSE", "atomic")
CAT = "polaris"
NS = f"m0_{int(time.time())}"

spark = (
    SparkSession.builder.master("local[2]")
    .appName("atomic-spark-m0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CAT}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CAT}.type", "rest")
    .config(f"spark.sql.catalog.{CAT}.uri", URI)
    .config(f"spark.sql.catalog.{CAT}.warehouse", WAREHOUSE)
    .config(f"spark.sql.catalog.{CAT}.credential", CRED)
    .config(f"spark.sql.catalog.{CAT}.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.defaultCatalog", CAT)
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
rest = IcebergRest(URI, CRED, WAREHOUSE)

# ----------------------------------------------------------------------------- helpers


def fq(t: str) -> str:
    return f"{CAT}.{NS}.{t}"


def sql(q: str):
    return spark.sql(q)


def make_table(t: str, wap: bool = True, rows: bool = True, mor: bool = False) -> str:
    props = {"format-version": "2"}
    if wap:
        props["write.wap.enabled"] = "true"
    if mor:
        props.update({m: "merge-on-read" for m in ("write.delete.mode", "write.update.mode", "write.merge.mode")})
    tbl = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    sql(f"CREATE TABLE {fq(t)} (id BIGINT, v STRING, p INT) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ({tbl})")
    if rows:
        sql(f"INSERT INTO {fq(t)} VALUES (1,'a',1),(2,'b',1),(3,'c',2),(4,'d',2)")
    return fq(t)


def refs(t: str) -> dict:
    return rest.refs(NS, t)


def count(t: str, ref: str | None = None) -> int:
    q = f"SELECT count(*) FROM {fq(t)}" + (f" VERSION AS OF '{ref}'" if ref else "")
    return sql(q).collect()[0][0]


def wap(branch: str | None):
    if branch:
        spark.conf.set("spark.wap.branch", branch)
    else:
        spark.conf.unset("spark.wap.branch")


def short(e: BaseException) -> str:
    msg = str(e).strip().splitlines()
    return (msg[0] if msg else type(e).__name__)[:400]


def commit(changes: list[dict]) -> tuple[int, str]:
    try:
        r = rest.commit_transaction(changes)
        return r.status_code, r.text[:300]
    except RestError as e:
        return e.status, e.body[:300]


CHECKS: list = []
RESULTS: list = []


def check(cid: str, title: str):
    def deco(fn):
        CHECKS.append((cid, title, fn))
        return fn

    return deco


# ----------------------------------------------------------------------------- checks


@check("C1", "Multi-table commit is all-or-nothing (one bad requirement)")
def c1():
    a, b = "c1_a", "c1_b"
    make_table(a), make_table(b)
    br = "txn_c1"
    for t in (a, b):
        sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    wap(br)
    sql(f"INSERT INTO {fq(a)} VALUES (10,'x',1)")
    sql(f"INSERT INTO {fq(b)} VALUES (10,'x',1)")
    wap(None)
    ra, rb = refs(a), refs(b)
    bad = [
        change(NS, a, [assert_ref("main", ra["main"])], [set_ref("main", ra[br])]),
        change(NS, b, [assert_ref("main", 123456789)], [set_ref("main", rb[br])]),
    ]
    s_bad, body_bad = commit(bad)
    ra2, rb2 = refs(a), refs(b)
    untouched = ra2["main"] == ra["main"] and rb2["main"] == rb["main"]
    good = [
        change(NS, a, [assert_ref("main", ra["main"])], [set_ref("main", ra[br])]),
        change(NS, b, [assert_ref("main", rb["main"])], [set_ref("main", rb[br])]),
    ]
    s_good, body_good = commit(good)
    ra3, rb3 = refs(a), refs(b)
    published = ra3["main"] == ra[br] and rb3["main"] == rb[br]
    ok = s_bad == 409 and untouched and s_good in (200, 204) and published
    return ("PASS" if ok else "FAIL"), {
        "bad_commit_status": s_bad,
        "bad_commit_body": body_bad,
        "both_untouched_after_bad": untouched,
        "good_commit_status": s_good,
        "good_commit_body": body_good,
        "both_published_after_good": published,
    }


def _wap_ops(mor: bool):
    t = "c2_mor" if mor else "c2_cow"
    make_table(t, mor=mor)
    src = f"{CAT}.{NS}.{t}_src"
    sql(f"CREATE TABLE {src} (id BIGINT, v STRING, p INT) USING iceberg")
    sql(f"INSERT INTO {src} VALUES (1,'m',1),(99,'n',2)")
    br = "txn_c2"
    sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    main0 = refs(t)["main"]
    wap(br)
    df = spark.createDataFrame([(50, "df", 1)], "id BIGINT, v STRING, p INT")
    ops = {
        "INSERT INTO": lambda: sql(f"INSERT INTO {fq(t)} VALUES (5,'e',1)"),
        "UPDATE": lambda: sql(f"UPDATE {fq(t)} SET v='u' WHERE id=1"),
        "DELETE": lambda: sql(f"DELETE FROM {fq(t)} WHERE id=2"),
        "MERGE INTO": lambda: sql(
            f"MERGE INTO {fq(t)} t USING {src} s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT *"
        ),
        "INSERT OVERWRITE (dynamic)": lambda: (
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic"),
            sql(f"INSERT OVERWRITE {fq(t)} SELECT 7, 'o', 2"),
            spark.conf.unset("spark.sql.sources.partitionOverwriteMode"),
        ),
        "df.writeTo().append()": lambda: df.writeTo(fq(t)).append(),
        "df.writeTo().overwritePartitions()": lambda: df.writeTo(fq(t)).overwritePartitions(),
        "df.write.mode(append).saveAsTable()": lambda: df.write.format("iceberg").mode("append").saveAsTable(fq(t)),
    }
    per_op = {}
    for name, op in ops.items():
        before = refs(t)
        try:
            op()
            after = refs(t)
            per_op[name] = (
                "branch-only"
                if after["main"] == before["main"] and after.get(br) != before.get(br)
                else f"LEAKED main moved={after['main'] != before['main']} branch moved={after.get(br) != before.get(br)}"
            )
        except Exception as e:  # noqa: BLE001
            per_op[name] = f"ERROR {short(e)}"
    # read-your-writes: unqualified read in the WAP session should see the branch
    ryw_session = count(t)
    ryw_branch = count(t, br)
    wap(None)
    main_now = count(t)
    ok = all(v == "branch-only" for v in per_op.values()) and refs(t)["main"] == main0
    return ("PASS" if ok else "FAIL"), {
        "ops": per_op,
        "read_in_wap_session": ryw_session,
        "read_branch_explicit": ryw_branch,
        "read_main_after_unset": main_now,
        "read_your_writes": ryw_session == ryw_branch and ryw_session != main_now,
    }


@check("C2a", "WAP branch captures every write type (copy-on-write) + read-your-writes")
def c2a():
    return _wap_ops(mor=False)


@check("C2b", "WAP branch captures every write type (merge-on-read) + read-your-writes")
def c2b():
    return _wap_ops(mor=True)


@check("C3", "CREATE BRANCH on an empty table (no snapshots)")
def c3():
    t = "c3_empty"
    make_table(t, rows=False)
    try:
        sql(f"ALTER TABLE {fq(t)} CREATE BRANCH txn_c3")
        return "INFO", {"create_branch_on_empty": "works", "refs": refs(t)}
    except Exception as e:  # noqa: BLE001
        return "INFO", {"create_branch_on_empty": f"ERROR {short(e)}"}


@check("C4", "WAP-enabled table, branch does NOT exist: what does a write do?")
def c4():
    out = {}
    # non-empty table
    t = "c4_nobranch"
    make_table(t)
    main0 = refs(t)["main"]
    br = "txn_c4"
    wap(br)
    try:
        out["read_before_write"] = count(t)
    except Exception as e:  # noqa: BLE001
        out["read_before_write"] = f"ERROR {short(e)}"
    try:
        sql(f"INSERT INTO {fq(t)} VALUES (9,'z',1)")
        r = refs(t)
        out["write"] = "ok"
        out["main_moved"] = r["main"] != main0
        out["branch_created"] = br in r
        if br in r:
            snap = next(s for s in rest.load_table(NS, t)["snapshots"] if s["snapshot-id"] == r[br])
            out["branch_first_snapshot_parent_is_main_start"] = snap.get("parent-snapshot-id") == main0
    except Exception as e:  # noqa: BLE001
        out["write"] = f"ERROR {short(e)}"
    wap(None)
    # empty table
    t2 = "c4_empty_nobranch"
    make_table(t2, rows=False)
    wap(br)
    try:
        sql(f"INSERT INTO {fq(t2)} VALUES (9,'z',1)")
        r2 = refs(t2)
        out["empty_table_write"] = "ok"
        out["empty_table_refs"] = sorted(r2)
    except Exception as e:  # noqa: BLE001
        out["empty_table_write"] = f"ERROR {short(e)}"
    wap(None)
    return "INFO", out


@check("C5", "Table WITHOUT write.wap.enabled while spark.wap.branch is set")
def c5():
    t = "c5_nowap"
    make_table(t, wap=False)
    main0 = refs(t)["main"]
    wap("txn_c5")
    try:
        sql(f"INSERT INTO {fq(t)} VALUES (9,'z',1)")
        r = refs(t)
        res = {"write": "ok", "main_moved": r["main"] != main0, "refs": sorted(r)}
    except Exception as e:  # noqa: BLE001
        res = {"write": f"ERROR {short(e)}"}
    wap(None)
    return "INFO", res


@check("C6", "Read-only requirement (no updates) is enforced inside a multi-table commit")
def c6():
    r_, w = "c6_read", "c6_write"
    make_table(r_), make_table(w)
    br = "txn_c6"
    sql(f"ALTER TABLE {fq(w)} CREATE BRANCH {br}")
    wap(br)
    sql(f"INSERT INTO {fq(w)} VALUES (10,'x',1)")
    wap(None)
    rr, rw = refs(r_), refs(w)
    # someone else changes the read-only input
    sql(f"INSERT INTO {fq(r_)} VALUES (11,'y',1)")
    s_bad, body = commit([
        change(NS, r_, [assert_ref("main", rr["main"])], []),
        change(NS, w, [assert_ref("main", rw["main"])], [set_ref("main", rw[br])]),
    ])
    w_untouched = refs(w)["main"] == rw["main"]
    s_ok, _ = commit([
        change(NS, r_, [assert_ref("main", refs(r_)["main"])], []),
        change(NS, w, [assert_ref("main", rw["main"])], [set_ref("main", rw[br])]),
    ])
    ok = s_bad == 409 and w_untouched and s_ok in (200, 204)
    return ("PASS" if ok else "FAIL"), {
        "stale_read_commit_status": s_bad, "body": body,
        "write_table_untouched": w_untouched, "fresh_read_commit_status": s_ok,
    }


@check("C7", "Table creation (stage-create + assert-create) inside a multi-table commit")
def c7():
    other = "c7_other"
    make_table(other)
    br = "txn_c7"
    sql(f"ALTER TABLE {fq(other)} CREATE BRANCH {br}")
    wap(br)
    sql(f"INSERT INTO {fq(other)} VALUES (10,'x',1)")
    wap(None)
    ro = refs(other)
    schema = {"type": "struct", "schema-id": 0, "fields": [
        {"id": 1, "name": "id", "required": False, "type": "long"},
        {"id": 2, "name": "v", "required": False, "type": "string"},
    ]}

    def create_change(name: str) -> dict:
        md = rest.stage_create(NS, name, schema, {"format-version": "2"})["metadata"]
        cur_schema = next(s for s in md["schemas"] if s["schema-id"] == md["current-schema-id"])
        spec = next(s for s in md["partition-specs"] if s["spec-id"] == md["default-spec-id"])
        order = next(o for o in md["sort-orders"] if o["order-id"] == md["default-sort-order-id"])
        return change(NS, name, [{"type": "assert-create"}], [
            {"action": "assign-uuid", "uuid": md["table-uuid"]},
            {"action": "upgrade-format-version", "format-version": md["format-version"]},
            {"action": "add-schema", "schema": cur_schema, "last-column-id": md["last-column-id"]},
            {"action": "set-current-schema", "schema-id": -1},
            {"action": "add-spec", "spec": spec},
            {"action": "set-default-spec", "spec-id": -1},
            {"action": "add-sort-order", "sort-order": order},
            {"action": "set-default-sort-order", "sort-order-id": -1},
            {"action": "set-location", "location": md["location"]},
            {"action": "set-properties", "updates": md.get("properties", {})},
        ])

    out = {}
    try:
        s_bad, body_bad = commit([
            create_change("c7_new"),
            change(NS, other, [assert_ref("main", 123456789)], [set_ref("main", ro[br])]),
        ])
        out.update(bad_status=s_bad, bad_body=body_bad, new_table_exists_after_bad=rest.table_exists(NS, "c7_new"))
        s_ok, body_ok = commit([
            create_change("c7_new"),
            change(NS, other, [assert_ref("main", ro["main"])], [set_ref("main", ro[br])]),
        ])
        out.update(good_status=s_ok, good_body=body_ok, new_table_exists_after_good=rest.table_exists(NS, "c7_new"))
        if not out["new_table_exists_after_good"]:
            # Control: is the create payload itself valid? Send it through the single-table endpoint.
            ch = create_change("c7_new")
            try:
                r = rest.commit_table(NS, "c7_new", ch["requirements"], ch["updates"])
                out["single_table_create_status"] = r.status_code
            except RestError as e:
                out["single_table_create_status"] = f"{e.status} {e.body[:200]}"
            out["new_table_exists_after_single"] = rest.table_exists(NS, "c7_new")
        try:
            sql(f"INSERT INTO {fq('c7_new')} VALUES (1,'a')")
            out["spark_can_write_new_table"] = count("c7_new") == 1
        except Exception as e:  # noqa: BLE001
            out["spark_can_write_new_table"] = f"ERROR {short(e)}"
    except Exception as e:  # noqa: BLE001
        out["error"] = short(e)
        return "FAIL", out
    ok = (out.get("bad_status") == 409 and not out.get("new_table_exists_after_bad")
          and out.get("good_status") in (200, 204) and out.get("new_table_exists_after_good"))
    return ("PASS" if ok else "FAIL"), out


@check("C8", "assert-current-schema-id is enforced inside a multi-table commit")
def c8():
    s, w = "c8_schema", "c8_write"
    make_table(s), make_table(w)
    br = "txn_c8"
    sql(f"ALTER TABLE {fq(w)} CREATE BRANCH {br}")
    wap(br)
    sql(f"INSERT INTO {fq(w)} VALUES (10,'x',1)")
    wap(None)
    sid0 = rest.load_table(NS, s)["current-schema-id"]
    rw = refs(w)
    sql(f"ALTER TABLE {fq(s)} ADD COLUMN extra STRING")
    sid1 = rest.load_table(NS, s)["current-schema-id"]
    st, body = commit([
        change(NS, s, [{"type": "assert-current-schema-id", "current-schema-id": sid0}], []),
        change(NS, w, [assert_ref("main", rw["main"])], [set_ref("main", rw[br])]),
    ])
    untouched = refs(w)["main"] == rw["main"]
    ok = sid0 != sid1 and st == 409 and untouched
    return ("PASS" if ok else "FAIL"), {"schema_id_before": sid0, "schema_id_after": sid1,
                                        "commit_status": st, "body": body, "write_table_untouched": untouched}


@check("C9", "Additive DDL during a transaction: ADD COLUMN then write the new column on the branch")
def c9():
    t = "c9_ddl"
    make_table(t)
    br = "txn_c9"
    sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    r0 = refs(t)
    wap(br)
    out = {}
    try:
        sql(f"ALTER TABLE {fq(t)} ADD COLUMN extra STRING")
        sql(f"INSERT INTO {fq(t)} VALUES (10,'x',1,'new-col')")
        out["branch_sees_new_col"] = sql(f"SELECT count(*) FROM {fq(t)} WHERE extra = 'new-col'").collect()[0][0] == 1
        wap(None)
        out["main_unchanged_before_commit"] = refs(t)["main"] == r0["main"]
        out["main_schema_already_has_col"] = "extra" in sql(f"SELECT * FROM {fq(t)} LIMIT 0").columns
        r1 = refs(t)
        st, _ = commit([change(NS, t, [assert_ref("main", r0["main"])], [set_ref("main", r1[br])])])
        out["commit_status"] = st
        sql(f"REFRESH TABLE {fq(t)}")
        out["main_has_row_after_commit"] = sql(
            f"SELECT count(*) FROM {fq(t)} WHERE extra = 'new-col'").collect()[0][0] == 1
    except Exception as e:  # noqa: BLE001
        out["error"] = short(e)
    finally:
        wap(None)
    ok = out.get("branch_sees_new_col") and out.get("main_unchanged_before_commit") and out.get("main_has_row_after_commit")
    return ("PASS" if ok else "FAIL"), out


@check("C10", "Concurrent writer on main (other SparkSession) makes our commit fail")
def c10():
    t = "c10_conc"
    make_table(t)
    br = "txn_c10"
    sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    r0 = refs(t)
    wap(br)
    sql(f"INSERT INTO {fq(t)} VALUES (10,'x',1)")
    other = spark.newSession()
    other_has_wap = other.conf.get("spark.wap.branch", None)
    other.sql(f"INSERT INTO {fq(t)} VALUES (20,'other',2)")  # should hit main, not our branch
    wap(None)
    r1 = refs(t)
    other_went_to_main = r1["main"] != r0["main"]
    st, body = commit([change(NS, t, [assert_ref("main", r0["main"])], [set_ref("main", r1[br])])])
    ok = other_has_wap is None and other_went_to_main and st == 409 and refs(t)["main"] == r1["main"]
    return ("PASS" if ok else "FAIL"), {"other_session_wap_conf": other_has_wap, "other_write_went_to_main": other_went_to_main,
                                        "commit_status": st, "body": body}


@check("C11", "Spark metadata cache after an external (REST) commit")
def c11():
    t = "c11_cache"
    make_table(t)
    br = "txn_c11"
    sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    wap(br)
    sql(f"INSERT INTO {fq(t)} VALUES (10,'x',1),(11,'y',1)")
    wap(None)
    before = count(t)  # warms the cache on main
    r = refs(t)
    st, _ = commit([change(NS, t, [assert_ref("main", r["main"])], [set_ref("main", r[br])])])
    stale = count(t)
    sql(f"REFRESH TABLE {fq(t)}")
    refreshed = count(t)
    return "INFO", {"commit_status": st, "count_before": before, "count_right_after_commit": stale,
                    "count_after_REFRESH_TABLE": refreshed, "cache_is_stale": stale == before}


@check("C12", "Drop the transaction branch after publish; main data intact")
def c12():
    t = "c12_drop"
    make_table(t)
    br = "txn_c12"
    sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    wap(br)
    sql(f"INSERT INTO {fq(t)} VALUES (10,'x',1)")
    wap(None)
    r = refs(t)
    st, _ = commit([change(NS, t, [assert_ref("main", r["main"])], [set_ref("main", r[br])])])
    sql(f"ALTER TABLE {fq(t)} DROP BRANCH {br}")
    sql(f"REFRESH TABLE {fq(t)}")
    after = refs(t)
    ok = st in (200, 204) and br not in after and after["main"] == r[br] and count(t) == 5
    return ("PASS" if ok else "FAIL"), {"commit_status": st, "refs_after_drop": sorted(after), "count": count(t)}


@check("C13", "Abandoned transaction: drop branch without publishing leaves main untouched")
def c13():
    t = "c13_abort"
    make_table(t)
    r0 = refs(t)
    br = "txn_c13"
    sql(f"ALTER TABLE {fq(t)} CREATE BRANCH {br}")
    wap(br)
    sql(f"DELETE FROM {fq(t)} WHERE p = 1")
    wap(None)
    sql(f"ALTER TABLE {fq(t)} DROP BRANCH {br}")
    after = refs(t)
    ok = after == {"main": r0["main"]} and count(t) == 4
    return ("PASS" if ok else "FAIL"), {"refs_after": after, "count": count(t)}


# ----------------------------------------------------------------------------- runner


def main():
    print(f"Spark {spark.version} | namespace {NS} | REST prefix {rest.prefix!r}")
    sql(f"CREATE NAMESPACE {CAT}.{NS}")
    only = set(sys.argv[1:])
    for cid, title, fn in CHECKS:
        if only and cid not in only:
            continue
        t0 = time.time()
        try:
            status, details = fn()
        except Exception as e:  # noqa: BLE001
            status, details = "ERROR", {"exception": short(e), "trace": traceback.format_exc()[-1500:]}
        finally:
            spark.conf.unset("spark.wap.branch")
        RESULTS.append({"id": cid, "title": title, "status": status, "seconds": round(time.time() - t0, 1),
                        "details": details})
        print(f"\n[{status:5}] {cid} {title}")
        for k, v in details.items():
            if isinstance(v, dict):
                print(f"         {k}:")
                for k2, v2 in v.items():
                    print(f"           - {k2}: {v2}")
            else:
                print(f"         {k}: {v}")
    out = {"spark": spark.version, "namespace": NS, "run_id": str(uuid.uuid4()), "results": RESULTS}
    with open(os.path.join(os.path.dirname(__file__), "results.json"), "w") as f:
        json.dump(out, f, indent=2, default=str)
    print("\nSummary:", ", ".join(f"{r['id']}={r['status']}" for r in RESULTS))


if __name__ == "__main__":
    main()
