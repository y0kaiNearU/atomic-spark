"""Public entry point: atomic(...) as a context manager or as a retrying decorator."""

from __future__ import annotations

import functools
import logging
import random
import time
from typing import Callable, Iterable, TypeVar

from .errors import ConflictError, TransactionUsageError
from .transaction import Transaction

log = logging.getLogger("atomic_spark")
T = TypeVar("T")


class atomic:  # noqa: N801 - reads like a keyword: `with atomic(...)`
    """Run Spark writes to several Iceberg tables as one all-or-nothing transaction.

    Context manager (no retries; a with-block cannot re-run its own body)::

        with atomic(spark, "polaris", tables=["db.orders", "db.customers"]) as tx:
            spark.sql("MERGE INTO polaris.db.orders ...")

    Decorator (re-runs the whole function on ConflictError, up to `retries` times)::

        @atomic(spark, "polaris", tables=["db.orders"], retries=3)
        def nightly_load(): ...

    tables       every table the block writes to (required; writes to unlisted tables bypass the transaction)
    read_tables  tables the block only reads; the commit fails if they changed meanwhile
    enable_wap   set write.wap.enabled=true on listed tables that lack it (otherwise WapNotEnabledError)
    """

    def __init__(
        self,
        spark,
        catalog: str,
        tables: Iterable[str],
        *,
        read_tables: Iterable[str] = (),
        retries: int = 0,
        retry_wait: float = 1.0,
        **transaction_options,
    ):
        if retries < 0:
            raise TransactionUsageError("retries must be >= 0")
        self.spark = spark
        self.catalog = catalog
        self.tables = list(tables) if not isinstance(tables, str) else tables
        self.read_tables = list(read_tables) if not isinstance(read_tables, str) else read_tables
        self.retries = retries
        self.retry_wait = retry_wait
        self.options = transaction_options
        self._tx: Transaction | None = None

    def _new_transaction(self) -> Transaction:
        return Transaction(self.spark, self.catalog, self.tables, read_tables=self.read_tables, **self.options)

    # -- context manager ------------------------------------------------------
    def __enter__(self) -> Transaction:
        if self.retries:
            raise TransactionUsageError("retries= only works with the decorator form; a with-block cannot re-run its body")
        if self._tx is not None:
            raise TransactionUsageError("This atomic(...) object is already in use")
        self._tx = self._new_transaction()
        try:
            return self._tx.begin()
        except BaseException:
            self._tx = None
            raise

    def __exit__(self, exc_type, exc, tb) -> bool:
        tx, self._tx = self._tx, None
        if exc_type is None:
            tx.commit()
        else:
            _safe_rollback(tx)
        return False

    # -- decorator ------------------------------------------------------------
    def __call__(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            attempt = 0
            while True:
                try:
                    return self._run_once(fn, args, kwargs)
                except ConflictError as e:
                    if attempt >= self.retries:
                        raise
                    wait = self.retry_wait * (2 ** attempt) * random.uniform(0.8, 1.2)
                    attempt += 1
                    log.warning("atomic-spark: conflict (%s); retry %d/%d in %.1fs", e, attempt, self.retries, wait)
                    time.sleep(wait)

        return wrapper

    def _run_once(self, fn, args, kwargs):
        tx = self._new_transaction().begin()
        try:
            result = fn(*args, **kwargs)
        except BaseException:
            _safe_rollback(tx)
            raise
        tx.commit()
        return result


def _safe_rollback(tx: Transaction) -> None:
    try:
        tx.rollback()
    except Exception:  # noqa: BLE001 - never mask the user's exception
        log.exception("atomic-spark: rollback of %s failed", tx.branch)
