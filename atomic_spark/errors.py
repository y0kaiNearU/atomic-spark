"""Exception hierarchy for atomic-spark."""

from __future__ import annotations


class AtomicSparkError(Exception):
    """Base class for every error raised by atomic-spark."""


class TransactionUsageError(AtomicSparkError):
    """The library was used incorrectly (nesting, bad arguments, misconfigured catalog)."""


class WapNotEnabledError(AtomicSparkError):
    """A declared table does not have write.wap.enabled=true, so its writes would bypass the transaction."""

    def __init__(self, tables: list[str]):
        self.tables = tables
        super().__init__(
            "Tables without write.wap.enabled=true would be written straight to main, outside the "
            f"transaction: {', '.join(tables)}. Set the property, or pass enable_wap=True."
        )


class ConflictError(AtomicSparkError):
    """Another writer changed a table (or a validated read table) during the transaction.

    Nothing was published. Re-running the job is safe; the decorator form does this with retries=N.
    """


class DestructiveSchemaChangeError(AtomicSparkError):
    """A declared table's schema changed destructively during the transaction. Nothing was published."""

    def __init__(self, table: str, changes: list[str]):
        self.table = table
        self.changes = changes
        super().__init__(
            f"Destructive schema change on {table} during the transaction: {'; '.join(changes)}. "
            "Nothing was published, but the schema change itself is already applied and is NOT rolled back."
        )


class CommitFailedError(AtomicSparkError):
    """The catalog definitively rejected the commit. Nothing was published."""


class CommitStateUnknownError(AtomicSparkError):
    """It could not be determined whether the commit was applied.

    The transaction branches are kept so the state can be inspected and repaired by hand.
    """

    def __init__(self, message: str, branch: str, tables: dict[str, dict]):
        self.branch = branch
        self.tables = tables
        super().__init__(f"{message} Branch {branch!r} was kept on every table for inspection: {tables}")


class CatalogError(AtomicSparkError):
    """An Iceberg REST call returned an error response."""

    def __init__(self, status: int, error_type: str | None, message: str, body: str = ""):
        self.status = status
        self.error_type = error_type
        self.body = body
        super().__init__(f"HTTP {status} {error_type or ''}: {message}".strip())
