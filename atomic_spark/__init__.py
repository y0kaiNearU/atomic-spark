"""atomic-spark: all-or-nothing multi-table Spark writes on Iceberg REST catalogs (e.g. Apache Polaris)."""

from .api import atomic
from .errors import (
    AtomicSparkError,
    CatalogError,
    CommitFailedError,
    CommitStateUnknownError,
    ConflictError,
    DestructiveSchemaChangeError,
    TransactionUsageError,
    WapNotEnabledError,
)
from .transaction import State, Transaction

__all__ = [
    "atomic",
    "Transaction",
    "State",
    "AtomicSparkError",
    "CatalogError",
    "CommitFailedError",
    "CommitStateUnknownError",
    "ConflictError",
    "DestructiveSchemaChangeError",
    "TransactionUsageError",
    "WapNotEnabledError",
]

__version__ = "0.1.0"
