"""Table identifiers and per-table transaction state."""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import TransactionUsageError


@dataclass(frozen=True)
class TableRef:
    """An Iceberg table inside one catalog: namespace levels plus a table name."""

    namespace: tuple[str, ...]
    name: str

    @classmethod
    def parse(cls, text: str, catalog: str) -> "TableRef":
        """Parse "db.table", "db.sub.table" or "<catalog>.db.table" (the catalog prefix is stripped)."""
        parts = [p.strip().strip("`") for p in text.split(".")]
        if any(not p for p in parts):
            raise TransactionUsageError(f"Invalid table name {text!r}")
        if len(parts) >= 3 and parts[0] == catalog:
            parts = parts[1:]
        if len(parts) < 2:
            raise TransactionUsageError(f"Table name {text!r} needs a namespace, e.g. 'db.{text}'")
        return cls(tuple(parts[:-1]), parts[-1])

    def __str__(self) -> str:
        return ".".join((*self.namespace, self.name))

    def spark_name(self, catalog: str) -> str:
        return ".".join(_quote(p) for p in (catalog, *self.namespace, self.name))

    def rest_identifier(self) -> dict:
        return {"namespace": list(self.namespace), "name": self.name}


def _quote(part: str) -> str:
    return "`" + part.replace("`", "``") + "`"


@dataclass
class TableState:
    """What the transaction recorded about a table when it began."""

    ref: TableRef
    uuid: str
    start_main: int | None  # main's snapshot at begin; None for a table with no snapshots
    branch_base: int | None  # snapshot the transaction branch was created at; None = created lazily on first write
    start_schema_id: int
    start_schema: dict = field(repr=False)


def main_snapshot(metadata: dict) -> int | None:
    ref = (metadata.get("refs") or {}).get("main")
    if ref is not None:
        return ref["snapshot-id"]
    sid = metadata.get("current-snapshot-id")
    return None if sid in (None, -1) else sid


def ref_snapshot(metadata: dict, name: str) -> int | None:
    ref = (metadata.get("refs") or {}).get(name)
    return None if ref is None else ref["snapshot-id"]


def current_schema(metadata: dict) -> dict:
    sid = metadata["current-schema-id"]
    for s in metadata.get("schemas", []):
        if s.get("schema-id") == sid:
            return s
    raise ValueError(f"Schema {sid} not found in table metadata")
