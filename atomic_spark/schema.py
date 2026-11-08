"""Classifies Iceberg schema changes as additive (allowed) or destructive (fails the commit)."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class FieldInfo:
    path: str
    type: str
    required: bool


def flatten(schema: dict) -> dict[int, FieldInfo]:
    """Map every field id (including nested, list-element and map key/value ids) to its info."""
    out: dict[int, FieldInfo] = {}
    _walk(schema, (), out)
    return out


def _type_repr(t) -> str:
    return t if isinstance(t, str) else t["type"]


def _walk(t, path: tuple[str, ...], out: dict[int, FieldInfo]) -> None:
    if not isinstance(t, dict):
        return
    kind = t.get("type")
    if kind == "struct":
        for f in t.get("fields", []):
            p = (*path, f["name"])
            out[f["id"]] = FieldInfo(".".join(p), _type_repr(f["type"]), bool(f.get("required", False)))
            _walk(f["type"], p, out)
    elif kind == "list":
        p = (*path, "element")
        out[t["element-id"]] = FieldInfo(".".join(p), _type_repr(t["element"]), bool(t.get("element-required", False)))
        _walk(t["element"], p, out)
    elif kind == "map":
        kp, vp = (*path, "key"), (*path, "value")
        out[t["key-id"]] = FieldInfo(".".join(kp), _type_repr(t["key"]), True)
        out[t["value-id"]] = FieldInfo(".".join(vp), _type_repr(t["value"]), bool(t.get("value-required", False)))
        _walk(t["key"], kp, out)
        _walk(t["value"], vp, out)


_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)")
_WIDENINGS = {("int", "long"), ("float", "double")}


def is_safe_type_change(old: str, new: str) -> bool:
    if old == new or (old, new) in _WIDENINGS:
        return True
    m_old, m_new = _DECIMAL.fullmatch(old), _DECIMAL.fullmatch(new)
    if m_old and m_new:
        return m_old.group(2) == m_new.group(2) and int(m_new.group(1)) >= int(m_old.group(1))
    return False


def destructive_changes(old_schema: dict, new_schema: dict) -> list[str]:
    """Describe every change from old to new that can lose or reinterpret existing data.

    Allowed (not reported): new columns, safe type widening, required -> optional.
    Reported: dropped columns, renames, unsafe type changes, optional -> required.
    """
    old, new = flatten(old_schema), flatten(new_schema)
    changes: list[str] = []
    for fid, f in old.items():
        n = new.get(fid)
        if n is None:
            changes.append(f"dropped column {f.path}")
            continue
        if n.path != f.path:
            changes.append(f"renamed column {f.path} -> {n.path}")
        if not is_safe_type_change(f.type, n.type):
            changes.append(f"changed type of {n.path} from {f.type} to {n.type}")
        if n.required and not f.required:
            changes.append(f"made column {n.path} required")
    return changes
