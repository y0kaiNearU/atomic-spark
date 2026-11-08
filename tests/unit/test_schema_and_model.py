from __future__ import annotations

import pytest

from atomic_spark.errors import TransactionUsageError
from atomic_spark.model import TableRef
from atomic_spark.schema import destructive_changes, is_safe_type_change


def schema(*fields):
    return {"type": "struct", "schema-id": 0, "fields": list(fields)}


def f(fid, name, typ, required=False):
    return {"id": fid, "name": name, "type": typ, "required": required}


def test_added_column_is_not_destructive():
    assert destructive_changes(schema(f(1, "a", "long")), schema(f(1, "a", "long"), f(2, "b", "string"))) == []


@pytest.mark.parametrize("old,new,msg", [
    (schema(f(1, "a", "long"), f(2, "b", "string")), schema(f(1, "a", "long")), "dropped column b"),
    (schema(f(1, "a", "long")), schema(f(1, "x", "long")), "renamed column a -> x"),
    (schema(f(1, "a", "long")), schema(f(1, "a", "string")), "changed type of a from long to string"),
    (schema(f(1, "a", "long")), schema(f(1, "a", "long", required=True)), "made column a required"),
])
def test_destructive_changes(old, new, msg):
    assert destructive_changes(old, new) == [msg]


def test_safe_widening_and_relaxing():
    old = schema(f(1, "a", "int", required=True), f(2, "d", "decimal(10,2)"))
    new = schema(f(1, "a", "long"), f(2, "d", "decimal(12, 2)"))
    assert destructive_changes(old, new) == []
    assert not is_safe_type_change("decimal(10,2)", "decimal(12,3)")


def test_nested_fields_are_tracked():
    struct = {"type": "struct", "fields": [f(3, "x", "int"), f(4, "y", "int")]}
    lst = {"type": "list", "element-id": 6, "element": "string", "element-required": False}
    old = schema(f(1, "s", struct), f(5, "l", lst))
    new_struct = {"type": "struct", "fields": [f(3, "x", "int")]}
    new_lst = {"type": "list", "element-id": 6, "element": "int", "element-required": False}
    new = schema(f(1, "s", new_struct), f(5, "l", new_lst))
    assert destructive_changes(old, new) == ["dropped column s.y", "changed type of l.element from string to int"]


@pytest.mark.parametrize("text,ns,name", [
    ("db.t", ("db",), "t"),
    ("db.sub.t", ("db", "sub"), "t"),
    ("polaris.db.t", ("db",), "t"),
    ("`db`.`t`", ("db",), "t"),
])
def test_table_ref_parse(text, ns, name):
    assert TableRef.parse(text, "polaris") == TableRef(ns, name)


@pytest.mark.parametrize("text", ["t", "db..t", ""])
def test_table_ref_parse_rejects(text):
    with pytest.raises(TransactionUsageError):
        TableRef.parse(text, "polaris")


def test_spark_name_quotes_parts():
    assert TableRef(("my db",), "t`x").spark_name("polaris") == "`polaris`.`my db`.`t``x`"
