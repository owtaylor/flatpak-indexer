from datetime import datetime, timezone
import pytest
from typing import List, Dict

from flatpak_indexer.json_model import BaseModel, IndexedList, Rename


class StringStuff(BaseModel):
    f1: str


def test_string_field():
    obj = StringStuff(f1="foo")
    JSON = {"F1": "foo"}

    assert obj.to_json() == JSON

    from_json = StringStuff.from_json(JSON)
    assert from_json.f1 == "foo"


class DateTimeStuff(BaseModel):
    f1: datetime


def test_datetime_field():
    obj = DateTimeStuff(f1=datetime(year=2020, month=8, day=13,
                                    hour=1, minute=2, second=3,
                                    tzinfo=timezone.utc))
    JSON = {"F1": "2020-08-13T01:02:03.000000+00:00"}

    assert obj.to_json() == JSON

    from_json = DateTimeStuff.from_json(JSON)
    assert from_json.f1 == obj.f1


def test_datetime_field_null():
    obj = DateTimeStuff(f1=None)
    JSON = {"F1": None}

    assert obj.to_json() == JSON

    from_json = DateTimeStuff.from_json(JSON)
    assert from_json.f1 is None


class ListStuff(BaseModel):
    f1: List[str]
    f2: List[StringStuff]


def test_list_field():
    obj = ListStuff(f1=["foo"], f2=[StringStuff(f1="foo")])
    JSON = {
        "F1": ["foo"],
        "F2": [
            {"F1": "foo"}
        ]
    }

    assert obj.to_json() == JSON

    from_json = ListStuff.from_json(JSON)
    assert from_json.f1 == ["foo"]
    assert from_json.f2[0].f1 == "foo"

    obj = ListStuff()
    assert obj.f1 == []

    obj = ListStuff.from_json({})
    assert obj.f1 == []


class IndexedListStuff(BaseModel):
    f1: IndexedList[StringStuff, "f1"]  # noqa: F821


def test_indexed_list_field():
    obj = IndexedListStuff(f1={"foo": StringStuff(f1="foo"), "bar": StringStuff(f1="bar")})
    JSON = {
        "F1": [
            {"F1": "bar"},
            {"F1": "foo"}
        ]
    }

    assert obj.to_json() == JSON

    from_json = IndexedListStuff.from_json(JSON)
    assert from_json.f1["foo"].f1 == "foo"

    obj = IndexedListStuff()
    assert obj.f1 == {}

    obj = IndexedListStuff.from_json({})
    assert obj.f1 == {}


class DictStuff(BaseModel):
    f1: Dict[str, str]
    f2: Dict[str, StringStuff]


def test_dict_field():
    obj = DictStuff(f1={"a": "foo"}, f2={"a": StringStuff(f1="foo")})
    JSON = {"F1": {"a": "foo"}, "F2": {"a": {"F1": "foo"}}}

    assert obj.to_json() == JSON

    from_json = DictStuff.from_json(JSON)
    assert from_json.f1 == {"a": "foo"}
    assert from_json.f2["a"].f1 == "foo"

    obj = DictStuff()
    assert obj.f1 == {}

    obj = DictStuff.from_json({})
    assert obj.f1 == {}


class NameStuff(BaseModel):
    foo_bar: str
    os: Rename[str, "OS"]  # noqa: F821


def test_field_names():
    obj = NameStuff(foo_bar="a", os="linux")

    assert obj.to_json() == {
        "FooBar": "a",
        "OS": "linux"
    }


def test_unexpected_types():
    with pytest.raises(TypeError, match=r"Only dict\[str\] is supported"):
        class A(BaseModel):
            f: Dict[int, int]

    with pytest.raises(TypeError, match=r"Unsupported type"):
        class B(BaseModel):
            f: set


def test_to_json_text():
    obj = StringStuff(f1="foo")
    new_obj = obj.from_json_text(obj.to_json_text())
    assert obj.f1 == new_obj.f1
