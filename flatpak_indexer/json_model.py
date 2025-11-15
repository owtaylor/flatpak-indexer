#!/usr/bin/python

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from typing import Any, Dict, Generic, Literal, Optional, TypeVar, Union, overload
import json

from .nvr import NVR
from .utils import format_date, parse_date, resolve_type


class Field:
    """
    Similar to the way that dataclasses.field works, assigning an instance of this
    to a model class field provides extra information beyond what the class
    annotation provides.
    """

    def __init__(self, *, index, json_name):
        self.indexed_field = index
        self.json_name = json_name


def field(index=None, json_name=None) -> Any:
    return Field(index=index, json_name=json_name)


T = TypeVar("T")


class ModelField(ABC, Generic[T]):
    def __init__(self, python_name: str, json_name: str, *, optional=False):
        self.python_name = python_name
        self.json_name = json_name
        self.optional = optional

    @abstractmethod
    def init_value(self, kwargs) -> T: ...

    @abstractmethod
    def json_include(self, instance) -> bool: ...

    @abstractmethod
    def json_value(self, instance) -> Any: ...

    @abstractmethod
    def python_value(self, data) -> T | None: ...


class ScalarField(ModelField[T]):
    @abstractmethod
    def to_json(self, value: T) -> Any: ...

    @abstractmethod
    def from_json(self, value: Any) -> T: ...

    def init_value(self, kwargs) -> T:
        value = kwargs.get(self.python_name)
        if value is None and not self.optional:
            raise AttributeError(f"{self.json_name} must be specified")

        return value

    def json_include(self, instance):
        if self.optional:
            return getattr(instance, self.python_name) is not None
        else:
            return True

    def json_value(self, instance):
        return self.to_json(getattr(instance, self.python_name))

    def python_value(self, data):
        value = data.get(self.json_name)
        if value is None:
            if not self.optional:
                raise ValueError(
                    f"{self.python_name} is not optional, but value is missing or null"
                )
            return None
        else:
            return self.from_json(value)


C = TypeVar("C", bound="BaseModel")


class ClassField(ScalarField[C]):
    def __init__(self, python_name: str, json_name: str, item_type: type[C], *, optional=False):
        super().__init__(python_name, json_name, optional=optional)
        self.item_type = item_type

    def to_json(self, value: C) -> Any:
        return value.to_json()

    def from_json(self, value: Any) -> C:
        return self.item_type.from_json(value)


def _make_field_type(
    to_json: Callable[[T], Any], from_json: Callable[[Any], T]
) -> type[ScalarField[T]]:
    # pyright does not like an inner class using a TypeVar
    # from the outer class, so we relax typing here and use ScalarField
    # rather than ScalarField[T]
    class _NewField(ScalarField):
        def to_json(self, value: T) -> Any:
            return to_json(value)

        def from_json(self, value: Any) -> T:
            return from_json(value)

    return _NewField


IntegerField = _make_field_type(int, int)
FloatField = _make_field_type(float, float)
StringField = _make_field_type(str, str)
BooleanField = _make_field_type(bool, bool)
DateTimeField = _make_field_type(format_date, parse_date)
NVRField = _make_field_type(str, NVR)


class CollectionField(ModelField[T]):
    def __init__(self, python_name, json_name, item_type, *, optional=False):
        if optional:
            raise TypeError(f"{python_name}: Optional[] cannot be used for collection fields")

        super().__init__(python_name, json_name)
        self.item_type = item_type

    @abstractmethod
    def make_empty_collection(self) -> T: ...

    def init_value(self, kwargs) -> T:
        try:
            return kwargs[self.python_name]
        except KeyError:
            return self.make_empty_collection()

    def json_include(self, instance):
        return bool(getattr(instance, self.python_name))


class ListField(CollectionField[list]):
    def make_empty_collection(self) -> list:
        return list()

    def json_value(self, instance):
        v = getattr(instance, self.python_name)

        if self.item_type is str or self.item_type is NVR:
            return [str(x) for x in v]
        else:
            return [x.to_json() for x in v]

    def python_value(self, data):
        try:
            v = data[self.json_name]
        except KeyError:
            return []

        if self.item_type is str:
            return [str(x) for x in v]
        elif self.item_type is NVR:
            return [NVR(x) for x in v]
        else:
            return [self.item_type.from_json(x) for x in v]


class IndexedListField(CollectionField[dict]):
    def __init__(self, python_name, json_name, item_type, indexed_field, *, optional=False):
        super().__init__(python_name, json_name, item_type, optional=optional)
        self.indexed_field = indexed_field

    def make_empty_collection(self) -> dict:
        return dict()

    def json_include(self, instance):
        return bool(getattr(instance, self.python_name))

    def json_value(self, instance):
        d = getattr(instance, self.python_name)

        return [
            x.to_json() for x in sorted(d.values(), key=lambda x: getattr(x, self.indexed_field))
        ]

    def python_value(self, data):
        try:
            raw_values = data[self.json_name]
        except KeyError:
            return {}

        values = (self.item_type.from_json(x) for x in raw_values)

        return {getattr(x, self.indexed_field): x for x in values}


class DictField(CollectionField[dict]):
    def make_empty_collection(self) -> dict:
        return dict()

    def json_include(self, instance):
        return bool(getattr(instance, self.python_name))

    def json_value(self, instance):
        d = getattr(instance, self.python_name)

        if self.item_type is str:
            return {str(k): str(v) for k, v in d.items()}
        else:
            return {str(k): v.to_json() for k, v in d.items()}

    def python_value(self, data):
        try:
            d = data[self.json_name]
        except KeyError:
            return {}

        if self.item_type is str:
            return {str(k): str(v) for k, v in d.items()}
        else:
            return {str(k): self.item_type.from_json(v) for k, v in d.items()}


def _make_model_field(name, type_, field_object):
    json_name = None
    indexed_field = None
    if field_object is not None:
        json_name = field_object.json_name
        indexed_field = field_object.indexed_field

    if json_name is None:
        json_name = "".join(x.capitalize() for x in name.split("_"))

    resolved, args, optional = resolve_type(type_)

    if indexed_field:
        if resolved is not dict or args[0] is not str:
            raise TypeError(f"{name}: field(index=<name>) can only be used with Dict[str]")

        return IndexedListField(name, json_name, args[1], indexed_field, optional=optional)

    if resolved is dict:
        if args[0] is not str:
            raise TypeError(f"{name}: Only Dict[str] is supported")
        return DictField(name, json_name, args[1], optional=optional)
    elif resolved is list:
        return ListField(name, json_name, args[0], optional=optional)
    elif issubclass(resolved, BaseModel):
        return ClassField(name, json_name, resolved, optional=optional)
    elif resolved is str:
        return StringField(name, json_name, optional=optional)
    elif resolved is int:
        return IntegerField(name, json_name, optional=optional)
    elif resolved is bool:
        return BooleanField(name, json_name, optional=optional)
    elif resolved is float:
        return FloatField(name, json_name, optional=optional)
    elif resolved is datetime:
        return DateTimeField(name, json_name, optional=optional)
    elif resolved is NVR:
        return NVRField(name, json_name, optional=optional)

    raise TypeError(f"{name}: Unsupported type {resolved}")


class BaseModelMeta(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)
        annotations = getattr(x, "__annotations__", None)

        if annotations:
            fields = {
                k: _make_model_field(k, v, getattr(x, k, None)) for k, v in annotations.items()
            }
        else:
            fields = {}

        for superclass in x.__mro__:
            if superclass is x:
                continue

            superfields = getattr(superclass, "__fields__", None)
            if superfields:
                for k, v in superfields.items():
                    if k not in fields:
                        fields[k] = v

        setattr(x, "__fields__", fields)
        return x


M = TypeVar("M", bound="BaseModel")


class BaseModel(metaclass=BaseModelMeta):
    def __init__(self, **kwargs):
        for field in self.__class__._fields().values():
            setattr(self, field.python_name, field.init_value(kwargs))

    @classmethod
    def _fields(cls) -> Dict[str, ModelField]:
        return getattr(cls, "__fields__")

    def to_json(self) -> Dict[str, Any]:
        return {
            field.json_name: field.json_value(self)
            for field in self._fields().values()
            if field.json_include(self)
        }

    def to_json_text(self):
        return json.dumps(self.to_json())

    @classmethod
    def class_from_json(cls: type[M], data: Any) -> type[M]:
        # Perhaps this would be better if a type-tag-field was required
        # with declarative tag => subclass.
        """Returns the appropriate subclass to instantiate for the data"""
        return cls

    @classmethod
    def check_json_current(cls, data: Any) -> bool:
        """
        Checks if the value of data is considered 'current'

        An application can pass check_current=True to cls.from_json() or
        cls.from_json_text() to make from_json() return None for not-current items.
        The idea here is schema-migration: the application can refetch and
        recache such items.

        For performance reasons, this isn't called at all unless check_current
        is passed - there is no assertion in the check_current=False case - we
        assume that demarshaling will fail.
        """
        return True

    @overload
    @classmethod
    def from_json(cls: type[M], data: Any, check_current: Literal[True]) -> Optional[M]: ...

    @overload
    @classmethod
    def from_json(cls: type[M], data: Any, check_current: Literal[False] = False) -> M: ...

    @classmethod
    def from_json(cls: type[M], data, check_current: bool = False) -> Optional[M]:
        if check_current and not cls.check_json_current(data):
            return None

        cls = cls.class_from_json(data)

        result = cls.__new__(cls)
        for field in cls._fields().values():
            setattr(result, field.python_name, field.python_value(data))

        return result

    @overload
    @classmethod
    def from_json_text(
        cls: type[M], text: Union[str, bytes], check_current: Literal[True]
    ) -> Optional[M]: ...

    @overload
    @classmethod
    def from_json_text(
        cls: type[M], text: Union[str, bytes], check_current: Literal[False] = False
    ) -> M: ...

    @classmethod
    def from_json_text(cls: type[M], text: Union[str, bytes], check_current: bool = False):
        if check_current:
            return cls.from_json(json.loads(text), check_current=True)
        else:
            return cls.from_json(json.loads(text), check_current=False)
