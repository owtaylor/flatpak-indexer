#!/usr/bin/python

from datetime import datetime
import json
from typing import Any, Dict, Literal, Optional, overload, TypeVar, Union

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


class ModelField:
    def __init__(self, python_name, json_name, *, optional=False):
        self.python_name = python_name
        self.json_name = json_name
        self.optional = optional

    # This is a workaround to allow to_json/from_json to either be
    # a method or a type like 'int' without creating type warnings
    def _unimplemented(self, value):
        raise NotImplementedError()

    to_json: Any = _unimplemented
    from_json: Any = _unimplemented

    def init_value(self, kwargs):
        raise NotImplementedError()

    def json_include(self, instance):
        raise NotImplementedError()

    def json_value(self, instance):
        raise NotImplementedError()

    def python_value(self, data):
        raise NotImplementedError()


class ScalarField(ModelField):
    def init_value(self, kwargs):
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
                    f"{self.python_name} is not optional, but value is missing or null")
            return None
        else:
            return self.from_json(value)


class ClassField(ScalarField):
    def __init__(self, python_name, json_name, item_type, *, optional=False):
        super().__init__(python_name, json_name, optional=optional)
        self.item_type = item_type

    def to_json(self, value):
        return value.to_json()

    def from_json(self, value):
        return self.item_type.from_json(value)


class IntegerField(ScalarField):
    to_json = int
    from_json = int


class FloatField(ScalarField):
    to_json = float
    from_json = float


class StringField(ScalarField):
    to_json = str
    from_json = str


class BooleanField(ScalarField):
    to_json = bool
    from_json = bool


class DateTimeField(ScalarField):
    to_json = staticmethod(format_date)
    from_json = staticmethod(parse_date)


class NVRField(ScalarField):
    to_json = str
    from_json = NVR


class CollectionField(ModelField):
    def __init__(self, python_name, json_name, item_type, *, optional=False):
        if optional:
            raise TypeError(f"{python_name}: Optional[] cannot be used for collection fields")

        super().__init__(python_name, json_name)
        self.item_type = item_type

    @staticmethod
    def collection_type():
        raise NotImplementedError()

    def init_value(self, kwargs):
        try:
            return kwargs[self.python_name]
        except KeyError:
            return self.collection_type()

    def json_include(self, instance):
        return bool(getattr(instance, self.python_name))


class ListField(CollectionField):
    collection_type = list

    def json_value(self, instance):
        v = getattr(instance, self.python_name)

        if self.item_type == str or self.item_type == NVR:
            return [
                str(x) for x in v
            ]
        else:
            return [
                x.to_json() for x in v
            ]

    def python_value(self, data):
        try:
            v = data[self.json_name]
        except KeyError:
            return []

        if self.item_type == str:
            return [
                str(x) for x in v
            ]
        elif self.item_type == NVR:
            return [
                NVR(x) for x in v
            ]
        else:
            return [
                self.item_type.from_json(x) for x in v
            ]


class IndexedListField(CollectionField):
    collection_type = dict

    def __init__(self, python_name, json_name, item_type, indexed_field, *, optional=False):
        super().__init__(python_name, json_name, item_type, optional=optional)
        self.indexed_field = indexed_field

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

        values = (
            self.item_type.from_json(x) for x in raw_values
        )

        return {
            getattr(x, self.indexed_field): x for x in values
        }


class DictField(CollectionField):
    collection_type = dict

    def json_include(self, instance):
        return bool(getattr(instance, self.python_name))

    def json_value(self, instance):
        d = getattr(instance, self.python_name)

        if self.item_type == str:
            return {
                str(k): str(v) for k, v in d.items()
            }
        else:
            return {
                str(k): v.to_json() for k, v in d.items()
            }

    def python_value(self, data):
        try:
            d = data[self.json_name]
        except KeyError:
            return {}

        if self.item_type == str:
            return {
                str(k): str(v) for k, v in d.items()
            }
        else:
            return {
                str(k): self.item_type.from_json(v) for k, v in d.items()
            }


def _make_model_field(name, type_, field_object):
    json_name = None
    indexed_field = None
    if field_object is not None:
        json_name = field_object.json_name
        indexed_field = field_object.indexed_field

    if json_name is None:
        json_name = ''.join(x.capitalize() for x in name.split('_'))

    resolved, args, optional = resolve_type(type_)

    if indexed_field:
        if resolved != dict or args[0] != str:
            raise TypeError(f"{name}: field(index=<name>) can only be used with Dict[str]")

        return IndexedListField(name, json_name, args[1], indexed_field,
                                optional=optional)

    if resolved == dict:
        if args[0] != str:
            raise TypeError(f"{name}: Only Dict[str] is supported")
        return DictField(name, json_name, args[1], optional=optional)
    elif resolved == list:
        return ListField(name, json_name, args[0], optional=optional)
    elif issubclass(resolved, BaseModel):
        return ClassField(name, json_name, resolved, optional=optional)
    elif resolved == str:
        return StringField(name, json_name, optional=optional)
    elif resolved == int:
        return IntegerField(name, json_name, optional=optional)
    elif resolved == bool:
        return BooleanField(name, json_name, optional=optional)
    elif resolved == float:
        return FloatField(name, json_name, optional=optional)
    elif resolved == datetime:
        return DateTimeField(name, json_name, optional=optional)
    elif resolved == NVR:
        return NVRField(name, json_name, optional=optional)

    raise TypeError(f"{name}: Unsupported type {resolved}")


class BaseModelMeta(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)
        annotations = getattr(x, '__annotations__', None)

        if annotations:
            fields = {k: _make_model_field(k, v, getattr(x, k, None))
                      for k, v in annotations.items()}
        else:
            fields = {}

        for superclass in x.__mro__:
            if superclass is x:
                continue

            superfields = getattr(superclass, '__fields__', None)
            if superfields:
                for k, v in superfields.items():
                    if k not in fields:
                        fields[k] = v

        setattr(x, '__fields__', fields)
        return x


M = TypeVar('M', bound='BaseModel')


class BaseModel(metaclass=BaseModelMeta):
    def __init__(self, **kwargs):
        for field in self.__class__._fields().values():
            setattr(self, field.python_name, field.init_value(kwargs))

    @classmethod
    def _fields(cls) -> Dict[str, ModelField]:
        return getattr(cls, '__fields__')

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
    def from_json(cls: type[M], data: Any) -> M: ...

    @overload
    @classmethod
    def from_json(cls: type[M], data: Any, check_current: Literal[True] = ...) -> Optional[M]: ...

    @overload
    @classmethod
    def from_json(cls: type[M], data: Any, check_current: Literal[False] = ...) -> M: ...

    @classmethod
    def from_json(cls, data, check_current: bool = False):
        if check_current and not cls.check_json_current(data):
            return None

        cls = cls.class_from_json(data)

        result = cls.__new__(cls)
        for field in cls._fields().values():
            setattr(result, field.python_name, field.python_value(data))

        return result

    @overload
    @classmethod
    def from_json_text(cls: type[M], text: Union[str, bytes]) -> M: ...

    @overload
    @classmethod
    def from_json_text(
        cls: type[M], text: Union[str, bytes], check_current: Literal[True] = ...
    ) -> Optional[M]: ...

    @overload
    @classmethod
    def from_json_text(
        cls: type[M], text: Union[str, bytes], check_current: Literal[False] = ...
    ) -> M: ...

    @classmethod
    def from_json_text(cls: type[M], text: Union[str, bytes], check_current: bool = False):
        if check_current:
            return cls.from_json(json.loads(text), check_current=True)
        else:
            return cls.from_json(json.loads(text), check_current=False)
