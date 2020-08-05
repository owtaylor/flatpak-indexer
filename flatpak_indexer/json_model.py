#!/usr/bin/python

from datetime import datetime
import json

from .utils import format_date, parse_date


class RenameAlias:
    def __init__(self, origin, json_name):
        self.origin = origin
        self.json_name = json_name


class RenameMeta(type):
    def __getitem__(self, params):
        return RenameAlias(params[0], params[1])


class Rename(metaclass=RenameMeta):
    pass


class IndexedListAlias:
    def __init__(self, origin, indexed_field):
        self.origin = origin
        self.indexed_field = indexed_field


class IndexedListMeta(type):
    def __getitem__(self, params):
        return IndexedListAlias(params[0], params[1])


class IndexedList(metaclass=IndexedListMeta):
    pass


class ModelField:
    def __init__(self, python_name, json_name):
        self.python_name = python_name
        self.json_name = json_name

    def init_value(self, kwargs):
        return kwargs[self.python_name]

    def json_include(self, instance):
        return True


class StringField(ModelField):
    def json_value(self, instance):
        return str(getattr(instance, self.python_name))

    def python_value(self, data):
        return str(data[self.json_name])


class DateTimeField(ModelField):
    # We need to be able to represent null dates for Bodhi's date_testing/date_stable
    # We do this by making all dates able to be null but not missing. This probably
    # should be made more similar to the the handling of empty lists/sets where
    # empty is the same as missing.

    def json_value(self, instance):
        v = getattr(instance, self.python_name)
        if v:
            return format_date(getattr(instance, self.python_name))
        else:
            return None

    def python_value(self, data):
        v = data[self.json_name]
        if v:
            return parse_date(data[self.json_name])
        else:
            return None


class ListField(ModelField):
    def __init__(self, python_name, json_name, item_type):
        super().__init__(python_name, json_name)
        self.item_type = item_type

    def init_value(self, kwargs):
        try:
            return kwargs[self.python_name]
        except KeyError:
            return []

    def json_include(self, instance):
        return bool(getattr(instance, self.python_name))

    def json_value(self, instance):
        v = getattr(instance, self.python_name)

        if self.item_type == str:
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
        else:
            return [
                self.item_type.from_json(x) for x in v
            ]


class IndexedListField(ModelField):
    def __init__(self, python_name, json_name, item_type, indexed_field):
        super().__init__(python_name, json_name)
        self.item_type = item_type
        self.indexed_field = indexed_field

    def init_value(self, kwargs):
        try:
            return kwargs[self.python_name]
        except KeyError:
            return {}

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


class DictField(ModelField):
    def __init__(self, python_name, json_name, item_type):
        super().__init__(python_name, json_name)
        self.item_type = item_type

    def init_value(self, kwargs):
        try:
            return kwargs[self.python_name]
        except KeyError:
            return {}

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


def _make_model_field(name, type_):
    if isinstance(type_, RenameAlias):
        json_name = type_.json_name
        type_ = type_.origin
    else:
        json_name = ''.join(x.capitalize() for x in name.split('_'))

    if isinstance(type_, IndexedListAlias):
        return IndexedListField(name, json_name, type_.origin, type_.indexed_field)

    origin = getattr(type_, '__origin__', None)
    if origin == dict:
        if type_.__args__[0] != str:
            raise TypeError(f"{name}: Only dict[str] is supported")
        return DictField(name, json_name, type_.__args__[1])
    elif origin == list:
        return ListField(name, json_name, type_.__args__[0])
    elif origin is None:
        if type_ == str:
            return StringField(name, json_name)
        elif type_ == datetime:
            return DateTimeField(name, json_name)

    raise TypeError(f"{name}: Unsupported type {type_}")


class BaseModelMeta(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)
        annotations = getattr(x, '__annotations__', None)
        if annotations:
            x.__fields__ = {k: _make_model_field(k, v) for k, v in annotations.items()}
        else:
            x.__fields__ = {}
        return x


class BaseModel(metaclass=BaseModelMeta):
    def __init__(self, **kwargs):
        for field in self.__fields__.values():
            setattr(self, field.python_name, field.init_value(kwargs))

    def to_json(self):
        return {
            field.json_name: field.json_value(self)
            for field in self.__fields__.values()
            if field.json_include(self)
        }

    @classmethod
    def from_json(cls, data):
        return cls(**{
            field.python_name: field.python_value(data) for field in cls.__fields__.values()
        })
