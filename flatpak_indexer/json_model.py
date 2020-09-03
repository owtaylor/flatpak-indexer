#!/usr/bin/python

from datetime import datetime
import json
import typing

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
    def __init__(self, python_name, json_name, *, optional=False):
        self.python_name = python_name
        self.json_name = json_name
        self.optional = optional


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


class DateTimeField(ScalarField):
    to_json = staticmethod(format_date)
    from_json = staticmethod(parse_date)


class CollectionField(ModelField):
    def __init__(self, python_name, json_name, item_type, *, optional=False):
        if optional:
            raise TypeError(f"{python_name}: Optional[] cannot be used for collection fields")

        super().__init__(python_name, json_name)
        self.item_type = item_type

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


def _make_model_field(name, type_):
    if isinstance(type_, RenameAlias):
        json_name = type_.json_name
        type_ = type_.origin
    else:
        json_name = ''.join(x.capitalize() for x in name.split('_'))

    # could use typing_inspect PyPI module; this hack is especially ugly
    # since the string representation changed from python-3.8 to python-3.9
    type_str = str(type_)
    if (type_str.startswith('typing.Optional[') or
            (type_str.startswith('typing.Union[') and type_str.endswith(', NoneType]'))):
        type_ = typing.get_args(type_)[0]
        optional = True
    else:
        optional = False

    if isinstance(type_, IndexedListAlias):
        return IndexedListField(name, json_name, type_.origin, type_.indexed_field,
                                optional=optional)

    origin = getattr(type_, '__origin__', None)
    if origin == dict:
        if type_.__args__[0] != str:
            raise TypeError(f"{name}: Only dict[str] is supported")
        return DictField(name, json_name, type_.__args__[1], optional=optional)
    elif origin == list:
        return ListField(name, json_name, type_.__args__[0], optional=optional)
    elif origin is None:
        if issubclass(type_, BaseModel):
            return ClassField(name, json_name, type_, optional=optional)
        elif type_ == str:
            return StringField(name, json_name, optional=optional)
        elif type_ == int:
            return IntegerField(name, json_name, optional=optional)
        elif type_ == float:
            return FloatField(name, json_name, optional=optional)
        elif type_ == datetime:
            return DateTimeField(name, json_name, optional=optional)

    raise TypeError(f"{name}: Unsupported type {type_}")


class BaseModelMeta(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)
        annotations = getattr(x, '__annotations__', None)

        if annotations:
            x.__fields__ = {k: _make_model_field(k, v) for k, v in annotations.items()}
        else:
            x.__fields__ = {}

        for superclass in x.__mro__:
            if superclass is x:
                continue

            if hasattr(superclass, '__fields__'):
                for k, v in superclass.__fields__.items():
                    if k not in x.__fields__:
                        x.__fields__[k] = v

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

    def to_json_text(self):
        return json.dumps(self.to_json())

    @classmethod
    def from_json(cls, data):
        result = cls.__new__(cls)
        for field in cls.__fields__.values():
            setattr(result, field.python_name, field.python_value(data))

        return result

    @classmethod
    def from_json_text(cls, text):
        return cls.from_json(json.loads(text))
