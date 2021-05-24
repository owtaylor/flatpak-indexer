from datetime import timedelta
from enum import Enum
import re
import os
from typing import Any, Dict, List, Optional, Union

import yaml

from .utils import resolve_type, substitute_env_vars


"""
This implements a simple semi-declarative configuration system based on type
annotations. Configuration objects derive from BaseConfig, and configuration fields
are found by looking at the annotated attributes of the class.

Validity checks and special handling are implemented by overriding the __init__
method of the configuration class.

Example:
    ::
         class MyConfig(BaseConfig):
            x: int  = 42  # field with a default
            y: str        # required field
            z: str = configfield(skip=True)

         def __init__(self, lookup):
             super().__init__(lookup)

             # Validation
             if self.x > 100:
                 raise ConfigError("x must be less than 100")

            # Special handling
             z = lookup.get_str("z", default=None)
             if z is None:
                 z = lookup.get_str("oldZ")
             self.z = z

        my_config = MyConfig.from_yaml("config.yaml")
"""


class ConfigError(Exception):
    pass


class Defaults(Enum):
    REQUIRED = 1


class ConfigField:
    def __init__(self, *, skip, default, extra):
        self.skip = skip
        self.default = default
        self.extra = extra


def configfield(*, skip=False, default=Defaults.REQUIRED, **kwargs) -> Any:
    return ConfigField(skip=skip, default=default, extra=kwargs)


class Lookup:
    def __init__(self, attrs: Dict[str, Any], path: Optional[str] = None):
        self.path = path
        self.attrs = attrs

    def _get_path(self, key: str):
        if self.path is not None:
            return self.path + '/' + key
        else:
            return key

    def sublookup(self, parent_key: str):
        attrs = self.attrs.get(parent_key, {})
        return Lookup(attrs, parent_key)

    def iterate_objects(self, parent_key: str):
        objects = self.attrs.get(parent_key)
        if not objects:
            return

        for name, attrs in objects.items():
            yield name, Lookup(attrs, parent_key + '/' + name)

    def _get(self, key: str, default: Any):
        if default is Defaults.REQUIRED:
            try:
                return self.attrs[key]
            except KeyError:
                raise ConfigError("A value is required for {}".format(self._get_path(key))) \
                    from None
        else:
            return self.attrs.get(key, default)

    def get_str(
        self, key: str,
        default: Union[str, None, Defaults] = Defaults.REQUIRED,
        force_trailing_slash: bool = False
    ) -> Optional[str]:
        val = self._get(key, default)
        if val is None:
            return None

        if not isinstance(val, str):
            raise ConfigError("{} must be a string".format(self._get_path(key)))

        val = substitute_env_vars(val)

        if force_trailing_slash and not val.endswith('/'):
            val += '/'

        return val

    def get_bool(self, key: str, default: Union[bool, Defaults] = Defaults.REQUIRED) -> bool:
        val = self._get(key, default)
        if not isinstance(val, bool):
            raise ConfigError("{} must be a boolean".format(self._get_path(key)))

        return val

    def get_int(self, key: str, default: Union[int, Defaults] = Defaults.REQUIRED) -> int:
        val = self._get(key, default)
        if not isinstance(val, int):
            raise ConfigError("{} must be an integer".format(self._get_path(key)))

        return val

    def get_str_list(
        self, key: str, default: Union[List[str], Defaults] = Defaults.REQUIRED
    ) -> List[str]:
        val = self._get(key, default)
        if not isinstance(val, list) or not all(isinstance(v, str) for v in val):
            raise ConfigError("{} must be a list of strings".format(self._get_path(key)))

        return [substitute_env_vars(v) for v in val]

    def get_str_dict(
        self, key: str, default: Union[Dict[str, str], Defaults] = Defaults.REQUIRED
    ) -> Dict[str, str]:
        val = self._get(key, default)
        if not isinstance(val, dict) or not all(isinstance(v, str) for v in val.values()):
            raise ConfigError("{} must be a mapping with string values".format(self._get_path(key)))

        return {substitute_env_vars(k): substitute_env_vars(v) for k, v in val.items()}

    def get_timedelta(
        self, key: str,
        default: Union[timedelta, None, Defaults] = Defaults.REQUIRED,
        force_suffix: bool = True
    ) -> Optional[timedelta]:
        val = self._get(key, default=default)
        if val is None:
            return None

        if isinstance(val, timedelta):  # the default
            return val

        if isinstance(val, int) and not force_suffix:
            return timedelta(seconds=val)

        if isinstance(val, str):
            m = re.match(r'^(\d+)([dhms])$', val)
            if m:
                if m.group(2) == "d":
                    return timedelta(days=int(m.group(1)))
                elif m.group(2) == "h":
                    return timedelta(hours=int(m.group(1)))
                elif m.group(2) == "m":
                    return timedelta(minutes=int(m.group(1)))
                else:
                    return timedelta(seconds=int(m.group(1)))

        raise ConfigError("{} should be a time interval of the form <digits>[dhms]"
                          .format(self._get_path(key)))


class BaseConfig:
    def __init__(self, lookup: Lookup):
        annotations = getattr(self, '__annotations__', None)
        if annotations:
            for name, v in annotations.items():
                resolved, args, optional = resolve_type(v)
                classval = getattr(self, name, Defaults.REQUIRED)
                if isinstance(classval, ConfigField):
                    if classval.skip:
                        continue
                    kwargs = {"default": classval.default}
                    kwargs.update(classval.extra)
                else:
                    kwargs = {"default": classval}

                if resolved == str:
                    val: Any = lookup.get_str(name, **kwargs)
                elif resolved == bool:
                    val = lookup.get_bool(name, **kwargs)
                elif resolved == int:
                    val = lookup.get_int(name, **kwargs)
                elif resolved == list and args[0] == str:
                    val = lookup.get_str_list(name, **kwargs)
                elif resolved == dict and args[0] == str and args[1] == str:
                    val = lookup.get_str_dict(name, **kwargs)
                elif resolved == timedelta:
                    val = lookup.get_timedelta(name, **kwargs)
                elif issubclass(resolved, BaseConfig):
                    val = resolved(lookup.sublookup(name))
                else:
                    raise RuntimeError(f"Don't know how to handle config field of type {v}")

                setattr(self, name, val)

    @classmethod
    def from_path(cls, path: str):
        with open(path, 'r') as f:
            yml = yaml.safe_load(f)

        if not isinstance(yml, dict):
            raise ConfigError(f"Top level of {os.path.basename(path)} must be an object with keys")

        return cls(Lookup(yml))
