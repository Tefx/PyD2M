from collections import OrderedDict

from copy import deepcopy
import re
from string import Formatter


class RETrans:
    def __init__(self):
        self.fields = []

    def __getitem__(self, name):
        self.fields.append(name)
        return "([\w+\d\-_]+?)"


def cmp_path(pattern, path):
    trans = RETrans()
    matches = re.fullmatch(Formatter().vformat(pattern, (), trans), path)
    if matches:
        return {k: v for k, v in zip(trans.fields, matches.groups())}
    else:
        return None


def deep_update(d1, d2):
    if not isinstance(d1, dict):
        return d2
    for key, value in d2.items():
        d1[key] = deep_update(d1.get(key, None), value)
    return d1


class DataConfig:
    def __init__(self, path, config, defaults={}, declared_fields={}):
        self.path = path
        self._config = deep_update(deepcopy(defaults), config)
        self.fields = OrderedDict()
        for field in self._config["FIELDS"]:
            if isinstance(field, dict):
                self.fields = deep_update(self.fields, field)
            else:
                self.fields[field] = declared_fields.get(field, "Unknown")
        if not self._config["LOCAL_FIELDS_ONLY"] and not self._config["DECLARE_NEW_FIELDS"]:
            for field in self.fields:
                if field not in declared_fields:
                    print("Warning: undeclared field type \"{}\" in {}".format(field, path))

    def has_fields(self, *fields):
        if self._config["LOCAL_FIELDS_ONLY"] or self.path.startswith("tmp"):
            return []
        else:
            return set(f for f in fields if f in self.fields)

    def __getattr__(self, item):
        item = item.upper()
        if item in self._config:
            return self._config[item]
        else:
            raise AttributeError


def _traverse_data(config, path=""):
    if "FIELDS" in config:
        yield path, config
    elif isinstance(config, dict):
        for sub_path, sub_conf in config.items():
            yield from _traverse_data(sub_conf, "/".join([path, sub_path]).strip("/"))


class Config:
    def __init__(self):
        self.DEFAULTS = dict()
        self.FIELDS = dict()
        self.PARAMS = dict()
        self.DATA = dict()

    def update(self, config):
        name, config = next(iter(config.items()))
        if name != "DATA":
            setattr(self, name, deep_update(getattr(self, name), config))
        else:
            for path, data_conf in _traverse_data(config):
                data = DataConfig(path, data_conf, defaults=self.DEFAULTS, declared_fields=self.FIELDS)
                self.DATA[path] = data
                self.FIELDS = deep_update(self.FIELDS, data.fields)

    def __getitem__(self, item):
        return self.DATA[item]

    def __contains__(self, item):
        return item in self.DATA

    def exists(self, path):
        return path in self.DATA

    def search_fields(self, *fields):
        for data in self.DATA.values():
            _fields = data.has_fields(*fields)
            if _fields:
                yield data.path, _fields

    def search_ex(self, path):
        for pattern, data in self.DATA.items():
            matches = cmp_path(pattern, path)
            if matches is not None:
                return pattern, matches
