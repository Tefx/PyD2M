import importlib
import inspect
import os
from collections import OrderedDict

from copy import deepcopy
import re
from string import Formatter
import yaml

from .cookbook import CookBook
from .hooks import Hooks


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

    def fields(self):
        if self._config["LOCAL_FIELDS_ONLY"] or self.path.startswith("tmp"):
            return dict()
        else:
            return self.fields

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
    def __init__(self, conf_base):
        self.DEFAULTS = dict()
        self.FIELDS = dict()
        self.PARAMS = dict()
        self.DATA = dict()
        self.conf_base = set()
        self.read_config(conf_base)

    def read_config(self, base):
        if base not in self.conf_base:
            self.conf_base.add(base)
            file_path = os.path.join(base, "d2m.rc")
            with open(file_path, "r") as f:
                for config in yaml.load(f.read(), Loader=yaml.FullLoader):
                    self.update(config, file_path)

    def read_includes(self, item, current_file):
        current_path = os.path.dirname(current_file)
        include_base = os.path.realpath(os.path.join(current_path, item))
        self.read_config(include_base)

    def update(self, config, file_path):
        name, config = next(iter(config.items()))
        if name == "INCLUDE":
            for item in config:
                self.read_includes(item, file_path)
        elif name != "DATA":
            setattr(self, name, deep_update(getattr(self, name), config))
        else:
            for path, data_conf in _traverse_data(config):
                data = DataConfig(path, data_conf, defaults=self.DEFAULTS, declared_fields=self.FIELDS)
                self.DATA[path] = data
                self.FIELDS = deep_update(self.FIELDS, data.fields)

    def cookbooks(self):
        for base in self.conf_base:
            with os.scandir(base) as it:
                for entry in it:
                    if entry.name.endswith('.cb') and entry.is_file():
                        loader = importlib.machinery.SourceFileLoader(entry.name[:-3], entry.path)
                        spec = importlib.util.spec_from_loader(loader.name, loader)
                        cb = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(cb)
                        for _, item in inspect.getmembers(cb, inspect.isclass):
                            if issubclass(item, CookBook) and item is not CookBook:
                                yield item(ds=self)

    def hooks(self):
        for base in self.conf_base:
            with os.scandir(base) as it:
                for entry in it:
                    if entry.name.endswith('.hk') and entry.is_file():
                        loader = importlib.machinery.SourceFileLoader(entry.name[:-3], entry.path)
                        spec = importlib.util.spec_from_loader(loader.name, loader)
                        hk = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(hk)
                        if isinstance(hk.hooks, Hooks) and hk.hooks not in self.hooks:
                            yield hk.hooks

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

    @property
    def all_files(self):
        return set(self.DATA.keys())

    @property
    def all_fields(self):
        s = dict()
        for value in self.DATA.values():
            s.update(value.fields)
        return s

