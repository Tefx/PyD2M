import shutil
import yaml
import os
from copy import deepcopy
from string import Formatter
import inspect
import importlib.util
import importlib.machinery
from glob import glob
import re
from contextlib import contextmanager

from .config import Config
from .cookbook import CookBook, cookbook
from .hooks import hooks, Hooks
from . import store


class GlobTrans:
    def __getitem__(self, name):
        return "*"


class PartialStringFormatter(dict):
    def __missing__(self, key):
        return "{" + key + "}"


class DataSource:
    def __init__(self, data_path, config_path=None,
                 clear_cache=False, clear_tmp=True, cache_in_memory=False, **vars):
        self.base = os.path.realpath(data_path)
        self.config_base = config_path or os.path.join(self.base, "conf")
        self.cache_in_memory = cache_in_memory
        self.mem_cache = {}

        self.cookbooks = [cookbook]
        cookbook.DS = self

        self.hooks = [hooks]
        self.vars = deepcopy(vars)
        self.stores = {}

        self.config_file = os.path.join(self.config_base, "d2m.rc")
        self.config = Config()
        with open(self.config_file, "r") as f:
            for config in yaml.load(f.read()):
                self.config.update(config)

        for name, cls in inspect.getmembers(store, inspect.isclass):
            if issubclass(cls, store.DataStore) and cls.TYPE_TAG is not None:
                self.stores[cls.TYPE_TAG] = cls()

        with os.scandir(self.config_base) as it:
            for entry in it:
                if entry.name.endswith('.cb') and entry.is_file():
                    loader = importlib.machinery.SourceFileLoader(entry.name[:-3], entry.path)
                    spec = importlib.util.spec_from_loader(loader.name, loader)
                    cb = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(cb)
                    for _, item in inspect.getmembers(cb, inspect.isclass):
                        if issubclass(item, CookBook) and item is not CookBook:
                            self.cookbooks.append(item(ds=self))
                if entry.name.endswith('.hk') and entry.is_file():
                    loader = importlib.machinery.SourceFileLoader(entry.name[:-3], entry.path)
                    spec = importlib.util.spec_from_loader(loader.name, loader)
                    hk = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(hk)
                    if isinstance(hk.hooks, Hooks) and hk.hooks not in self.hooks:
                        self.hooks.append(hk.hooks)

        if clear_tmp is True:
            tmp_path = os.path.join(self.base, "tmp")
        elif isinstance(clear_tmp, str):
            tmp_path = os.path.join(self.base, self._format_path(clear_tmp))
        else:
            tmp_path = None
        if tmp_path is not None and os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)

        if clear_cache and os.path.exists(os.path.join(self.base, "cache")):
            shutil.rmtree(os.path.join(self.base, "cache"))

    def expand_path(self, path, vars=None):
        vars = {} if vars is None else vars
        matches = self.config.search_ex(path)
        if matches is not None:
            vars.update(matches[1])
            return matches[0]
        else:
            return path

    def __getstate__(self):
        return self.base, self.config_base, self.vars, self.cache_in_memory

    def __setstate__(self, state):
        base, config_base, vars, cache_in_memory = state
        return self.__init__(base, config_base, clear_cache=False, clear_tmp=False,
                             cache_in_memory=cache_in_memory, **vars)

    def _format_path(self, path, vars=None):
        vars = {} if vars is None else vars
        formatter = PartialStringFormatter(**self.vars)
        formatter.update(vars)
        return Formatter().vformat(path, (), formatter)

    def real_path(self, path_node, check_existing=True, **vars):
        path = self._format_path(path_node, vars)
        if re.search(r"{[\w\d\-_]+?}", path) is not None:
            pattern = Formatter().vformat(os.path.join(self.base, self.expand_path(path)), {}, GlobTrans())
            return [f[len(self.base):].strip("/") for f in glob(pattern, recursive=True)] or None
        else:
            real_path = os.path.abspath(os.path.join(self.base, path))
            if check_existing and not self.stores[self.config[path_node].type].exists(real_path):
                return None
            return real_path

    def exists(self, path, **vars):
        path = self.expand_path(path, vars)
        return self.real_path(path, **vars, check_existing=True) is not None

    def load(self, path, generate=True, callback=None, **vars):
        path = self.expand_path(path, vars)
        real_path = self.real_path(path, check_existing=True, **vars)
        if not isinstance(real_path, list) and real_path in self.mem_cache:
            return self.mem_cache[real_path]
        if real_path is None:
            if generate:
                return self.generate(path, callback=callback, **vars)
        elif isinstance(real_path, list):
            return [self.load(path, generate=False) for path in real_path]
        else:
            data_conf = self.config[path]
            data = self.stores[data_conf.type].load(real_path, data_conf)
            for hooks in self.hooks:
                if path in hooks.load_hooks:
                    data = hooks.load_hooks[path](data)
            if not data_conf.free_fields:
                data = data.reindex(columns=data_conf.fields.keys())
                fields = {k: v for k, v in data_conf.fields.items() if v != "obj"}
                data = data.astype(dtype=fields, copy=False)
            if self.cache_in_memory:
                self.mem_cache[real_path] = data
            return data

    def dump(self, path, data, **vars):
        path = self.expand_path(path, vars)
        real_path = self.real_path(path, check_existing=False, **vars)
        if not real_path: raise SystemError
        data_conf = self.config[path]
        if not data_conf.free_fields:
            data = data.reindex(columns=data_conf.fields.keys())
            fields = {k: v for k, v in data_conf.fields.items() if v != "obj"}
            data = data.astype(dtype=fields, copy=False)
        _data = data
        for hooks in self.hooks:
            if path in hooks.dump_hooks:
                data = hooks.dump_hooks[path](data)
        dir_path = os.path.split(real_path)[0]
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        self.stores[data_conf.type].dump(real_path, data, data_conf)
        return _data

    def delete(self, path, **vars):
        path = self.expand_path(path, vars)
        real_path = self.real_path(path, check_existing=False, **vars)
        if not real_path: raise SystemError
        if os.path.exists(real_path):
            if os.path.isdir(real_path):
                shutil.rmtree(real_path)
            else:
                self.stores[self.config[path].type].delete(real_path)

    @contextmanager
    def update(self, paths, **vars):
        for path in paths:
            self.delete(path, **vars)
        yield
        for path in paths:
            self.generate(path, **vars)

    def search_fields(self, *fields):
        return sorted(list(self.config.search_fields(*fields)), key=lambda x: len(x[1]))

    def search_recipes(self, path, **vars):
        if self.exists(path, **vars):
            return True
        else:
            for cookbook in self.cookbooks:
                for recipe in cookbook.search(path):
                    steps = []
                    for ingredient in recipe.ingredients:
                        sr = self.search_recipes(ingredient, **vars)
                        if not sr:
                            steps = None
                            break
                        elif isinstance(sr, list):
                            steps.extend(sr)
                    if steps is not None:
                        steps.append(recipe)
                        return steps
            return False

    def generate_by_recipe(self, recipe, **vars):
        if all(self.exists(i, **vars) for i in recipe.dishes):
            return
        from_data = [self.load(path, **vars) for path in recipe.ingredients]
        print("{} => {} By <{}>".format(recipe.ingredients, recipe.dishes, recipe.name))
        for path, data, svars in recipe.cook(from_data, **self.config.PARAMS):
            _vars = deepcopy(vars)
            _vars.update(svars)
            self.dump(path, data, **_vars)

    def can_generate(self, path, **vars):
        path = self.expand_path(path, vars)
        recipe = self.search_recipes(path, **vars)
        return recipe is not False

    def generate(self, path, callback=None, **vars):
        path = self.expand_path(path, vars)
        print("Generating", self._format_path(path, vars))
        steps = self.search_recipes(path, **vars)
        if not steps:
            if callback:
                data = callback()
                self.dump(path, data, **vars)
            else:
                print("Cannot find any recipe for {}".format(path))
                for cookbook in self.cookbooks:
                    cookbook.list_recipes()
                raise SystemError
        elif isinstance(steps, list):
            for recipe in steps:
                if recipe is not True:
                    self.generate_by_recipe(recipe, **vars)
        return self.load(path, generate=False, **vars)

    def related_data(self, fields, path=None):
        path = [] if path is None else path
        unknown_fields = set(fields)
        related_data = []
        for other_path, fields in self.search_fields(*fields):
            if other_path in path: continue
            related_data.append((other_path, fields))
            for f in fields:
                if f in unknown_fields:
                    unknown_fields.remove(f)
        related_data.sort(key=lambda x: len(x[1]), reverse=True)
        return related_data, unknown_fields

    def show_related_data(self, path, **vars):
        path = self.expand_path(path, vars)
        other_data, unknown_fields = self.related_data(self.fields(path), path=[path])
        for other_path, fields in other_data:
            print("{} in {}".format(fields, other_path))
        if unknown_fields:
            print("Fields not found: ", unknown_fields)

    @staticmethod
    def print_fields(df):
        print("- {}: {}".format(df.index.name, df.index.dtype))
        print("\n".join("- {}: {}".format(s, df[s].dtype) for s in df.columns.tolist()))

    def fields(self, path, name_only=True):
        path = self.expand_path(path)
        fields = self.config[path].fields
        if name_only:
            fields = fields.keys()
        return fields

    @property
    def params(self):
        return self.config.PARAMS

    def autogen_scheme(self, fs, skip_path=None):
        skip_path = [] if skip_path is None else skip_path
        if isinstance(fs, str):
            skip_path.append(fs)
            fs = self.fields(fs)
        else:
            fs = fs

        rfs = self.related_data(fs, path=skip_path)[0]
        needs = set(fs)
        gets = set()

        if not rfs:
            return None, [], needs

        path, fields = rfs[0]
        needs -= fields
        gets |= fields
        base = (path, list(fields))

        joins = []
        can_join = True
        while needs and can_join:
            can_join = False
            for p, fields in rfs[1:]:
                if self.can_generate(p) and fields & gets and fields & needs:
                    keys = fields & gets
                    joins.append((p, list(keys), list((fields - gets) | keys)))
                    gets |= fields
                    needs -= fields
                    can_join = True

        return base, joins, needs, fs

    def autogen(self, path, how="inner", skip_path=None):
        skip_path = [] if skip_path is None else skip_path
        base, joins, unknown, fs = self.autogen_scheme(path, skip_path=skip_path)
        data = self.load(base[0])[base[1]].copy()
        print("Base: ", base[0])
        for path, keys, fields in joins:
            print("Joining:", path)
            data = data.merge(self.load(path)[fields], on=keys, how=how, copy=False)
        data = data.reindex(columns=fs, copy=False)
        return data, unknown

    def __getitem__(self, item):
        if isinstance(item, str):
            item = [item]
        return self.autogen(item)[0]

    def __getattr__(self, item):
        v = self.config.PARAMS.get(item, None)
        if v is None:
            v = self.vars.get(item, None)
        if v is None:
            raise AttributeError
        return v

    def filter(self, fields, func, gen_fields=None):
        data = self[fields]
        data = data[func(data)]
        if gen_fields:
            return data[gen_fields]
        else:
            return data
