import os

import pandas as pd
import numpy as np
import msgpack
import pickle


class DataStore:
    TYPE_TAG = None

    def dump(self, path, data, config):
        raise NotImplementedError

    def load(self, path, config):
        raise NotImplementedError

    def exists(self, path):
        return os.path.exists(path)

    def delete(self, path):
        if os.path.exists(path):
            os.remove(path)

    def __repr__(self):
        return "[DataStore({})]".format(self.TYPE_TAG)


class DSMemory(DataStore):
    TYPE_TAG = "memory"

    def __init__(self):
        self.cache = {}

    def dump(self, path, data, config):
        self.cache[path] = data

    def load(self, path, config):
        return self.cache[path]

    def exists(self, path):
        return path in self.cache

    def delete(self, path):
        if path in self.cache:
            del self.cache[path]


try:
    import feather
except ImportError:
    pass


class DSFeather(DataStore):
    TYPE_TAG = "feather"

    def dump(self, path, data, config):
        data.reset_index(drop=True, inplace=True)
        data.to_feather(path)

    def load(self, path, config):
        return feather.read_dataframe(path)


class DSParquet(DataStore):
    TYPE_TAG = "parquet"

    def dump(self, path, data, config):
        data.to_parquet(path)

    def load(self, path, config):
        return pd.read_parquet(path)


class DSCsv(DataStore):
    TYPE_TAG = "csv"

    def dump(self, path, data, config):
        csv_kwargs = {k: eval(v) if isinstance(v, str) else v for k, v in getattr(config, "CSV_DUMP_ARG", {}).items()}
        data.to_csv(path, **csv_kwargs)

    def load(self, path, config):
        kwargs = {k: eval(v) if isinstance(v, str) else v for k, v in getattr(config, "CSV_LOAD_ARG", {}).items()}
        df = pd.read_csv(path, **kwargs)
        if kwargs.get("header", True) is None:
            fields = {idx: f for idx, f in
                      enumerate([k for k in config.fields.keys() if k not in getattr(config, "CSV_EXCLUDE", [])])}
            if getattr(config, "INDEX", False):
                df.index.name = fields[0]
                fields = fields[1:]
            df.rename(columns=fields, inplace=True)
        return df


class DSNumpy(DataStore):
    TYPE_TAG = "npy"

    def dump(self, path, data, config):
        np.save(path, data)

    def load(self, path, config):
        return np.load(path)


class DSPickle(DataStore):
    TYPE_TAG = "pickle"

    def dump(self, path, data, config):
        with open(path, "wb") as f:
            pickle.dump(data, f)

    def load(self, path, config):
        with open(path, "rb") as f:
            data = pickle.load(f)
        return data

import json

class DSJSON(DataStore):
    TYPE_TAG = "json"

    def dump(self, path, data, config):
        with open(path, "w") as f:
            json.dump(data, f)

    def load(self, path, config):
        with open(path, "r") as f:
            data = json.load(f)
        return data

class DSStrList(DataStore):
    TYPE_TAG = "str_list"

    def dump(self, path, data, config):
        with open(path, "w") as f:
            f.write("\n".join(data))

    def load(self, path, config):
        data = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    data.append(line)
        return data

class DSMsgpackStream(DataStore):
    TYPE_TAG = "msgpack_stream"

    def dump(self, path, data, config):
        raise NotImplementedError

    def load(self, path, config):
        with open(path, "rb") as f:
            unpacker = msgpack.Unpacker(f)
            columns = unpacker.unpack()
            data = []
            try:
                for item in unpacker:
                    data.append(item)
            except msgpack.OutOfData:
                pass
        return pd.DataFrame(data, columns=columns)


class PickleStream(DataStore):
    TYPE_TAG = "pickle_stream"

    def dump(self, path, data, config):
        raise NotImplementedError

    def load(self, path, config):
        data = []
        with open(path, "rb") as f:
            try:
                while True:
                    data.append(pickle.load(f))
            except EOFError:
                pass
        return data
