class Hooks:
    def __init__(self):
        self.load_hooks = {}
        self.dump_hooks = {}

    def load(self, path):
        def wrapper(func):
            self.load_hooks[path] = func
            return func

        return wrapper

    def dump(self, path):
        def wrapper(func):
            self.dump_hooks[path] = func
            return func

        return wrapper


hooks = Hooks()
load = hooks.load
dump = hooks.dump
