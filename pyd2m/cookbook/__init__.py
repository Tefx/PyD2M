from .cookbook import *

cookbook = CookBook()
recipe = cookbook.recipe
auto_recipe = cookbook.auto_recipe
quick_recipe = cookbook.quick_recipe


def load_lib(name):
    fila_name = os.path.join(os.path.dirname(inspect.stack()[1][1]), "{}.py".format(name))
    spec = importlib.util.spec_from_file_location(name, fila_name)
    lib = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lib)
    return lib
