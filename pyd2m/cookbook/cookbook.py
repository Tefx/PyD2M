import inspect
import pandas as pd


class MultiData:
    def __init__(self, data=[]):
        self.data = data

    def add(self, df, **kwargs):
        self.data.append((df, kwargs))

    def __iter__(self):
        yield from self.data


class Recipe:
    def __init__(self, ingredients=[], dishes=[]):
        self.ingredients = ingredients
        self.dishes = dishes
        self.procedure = None
        self.name = None
        self.cookbook = None

    def __call__(self, func):
        self.procedure = func
        return self

    def cook(self, ingredients, **condiments):
        condiments = {k: v for k, v in condiments.items() if k in inspect.signature(self.procedure).parameters.keys()}
        dishes = self.procedure(self.cookbook, *ingredients, **condiments)
        if not isinstance(dishes, tuple):
            dishes = dishes,
        for dish, path in zip(dishes, self.dishes):
            if isinstance(dish, MultiData):
                for sd, svars in dish:
                    yield path, sd, svars
            else:
                yield path, dish, {}


class CookBook:
    def __init__(self, ds=None):
        self.menu = {}
        for name, recipe in inspect.getmembers(self, lambda x: isinstance(x, Recipe)):
            self.register(recipe)
        self.DS = ds

    def register(self, recipe):
        recipe.cookbook = self
        recipe.name = "{}.{}".format(self.__class__.__name__, recipe.procedure.__name__)
        for item in recipe.dishes:
            if item not in self.menu:
                self.menu[item] = []
            self.menu[item].append(recipe)

    def search(self, item):
        return self.menu.get(item, [])

    def list_recipes(self):
        for name, recipes in self.menu.items():
            print(name)
            for recipe in recipes:
                print("\t{} => {}: {}".format(recipe.ingredients, recipe.dishes, recipe.name))
            print()

    def recipe(self, single_dishes=None, ingredients=[], dishes=[]):

        if single_dishes:
            dishes = [single_dishes]

        def wrapper(func):
            self.register(Recipe(ingredients, dishes)(func))
            return func

        return wrapper

    def auto_recipe(self, dish):
        self.register(Recipe(dishes=[dish])(lambda cb: cb.DS.autogen(dish)[0]))

    def quick_recipe(self, name, ingredients=[], dishes=[], **kwargs):
        if name == "concat":
            proc = lambda cb, data: pd.concat(data, **kwargs)
        elif name == "merge":
            proc = lambda cb, left, right: left.merge(right, **kwargs)
        elif name == "groupby":
            field = kwargs.get("field")
            proc = lambda cb, data: MultiData([(g.reset_index(), {field: k}) for k, g in data.groupby(field)])
        else:
            raise NotImplementedError
        self.register(Recipe(ingredients=ingredients, dishes=dishes)(proc))
