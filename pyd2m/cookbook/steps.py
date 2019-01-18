from .cookbook import MultiData


def groupby(data, field):
    return MultiData([(g.reset_index(), {field: k}) for k, g in data.groupby(field)])
