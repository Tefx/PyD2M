from .cookbook import MultiData


def groupby(data, field, drop_index=False):
    return MultiData([(g.reset_index(drop=drop_index), {field: k}) for k, g in data.groupby(field)])
