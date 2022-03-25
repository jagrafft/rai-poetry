from functools import reduce
from pandas import DataFrame


def to_DF(rel, columns=None, return_df=True):
    """
    Invoke `single_key_relation_to_DF` or `multi_key_relation_to_DF` based on length of
    `output` key in dictionary returned from RAI.
    """
    return (
        single_key_relation_to_DF(rel, return_df, columns=columns)
        if len(rel["output"]) == 1
        else multikey_relation_to_DF(rel, return_df)
    )


def multikey_relation_to_DF(rel, return_df):
    """
    Transform a multikey relation with a 'ragged right' structure (e.g. not all relations
    are the same cardinality) to a Pandas `DataFrame` (default) or Python `dict`.

    NB. Function fills 'missing' keys in each relation with the value `None`.
    """

    # Traverse all `dict`s in the `output` array and add the contents of
    # the first index of `columns`--the keys for our data--to a `set`.
    global_key_set = set(
        reduce(lambda X, Y: X + Y, [x["columns"][0] for x in rel["output"]])
    )

    # Used in the `for` loop below
    data = {}

    # Traverse all `dict`s in the `output` array (again) and insert `None` values
    # for missing keys in each relation.
    for X in rel["output"]:
        # Create a `dict` of key-value pairs for the data included in the current relation.
        key_value_pairs = {
            x: X["columns"][1][i] for (i, x) in enumerate(X["columns"][0])
        }

        """
        Create key in `data` to store 'polyfilled' relation (e.g. relation with `None`s) then
        iterate `global_key_set` and check whether each of its members is included in `key_value_pairs`.

        Rules
            YES => insert value associated with key
            NO  => insert `None`
        """
        data[X["rel_key"]["keys"][0].replace(":", "")] = [
            key_value_pairs[k] if k in key_value_pairs else None for k in global_key_set
        ]

    # Return a Pandas `DataFrame` or Python `dict`
    return DataFrame(data) if return_df else data


def single_key_relation_to_DF(rel, return_df, columns=None):
    """
    Transform a single key relation to a Pandas `DataFrame` or Python `dict`.
    """
    if columns is not None:
        # If user provided column names, ensure there are exactly enough for available data relations.
        if len(columns) != len(rel["output"][0]["columns"]):
            print("ERROR: Length of `columns` != number of columns in data.")
            return
        else:
            d = {columns[i]: v for (i, v) in enumerate(rel["output"][0]["columns"])}

            # Return `DataFrame` or `dict` of key-value pairs where keys populate from `columns`.
            return DataFrame(d) if return_df else d
    else:
        d = {f"x{i}": v for (i, v) in enumerate(rel["output"][0]["columns"])}

        # Return `DataFrame` or `dict` of key-value pairs where keys are generated automatically.
        return DataFrame(d) if return_df else d
