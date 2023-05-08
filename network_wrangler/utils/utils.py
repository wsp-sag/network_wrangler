import pandas as pd

from ..logger import WranglerLogger


def topological_sort(adjacency_list, visited_list):
    """
    Topological sorting for Acyclic Directed Graph
    """

    output_stack = []

    def _topology_sort_util(vertex):
        if not visited_list[vertex]:
            visited_list[vertex] = True
            for neighbor in adjacency_list[vertex]:
                _topology_sort_util(neighbor)
            output_stack.insert(0, vertex)

    for vertex in visited_list:
        _topology_sort_util(vertex)

    return output_stack


def make_slug(text, delimiter: str = "_"):
    """
    makes a slug from text
    """
    import re

    text = re.sub("[,.;@#?!&$']+", "", text.lower())
    return re.sub("[\ ]+", delimiter, text)


def delete_keys_from_dict(dictionary: dict, keys: list) -> dict:
    """Removes list of keys from potentially nested dictionary.

    SOURCE: https://stackoverflow.com/questions/3405715/elegant-way-to-remove-fields-from-nested-dictionaries
    User: @mseifert

    Args:
        dictionary: dictionary to remove keys from
        keys: list of keys to remove

    """
    keys_set = set(keys)  # Just an optimization for the "if key in keys" lookup.

    modified_dict = {}
    for key, value in dictionary.items():
        if key not in keys_set:
            if isinstance(value, dict):
                modified_dict[key] = delete_keys_from_dict(value, keys_set)
            else:
                modified_dict[
                    key
                ] = value  # or copy.deepcopy(value) if a copy is desired for non-dicts.
    return modified_dict


def parse_time_spans_to_secs(times):
    """
    parse time spans into tuples of seconds from midnight
    can also be used as an apply function for a pandas series
    Parameters
    -----------
    times: tuple(string) or tuple(int) or list(string) or list(int)

    returns
    --------
    tuple(integer)
      time span as seconds from midnight
    """
    try:
        start_time, end_time = times
    except:
        msg = "ERROR: times should be a tuple or list of two, got: {}".format(times)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    # If times are strings, convert to int in seconds, else return as ints
    if isinstance(start_time, str) and isinstance(end_time, str):
        start_time = start_time.strip()
        end_time = end_time.strip()

        # If time is given without seconds, add 00
        if len(start_time) <= 5:
            start_time += ":00"
        if len(end_time) <= 5:
            end_time += ":00"

        # Convert times to seconds from midnight (Partride's time storage)
        h0, m0, s0 = start_time.split(":")
        start_time_sec = int(h0) * 3600 + int(m0) * 60 + int(s0)

        h1, m1, s1 = end_time.split(":")
        end_time_sec = int(h1) * 3600 + int(m1) * 60 + int(s1)

        return (start_time_sec, end_time_sec)

    elif isinstance(start_time, int) and isinstance(end_time, int):
        return times

    else:
        WranglerLogger.error("ERROR: times should be ints or strings")
        raise ValueError()

    return (start_time_sec, end_time_sec)

def coerce_dict_to_df_types(
        d: dict,
        df: pd.DataFrame, 
        skip_keys: list = [], 
        return_skipped: bool = False
    ) -> dict:
    """Coerce dictionary values to match the type of a dataframe columns matching dict keys.

    Will also coerce a list of values.

    Args:
        d (dict): dictionary to coerce with singleton or list values
        df (pd.DataFrame): dataframe to get types from
        skip_keys: list of dict keys to skip. Defaults to []/
        return_skipped: keep the uncoerced, skipped keys/vals in the resulting dict. 
            Defaults to False.

    Returns:
        dict: dict with coerced types
    """
    coerced_dict = {}
    for k,vals in d.items():
        if k in skip_keys: 
            if return_skipped:
                coerced_dict[k] = vals
            continue
        if pd.api.types.infer_dtype(df[k]) == 'integer':
            if isinstance(vals,list):
                coerced_v = [int(float(v)) for v in vals]
            else:
                coerced_v = int(float(vals))
        elif pd.api.types.infer_dtype(df[k]) == 'floating':
            if isinstance(vals,list):
                coerced_v = [float(v) for v in vals]
            else:
                coerced_v = float(vals)
        elif pd.api.types.infer_dtype(df[k]) == 'boolean':
            if isinstance(vals,list):
                coerced_v = [float(v) for v in vals]
            else:
                coerced_v = float(vals)
        else:
            if isinstance(vals,list):
                coerced_v = [str(v) for v in vals]
            else:
                coerced_v = str(vals)
        coerced_dict[k] = coerced_v
    return coerced_dict