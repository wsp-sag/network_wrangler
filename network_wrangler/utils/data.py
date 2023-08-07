import hashlib
from typing import Mapping, Any

import pandas as pd

from ..logger import WranglerLogger


@pd.api.extensions.register_dataframe_accessor("dict_query")
class DictQueryAccessor:
    """
    Query link, node and shape dataframes using project selection dictionary.

    Will overlook any keys which are not columns in the dataframe.

    Usage:

    ```
    selection_dict = {
        "lanes":[1,2,3],
        "name":['6th','Sixth','sixth'],
        "drive_access": 1,
    }
    selected_links_df = links_df.dict_query(selection_dict)
    ```

    """

    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def __call__(self, selection_dict: dict, return_all_if_none: bool = False):
        """_summary_

        Args:
            selection_dict (dict): _description_
            return_all_if_none (bool, optional): If True, will return entire df if dict has
                 no values. Defaults to False.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        _selection_dict = {
            k: v for k, v in selection_dict.items() if k in self._obj.columns
        }

        if not _selection_dict:
            if return_all_if_none:
                return self._obj
            raise ValueError(
                f"Relevant part of selection dictionary is empty: {selection_dict}"
            )

        _sel_query = _dict_to_query(_selection_dict)
        WranglerLogger.debug(f"_sel_query:\n   {_sel_query}")
        _df = self._obj.query(_sel_query, engine="python")

        if len(_df) == 0:
            WranglerLogger.warning(
                f"No records found in {_df.name} \
                                   using selection: {selection_dict}"
            )
        return _df


def _dict_to_query(
    selection_dict: Mapping[str, Any],
) -> str:
    """Generates the query of from selection_dict.

    Args:
        selection_dict: selection dictionary

    Returns:
        _type_: Query value
    """
    WranglerLogger.debug("Building selection query")

    def _kv_to_query_part(k, v, _q_part=""):
        if isinstance(v, list):
            _q_part += "(" + " or ".join([_kv_to_query_part(k, i) for i in v]) + ")"
            return _q_part
        if isinstance(v, str):
            return k + '.str.contains("' + v + '")'
        else:
            return k + "==" + str(v)

    query = (
        "("
        + " and ".join([_kv_to_query_part(k, v) for k, v in selection_dict.items()])
        + ")"
    )
    WranglerLogger.debug(f"Selection query:\n{query}")
    return query


@pd.api.extensions.register_dataframe_accessor("df_hash")
class dfHash:
    """Creates a dataframe hash that is compatable with geopandas and various metadata.

    Definitely not the fastest, but she seems to work where others have failed.
    """

    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def __call__(self):
        _value = str(self._obj.values).encode()
        hash = hashlib.sha1(_value).hexdigest()
        return hash


def _check_compatable_df_update(
    destination_df: pd.DataFrame,
    source_df: pd.DataFrame,
    join_col: str,
    properties: list[str] = None,
) -> None:
    """Evaluates if destination df is comptable for being updated with source_df based on id_col.

    Args:
        destination_df (pd.DataFrame): Dataframe to modify.
        source_df (pd.DataFrame): Dataframe with updated columns
        join_col (str): column to join on
        properties (list[str]): List of properties to use. If None, will default to all.
    """

    if join_col not in source_df.columns:
        raise ValueError(f"source_df must have {join_col}.")

    if properties is None:
        properties = [p for p in source_df.columns if p != join_col]

    _missing_cols = set(properties) - set(source_df.columns)
    if _missing_cols:
        raise ValueError(f"Properties missing from source_df: {_missing_cols}")

    if join_col not in destination_df:
        raise ValueError(f"joim_col {join_col} not in destination_df columns.")

    _missing_t_cols = set(properties) - set(destination_df.columns)
    if _missing_t_cols:
        raise NotImplementedError(
            f"Properties missing from destination_df: {_missing_t_cols}"
        )

    _missing_ids = set(source_df[join_col]) - set(destination_df[join_col])
    if _missing_ids:
        raise ValueError(
            f"join values specified in set_df missing from destionation_df table:\
            {_missing_ids}"
        )


def update_df_by_col_value(
    destination_df: pd.DataFrame,
    source_df: pd.DataFrame,
    join_col: str,
    properties: list[str] = None,
) -> pd.DataFrame:
    """Creates an updated destination_df based on values in source_df with same  join_col.

    ```
    >> destination_df
    trip_id  property1  property2
    1         10      100
    2         20      200
    3         30      300
    4         40      400

    >> source_df
    trip_id  property1  property2
    2         25      250
    3         35      350

    >> updated_df
    trip_id  property1  property2
    0        1       10      100
    1        2       25      250
    2        3       35      350
    3        4       40      400
    ```

    Args:
        destination_df (pd.DataFrame): Dataframe to modify.
        source_df (pd.DataFrame): Dataframe with updated columns
        join_col (str): column to join on
        properties (list[str]): List of properties to use. If None, will default to all 
            in source_df.
    """
    if properties is None:
        properties = [p for p in properties if p != join_col]

    _check_compatable_df_update(
        destination_df, source_df, join_col, properties=properties
    )

    merged_df = destination_df.merge(
        source_df, on=join_col, how="left", suffixes=("", "_updated")
    )
    for prop in properties:
        merged_df[prop] = merged_df[f"{prop}_updated"].combine_first(merged_df[prop])
    updated_df = merged_df.drop([f"{prop}_updated" for prop in properties], axis=1)

    return updated_df
