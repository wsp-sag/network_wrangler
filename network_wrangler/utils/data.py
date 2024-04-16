import hashlib
from typing import Mapping, Any, Union, Tuple

import pandas as pd
from numpy import ndarray

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
            k: v for k, v in selection_dict.items() if k in self._obj.columns and v is not None
        }

        if not _selection_dict:
            if return_all_if_none:
                return self._obj
            raise ValueError(
                f"Relevant part of selection dictionary is empty: {selection_dict}"
            )

        _sel_query = dict_to_query(_selection_dict)
        WranglerLogger.debug(f"_sel_query:\n   {_sel_query}")
        _df = self._obj.query(_sel_query, engine="python")

        if len(_df) == 0:
            WranglerLogger.warning(
                f"No records found in df \
                  using selection: {selection_dict}"
            )
        return _df


def dict_to_query(
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


def _df_missing_cols(df, cols):
    return [col for col in cols if col not in df.columns]


def _s1_missing_s2_vals(s1, s2):
    return set(s1) - set(s2)


def _common_df_cols(df1, df2):
    return [col for col in df1.columns if col in df2.columns]


def update_df_by_col_value(
    destination_df: pd.DataFrame,
    source_df: pd.DataFrame,
    join_col: str,
    properties: list[str] = None,
    source_must_update_all: bool = True,
) -> pd.DataFrame:
    """Updates destination_df will ALL values in source_df for specified props with same join_col.

    Source_df can contain a subset of IDs of destination_df.
    If source_must_update_all is true, destination_df must have all
    the IDS in source DF - ensuring all source_df values are contained in resulting df.

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
        properties = _common_df_cols(destination_df, source_df).drop(join_col)
    else:
        _dest_miss = _df_missing_cols(destination_df, properties + [join_col])
        if _dest_miss:
            raise ValueError(f"Properties missing from destination_df: {_dest_miss}")
        _source_miss = _df_missing_cols(source_df, properties + [join_col])
        if _source_miss:
            raise ValueError(f"Properties missing from source_df: {_source_miss}")

    if source_must_update_all:
        _source_ids_miss = _s1_missing_s2_vals(source_df[join_col], destination_df[join_col])
        if _source_ids_miss:
            raise ValueError(f"IDs missing from source_df:\n{_source_ids_miss}")

    merged_df = destination_df.merge(
        source_df, on=join_col, how="left", suffixes=("", "_updated")
    )
    for prop in properties:
        merged_df[prop] = merged_df[f"{prop}_updated"].combine_first(merged_df[prop])
    updated_df = merged_df.drop([f"{prop}_updated" for prop in properties], axis=1)

    return updated_df


def list_like_columns(df, item_type: type = None) -> list[str]:
    """Find columns in a dataframe that contain list-like items that can't be json-serialized.

    args:
        df: dataframe to check
        item_type: if not None, will only return columns where all items are of this type by
            checking **only** the first item in the column.  Defaults to None.
    """
    list_like_columns = []

    for column in df.columns:
        if df[column].apply(lambda x: isinstance(x, (list, ndarray))).any():
            if item_type is not None:
                if not isinstance(df[column].iloc[0], item_type):
                    continue
            list_like_columns.append(column)
    return list_like_columns


def diff_dfs(df1, df2, ignore: list[str] = []) -> bool:
    """Compare two dataframes and log differences."""

    diff = False
    if not set(df1.columns) == set(df2.columns):
        WranglerLogger.info(
            f" Columns are different 1vs2 \n    {set(df1.columns) ^ set(df2.columns)}"
        )
        common_cols = [col for col in df1.columns if col in df2.columns]
        df1 = df1[common_cols]
        df2 = df2[common_cols]
        diff = True

    cols_to_compare = [col for col in df1.columns if col not in ignore]
    df1 = df1[cols_to_compare]
    df2 = df2[cols_to_compare]

    if not len(df1) == len(df2):
        WranglerLogger.info(
            f" Length is different /" f"DF1: {len(df1)} vs /" f"DF2: {len(df2)}\n /"
        )
        diff = True

    llcs = list_like_columns(df1)

    diff_df = df1[df1.columns.difference(llcs)].compare(
        df2[df2.columns.difference(llcs)]
    )

    if not diff_df.empty:
        diff = True
        WranglerLogger.error(f"!!! Differences dfs:\n{diff_df}")

    for llc in llcs:
        WranglerLogger.info(f"...checking list-like column: {llc}")
        diff_s = _diff_list_like_series(df1[llc], df2[llc])
        if diff_s:
            diff = True

    if not diff:
        WranglerLogger.info("...no differences in df found.")
    return diff


def _diff_list_like_series(s1, s2) -> bool:
    """Compare two series that contain list-like items as strings"""
    diff_df = pd.concat([s1, s2], axis=1, keys=["s1", "s2"])
    diff_df["diff"] = diff_df.apply(lambda x: str(x["s1"]) != str(x["s2"]), axis=1)

    if diff_df["diff"].any():
        WranglerLogger.info("List-Like differences:")
        WranglerLogger.info(diff_df)
        return True
    return False


def segment_data_by_selection(
    item_list: list,
    data: Union[list, pd.DataFrame, pd.Series],
    field: str = None,
    end_val=0,
) -> tuple[Union[pd.Series, list, pd.DataFrame]]:
    """Segment a dataframe or series into before, middle, and end segments based on item_list.

    selected segment = everything from the first to last item in item_list inclusive of the first
        and last items.
    Before segment = everything before
    After segment = everything after


    Args:
        item_list (list): List of items to segment data by. If longer than two, will only
            use the first and last items.
        data (Union[pd.Series, pd.DataFrame]): Data to segment into before, middle, and after.
        field (str, optional): If a dataframe, specifies which field to reference.
            Defaults to None.
        end_val (int, optional): Notation for util the end or from the begining. Defaults to 0.

    Raises:
        ValueError: If item list isn't found in data in correct order.

    Returns:
        tuple: data broken out by beofore, selected segment, and after.
    """
    ref_data = data
    if isinstance(data, pd.DataFrame):
        ref_data = data[field].tolist()
    elif isinstance(data, pd.Series):
        ref_data = data.tolist()

    # ------- Replace "to the end" indicators with first or last value --------
    start_item, end_item = item_list[0], item_list[-1]
    if start_item == end_val:
        start_item = ref_data[0]
    if end_item == end_val:
        end_item = ref_data[-1]

    # --------Find the start and end indices -----------------------------------
    start_idxs = list(set([i for i, item in enumerate(ref_data) if item == start_item]))
    if not start_idxs:
        raise ValueError(f"Segment start item: {start_item} not found in data.")
    if len(start_idxs) > 1:
        WranglerLogger.warning(
            f"Found multiple starting locations for data segment: {start_item}.\
                                Choosing first – largest segment being selected."
        )
    start_idx = min(start_idxs)

    # find the end node starting from the start index.
    end_idxs = [
        i + start_idx for i, item in enumerate(ref_data[start_idx:]) if item == end_item
    ]
    # WranglerLogger.debug(f"End indexes: {end_idxs}")
    if not end_idxs:
        raise ValueError(
            f"Segment end item: {end_item} not found in data after starting idx."
        )
    if len(end_idxs) > 1:
        WranglerLogger.warning(
            f"Found multiple ending locations for data segment: {end_item}.\
                                Choosing last – largest segment being selected."
        )
    end_idx = max(end_idxs) + 1
    # WranglerLogger.debug(f"Segmenting data from {start_item} idx:{start_idx} to {end_item} idx:{end_idx}.\n{ref_data}")
    # -------- Extract the segments --------------------------------------------
    if isinstance(data, pd.DataFrame):
        before_segment = data.iloc[:start_idx]
        selected_segment = data.iloc[start_idx:end_idx]
        after_segment = data.iloc[end_idx:]
    else:
        before_segment = data[:start_idx]
        selected_segment = data[start_idx:end_idx]
        after_segment = data[end_idx:]

    if isinstance(data, pd.Series) or isinstance(data, pd.DataFrame):
        before_segment = before_segment.reset_index(drop=True)
        selected_segment = selected_segment.reset_index(drop=True)
        after_segment = after_segment.reset_index(drop=True)

    # WranglerLogger.debug(f"Segmented data into before, selected, and after.\n \
    #    Before:\n{before_segment}\nSelected:\n{selected_segment}\nAfter:\n{after_segment}")

    return before_segment, selected_segment, after_segment


def segment_data_by_selection_min_overlap(
    selection_list: list,
    data: pd.DataFrame,
    field: str,
    replacements_list: list,
    end_val=0,
) -> tuple[list, tuple[Union[pd.Series, list, pd.DataFrame]]]:
    """Segments data based on item_list reducing overlap with replacement list.

    selected segment = everything from the first to last item in item_list inclusive of the first
        and last items but not if first and last items overlap with replacement list.
    Before segment = everything before
    After segment = everything after


    Args:
        selection_list (list): List of items to segment data by. If longer than two, will only
            use the first and last items.
        data (Union[pd.Series, pd.DataFrame]): Data to segment into before, middle, and after.
        field (str): Specifies which field to reference.
        replacements_list (list): List of items to eventually replace the selected segment with.
        end_val (int, optional): Notation for util the end or from the begining. Defaults to 0.

    Returns: tuple containing:
        - updated replacement_list
        - tuple of before, selected segment, and after data
    """

    before_segment, segment_df, after_segment = segment_data_by_selection(
        selection_list, data, field=field, end_val=end_val
    )

    if replacements_list[0] == segment_df[field].iat[0]:
        # move first item from selected segment to the before_segment df
        replacements_list = replacements_list[1:]
        before_segment = pd.concat(
            [before_segment, segment_df.iloc[:1]], ignore_index=True, sort=False
        )
        WranglerLogger.debug(
            f"item start overlaps with replacement. Repl: {replacements_list}"
        )
    if replacements_list and replacements_list[-1] == data[field].iat[-1]:
        # move last item from selected segment to the after_segment df
        replacements_list = replacements_list[:-1]
        after_segment = pd.concat(
            [data.iloc[-1:], after_segment], ignore_index=True, sort=False
        )
        WranglerLogger.debug(
            f"item end overlaps with replacement. Repl: {replacements_list}"
        )

    return replacements_list, (before_segment, segment_df, after_segment)
