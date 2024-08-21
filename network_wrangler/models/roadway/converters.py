"""Converters for roadway networks between data models."""
from __future__ import annotations

from typing import Optional
from pandas import DataFrame

from ...utils.time import str_to_seconds_from_midnight
from ...logger import WranglerLogger


POTENTIAL_COMPLEX_PROPERTIES = ["lanes", "price", "ML_lanes", "ML_price"]


def translate_links_df_v0_to_v1(
    links_df: DataFrame, complex_properties: Optional[list[str]] = None
) -> DataFrame:
    """Translates a links dataframe from v0 to v1 format.

    Args:
        links_df (DataFrame): _description_
        complex_properties (Optional[list[str]], optional): List of complex properties to
            convert from v0 to v1 link data model. Defaults to None. If None, will detect 
            complex properties.
    """
    if complex_properties is None:
        complex_properties = detect_v0_scoped_link_properties(links_df)
    if not complex_properties:
        WranglerLogger.warning("No complex properties detected to convert in links_df.\
                               Returning links_df as-is.")
        return links_df

    for prop in complex_properties:
        links_df = _translate_scoped_link_property_v0_to_v1(links_df, prop)
    return links_df


def translate_links_df_v1_to_v0(
    links_df: DataFrame, complex_properties: Optional[list[str]] = None
) -> DataFrame:
    """Translates a links dataframe from v1 to v0 format.

    Args:
        links_df (DataFrame): _description_
        complex_properties (Optional[list[str]], optional): List of complex properties to
            convert from v0 to v1 link data model. Defaults to None. If None, will detect
            complex properties.
    """
    if complex_properties is None:
        complex_properties = [p for p in links_df.columns if p.startswith("sc_")]
    if not complex_properties:
        WranglerLogger.warning("No complex properties detected to convert in links_df.\
                               Returning links_df as-is.")
        return links_df

    for prop in complex_properties:
        links_df = _translate_scoped_link_property_v1_to_v0(links_df, prop)
    return links_df


# TRANSLATION 0 to 1 ###
def _v0_to_v1_scoped_link_property_item(v0_item: dict) -> dict:
    """Translates a scoped link property item from v0 to v1 format.

    v0 format:
        `value`
        `category` and or `time`

        where `time` is a timespan represented as a tuple(integer) in seconds from midnight

    v1 format:
        `value`
        `category` and or `timespan`

        where `timespan` is represented as a list(str) in 'HH:MM'

    Args:
        v0_item (dict):in v0 format

    Returns:
        dict: in v1 format
    """
    v1_item = {}
    if "time" in v0_item:
        v1_item["timespan"] = [f"{str(t[0]).zfill(2)}:{str(t[1]).zfill(2)}" for t in v0_item["time"]]
    if "category" in v0_item:
        v1_item["category"] = v0_item["category"]
    v1_item["value"] = v0_item["value"]
    return v1_item


def _translate_scoped_link_property_v0_to_v1(links_df: DataFrame, prop: str) -> DataFrame:
    """Translates a scoped link property from v0 to v1 format.

    Args:
        links_df: DataFrame
        prop: str, the property to translate

    Returns:
        DataFrame: the links dataframe with the property translated.
    """
    if prop not in links_df.columns:
        return links_df
    complex_idx = links_df[prop].apply(lambda x: isinstance(x, dict))

    links_df.loc[complex_idx, prop] = links_df.loc[complex_idx, prop].apply(lambda x: x["default"])
    links_df.loc[complex_idx, f"sc_{prop}"] = links_df.loc[complex_idx, prop].apply(
        lambda x: [_v0_to_v1_scoped_link_property_item(i) for i in x.get("timeofday",[])]
    )
    return links_df


# TRANSLATION 1 to 0 ###
def _v1_to_v0_scoped_link_property_item(v1_item: dict) -> dict:
    """Translates a scoped link property item from v1 to v0 format.

    v0 format:
        `value`
        `category` and or `time`

        where `time` is a timespan represented as a tuple(integer) in seconds from midnight

    v1 format:
        `value`
        `category` and or `timespan`

        where `timespan` is represented as a list(str) in 'HH:MM'

    Args:
        v1_item (dict):in v1 format

    Returns:
        dict: in v0 format
    """
    v0_item = {}
    if "time" in v1_item:
        # time is a tuple of seconds from midnight from a tuple of "HH:MM"
        v0_item["time"] = (str_to_seconds_from_midnight(t) for t in v1_item["timespan"])
    if "category" in v1_item:
        v0_item["category"] = v1_item["category"]
    v0_item["value"] = v1_item["value"]
    return v0_item


def _translate_scoped_link_property_v1_to_v0(links_df: DataFrame, prop: str) -> DataFrame:
    """Translates a scoped link property from v0 to v1 format.

    Args:
        links_df: DataFrame
        prop: str, the property to translate

    Returns:
        DataFrame: the links dataframe with the property translated.
    """
    if prop not in links_df.columns:
        return links_df

    default_prop = prop[:3]
    complex_idx = links_df[prop].apply(lambda x: isinstance(x, list))

    v0_prop_s = links_df.loc[complex_idx].apply(
        lambda x: {
            "default": x[default_prop],
            "timeofday": [
                _v0_to_v1_scoped_link_property_item(i)
                for i in x[prop].get("scoped", [])
            ],
        },
    )

    links_df.loc[complex_idx, default_prop] = v0_prop_s
    links_df = links_df.drop(columns=[prop])

    return links_df


# SNIFFS ###
def detect_v0_scoped_link_properties(links_df: DataFrame) -> list[str]:
    """Detects if a links dataframe has scoped properties in v0 format.

    If finds and columns that start with "sc_" will return [].
    If it finds any dictionaries in potential complex properties, it will return a list of them.
    Otherwise, it will return []

    Args:
        links_df: DataFrame

    Returns:
        list[str]: True if the links dataframe is in the v0 format, False otherwise.
    """
    explicit_scoped_columns = [c for c in links_df.columns if c.startswith("sc_")]
    if explicit_scoped_columns:
        return []

    complex_properties = []

    for prop in POTENTIAL_COMPLEX_PROPERTIES:
        if prop not in links_df.columns:
            continue
        if links_df[prop].apply(lambda x: isinstance(x, dict)).any():
            complex_properties.append(prop)

    return complex_properties
