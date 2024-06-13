"""Utilities for filtering and querying scoped properties based on scoping dimensions.

This module provides various utility functions for filtering and querying scoped properties based
on scoping dimensions such as category and timespan. It includes functions for filtering scoped
values based on non-overlapping or overlapping timespans, non-overlapping or overlapping
categories, and matching exact category and timespan. It also includes functions for creating
exploded dataframes for scoped properties and filtering them based on scope.

Public Functions:
- prop_for_scope: Creates a dataframe with the value of a property for a given category and
    timespan. Can return maximum overlapping timespan value given a minimum number of overlapping
    minutes, or strictly enforce timespans.

Internal function terminology for scopes:

- `matching` scope value: a scope that could be applied for a given category/timespan combination.
    This includes the default scopes as well as scopes that are contained within the given
    category AND timespan combination.
- `overlapping` scope value: a scope that fully or partially overlaps a given category OR timespan
    combination.  This includes the default scopes, all `matching` scopes and all scopes where
    at lest one minute of timespan or one category overlap.
- `conflicting` scope value: a scope that is overlapping but not matching for a given category/
    timespan. By definition default scope values are not conflicting.
- `independent` scope value: a scope value that is not overlapping.


Usage:

```python
model_links_df["lanes_AM_sov"] = prop_for_scope(links_df, ["6:00":"9:00"], category="sov")
```

"""

import copy
from typing import Union

import pandas as pd
from pydantic import validate_call
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models._base.types import TimeString
from ...models.projects.roadway_property_change import (
    IndivScopedPropertySetItem,
)
from ...models.roadway.tables import RoadLinksTable, ExplodedScopedLinkPropertyTable
from ...models.roadway.types import ScopedLinkValueItem
from ...params import DEFAULT_CATEGORY, DEFAULT_TIMESPAN
from ...utils.time import (
    filter_df_to_overlapping_timespans,
    dt_list_overlaps,
    convert_timespan_to_start_end_dt,
    str_to_time,
    dt_contains,
)


class InvalidScopedLinkValue(Exception):
    """Raised when there is an issue with a scoped link value."""

    pass


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_matching_timespan_scopes(
    scoped_values: list[ScopedLinkValueItem], timespan: list[TimeString]
) -> list[ScopedLinkValueItem]:
    """Filters scoped values to only include those that contain timespan.

    `matching` scope value: a scope that could be applied for a given category/timespan
        combination. This includes the default scopes as well as scopes that are contained within
        the given category AND timespan combination.
    """
    if timespan == DEFAULT_TIMESPAN:
        return scoped_values
    times_dt = list(map(str_to_time, timespan))
    return [
        s
        for s in scoped_values
        if dt_contains([str_to_time(i) for i in s.timespan], times_dt)
        or s.timespan == DEFAULT_TIMESPAN
    ]


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_matching_category_scopes(
    scoped_values: list[ScopedLinkValueItem], category: Union[str, list]
) -> list[ScopedLinkValueItem]:
    """Filters scoped values to only include those that contain given category.

    `matching` scope value: a scope that could be applied for a given category/timespan
        combination. This includes the default scopes as well as scopes that are contained within
        the given category AND timespan combination.
    """
    if category == DEFAULT_CATEGORY:
        return scoped_values
    return [s for s in scoped_values if s.category in category or s.category == DEFAULT_CATEGORY]


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_matching_scope(
    scoped_values: list[ScopedLinkValueItem],
    category: Union[str, list] = DEFAULT_CATEGORY,
    timespan: list[TimeString] = DEFAULT_TIMESPAN,
) -> list[ScopedLinkValueItem]:
    """Filters scoped values to only include those that match category and timespan.

    `matching` scope value: a scope that could be applied for a given category/timespan
        combination. This includes the default scopes as well as scopes that are contained within
        the given category AND timespan combination.
    """
    scoped_values = _filter_to_matching_category_scopes(scoped_values, category)
    scoped_values = _filter_to_matching_timespan_scopes(scoped_values, timespan)
    return scoped_values


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_overlapping_timespan_scopes(
    scoped_values: list[ScopedLinkValueItem], timespan: list[TimeString]
) -> list[ScopedLinkValueItem]:
    """_summary_.

    `overlapping` scope value: a scope that fully or partially overlaps a given category OR
        timespan combination.  This includes the default scopes, all `matching` scopes and
        all scopes where at least one minute of timespan or one category overlap.
    """
    if timespan == DEFAULT_TIMESPAN:
        return scoped_values
    times_dt = list(map(str_to_time, timespan))
    return [
        s
        for s in scoped_values
        if dt_list_overlaps([times_dt, [str_to_time(i) for i in s.timespan]])
        or s.timespan == DEFAULT_TIMESPAN
    ]


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_overlapping_scopes(
    scoped_prop_list: list[Union[ScopedLinkValueItem, IndivScopedPropertySetItem]],
    category: list = DEFAULT_CATEGORY,
    timespan: list[TimeString] = DEFAULT_TIMESPAN,
) -> list[Union[ScopedLinkValueItem, IndivScopedPropertySetItem]]:
    """Filter a list of IndivScopedPropertySetItem and ScopedLinkValueItems to a specific scope.

    Defaults are considered to overlap everything in their scope dimension.

    `overlapping` scope value: a scope that fully or partially overlaps a given category OR
        timespan combination.  This includes the default scopes, all `matching` scopes and
        all scopes where at least one minute of timespan or one category overlap.
    """
    scoped_prop_list = _filter_to_matching_category_scopes(scoped_prop_list, category)
    scoped_prop_list = _filter_to_overlapping_timespan_scopes(scoped_prop_list, timespan)
    return scoped_prop_list


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_conflicting_timespan_scopes(
    scoped_values: list[ScopedLinkValueItem], timespan: list[TimeString]
) -> list[ScopedLinkValueItem]:
    """Filters scoped values to only include those that conflict with the timespan.

    Default timespan does not conflict.

    `matching` scope value: a scope that could be applied for a given category/timespan
        combination. This includes the default scopes as well as scopes that are contained within
        the given category AND timespan combination.
    `overlapping` scope value: a scope that fully or partially overlaps a given category OR
        timespan combination.  This includes the default scopes, all `matching` scopes and
        all scopes where at least one minute of timespan or one category overlap.
    `conflicting` scope value: a scope that is overlapping but not matching for a given category/
        timespan. By definition default scope values are not conflicting.
    """
    overlaps = _filter_to_overlapping_timespan_scopes(scoped_values, timespan)
    matches = _filter_to_matching_timespan_scopes(scoped_values, timespan)

    return [s for s in overlaps if s not in matches]


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _filter_to_conflicting_scopes(
    scoped_values: list[ScopedLinkValueItem],
    timespan: list[TimeString],
    category: Union[str, list[str]],
) -> list[ScopedLinkValueItem]:
    """Filters scoped values to only include those that conflict with the timespan.

    Default timespan and categoies do not conflict.
    NOTE: Categories cannot conflict b/c we are limiting to one per item.

    `matching` scope value: a scope that could be applied for a given category/timespan
        combination. This includes the default scopes as well as scopes that are contained within
        the given category AND timespan combination.
    `overlapping` scope value: a scope that fully or partially overlaps a given category OR
        timespan combination.  This includes the default scopes, all `matching` scopes and
        all scopes where at least one minute of timespan or one category overlap.
    `conflicting` scope value: a scope that is overlapping but not matching for a given category/
        timespan. By definition default scope values are not conflicting.
    """
    if category == DEFAULT_CATEGORY and timespan == DEFAULT_TIMESPAN:
        return scoped_values

    return _filter_to_conflicting_timespan_scopes(scoped_values, timespan)


def _create_exploded_df_for_scoped_prop(
    links_df: DataFrame[RoadLinksTable],
    prop_name: str,
    default_timespan=DEFAULT_TIMESPAN,
    default_category=DEFAULT_CATEGORY,
) -> DataFrame[ExplodedScopedLinkPropertyTable]:
    """Creates a tidy df of `default`,`category`,`timespan`,`start_time`,`end_time`,`scoped`.

    Steps:

        1. Filter to records that have scoped values
        2. Create a record for each scope (explode records)
        3. Normalize dictionary to columns for category, timespan, value (explode columns)
        4. Fill default category
        5. Separate timespan to start_time and end_time of dt type to facilitate queries
        6. Tidy up and align with data model for export
    """
    # 1. Filter to records that have scoped values
    keep_cols = ["model_link_id", prop_name, f"sc_{prop_name}"]
    scoped_values_df = links_df.loc[links_df[f"sc_{prop_name}"].notna(), keep_cols]

    # 2. Create a record for each scope
    exp_df = scoped_values_df.explode(f"sc_{prop_name}")
    WranglerLogger.debug(f"Exploded Records: \n{exp_df}")

    # 3. normalize dictionary to columns for each dictionary key: category, timespan, value
    #       convert to dictionary from data model
    exp_df[f"sc_{prop_name}"] = exp_df[f"sc_{prop_name}"].apply(lambda x: x.model_dump())
    normalized_scope_df = pd.json_normalize(exp_df.pop(f"sc_{prop_name}")).set_index(exp_df.index)
    exp_df = scoped_values_df[["model_link_id"]].join(normalized_scope_df)
    WranglerLogger.debug(f"Exploded columns: \n{exp_df}")

    # 4. Fill default category (timespan query should take care of this itself)
    exp_df.loc[exp_df["category"].isna(), "category"] = default_category

    # 5. Separate timespan to start_time and end_time of dt type to facilitate queries
    exp_df[["start_time", "end_time"]] = convert_timespan_to_start_end_dt(exp_df.timespan)

    # 6. Tidy up and align with data model for export
    exp_df = exp_df.rename(columns={"value": "scoped"})
    exp_df = ExplodedScopedLinkPropertyTable(exp_df)
    WranglerLogger.debug(f"exp_df: \n{exp_df}")

    return exp_df


@validate_call(config=dict(arbitrary_types_allowed=True))
def _filter_exploded_df_to_scope(
    exp_scoped_prop_df: DataFrame[ExplodedScopedLinkPropertyTable],
    prop_name: str,
    timespan: list[TimeString] = DEFAULT_TIMESPAN,
    category: Union[str, int] = DEFAULT_CATEGORY,
    strict_timespan_match: bool = False,
    min_overlap_minutes: int = 60,
) -> pd.DataFrame:
    """Queries an exploded dataframe of prop_name to determine value for scope for each link.

    Args:
        exp_scoped_prop_df: `ExplodedScopedLinkPropertyTable` for property `prop_name `
        prop_name: name of property to query
        timespan: TimespanString of format ['HH:MM','HH:MM'] to query orig_df for overlapping
            records. Defaults to DEFAULT_TIMESPAN.
        category: category to query orig_df for overlapping records. Defaults to DEFAULT_CATEGORY.
        strict_timespan_match: boolean indicating if the returned df should only contain
            records that fully contain the query timespan. If set to True, min_overlap_minutes
            does not apply. Defaults to False.
        min_overlap_minutes: minimum number of minutes the timespans need to overlap to keep.
            Defaults to 0.

    Returns:
        pd.DataFrame with `model_link_id` and `prop_name`
    """
    match_df = exp_scoped_prop_df
    # Filter dataframe based on matching category and timespan
    if category != DEFAULT_CATEGORY:
        match_df = match_df[
            (match_df["category"] == category) | (match_df["category"] == DEFAULT_CATEGORY)
        ]

    if timespan != DEFAULT_TIMESPAN:
        match_df = filter_df_to_overlapping_timespans(
            match_df,
            timespan,
            strict_match=strict_timespan_match,
            min_overlap_minutes=min_overlap_minutes,
        )
    WranglerLogger.debug(f"match_df: \n{match_df}")
    return match_df


@validate_call(config=dict(arbitrary_types_allowed=True))
def prop_for_scope(
    links_df: DataFrame[RoadLinksTable],
    prop_name: str,
    timespan: Union[None, list[TimeString]] = DEFAULT_TIMESPAN,
    category: Union[str, int, None] = DEFAULT_CATEGORY,
    strict_timespan_match: bool = False,
    min_overlap_minutes: int = 60,
    allow_default: bool = True,
) -> pd.DataFrame:
    """Creates a df with the value of a property for a given category and timespan.

    Args:
        links_df:(RoadLinksTable
        prop_name: name of property to query
        timespan: TimespanString of format ['HH:MM','HH:MM'] to query orig_df for overlapping
            records.
        category: category to query orig_df for overlapping records. Defaults to None.
        strict_timespan_match: boolean indicating if the returned df should only contain
            records that fully contain the query timespan. If set to True, min_overlap_minutes
            does not apply. Defaults to False.
        min_overlap_minutes: minimum number of minutes the timespans need to overlap to keep.
            Defaults to 0.
        allow_default: boolean indicating if the default value should be returned if no scoped
            values are found. Defaults to True.

    Returns:
        pd.DataFrame with `model_link_id` and `prop_name`
    """
    timespan = timespan if timespan is not None else DEFAULT_TIMESPAN
    category = category if category is not None else DEFAULT_CATEGORY

    if prop_name not in links_df.columns:
        raise ValueError(f"{prop_name} not in dataframe.")

    # Check if scoped values even exist and if can just return the default.
    if f"sc_{prop_name}" not in links_df.columns or links_df[f"sc_{prop_name}"].isna().all():
        if not allow_default:
            WranglerLogger.error(
                "{prop_name} does not have a scoped property column: \
                                 sc_{prop_name} or it is all null - and allow_default is False."
            )
            raise ValueError(f"sc_{prop_name} not a column/is null - and no default allowed.")
        WranglerLogger.debug(f"No scoped values {prop_name}. Returning default.")
        return copy.deep_copy(links_df[["model_link_id", prop_name]])

    # All possible scopings
    candidate_scoped_prop_df = _create_exploded_df_for_scoped_prop(links_df, prop_name)

    # Find scopes that apply
    scoped_prop_df = _filter_exploded_df_to_scope(
        candidate_scoped_prop_df,
        prop_name,
        timespan=timespan,
        category=category,
        strict_timespan_match=strict_timespan_match,
        min_overlap_minutes=min_overlap_minutes,
    )

    # Attach them back to all links and update default.
    result_df = copy.deepcopy(links_df[["model_link_id", prop_name]])
    result_df.loc[scoped_prop_df.index, prop_name] = scoped_prop_df["scoped"]
    WranglerLogger.debug(
        f"result_df[prop_name]: \n{result_df.loc[scoped_prop_df.index, prop_name]}"
    )
    return result_df
