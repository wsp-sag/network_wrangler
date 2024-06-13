"""Edits RoadLinksTable properties.

NOTE: Each public method will return a new, whole copy of the RoadLinksTable with associated edits.
Private methods may return mutated originals.

Usage:
    # Returns copy of links_df with lanes set to 2 for links in link_idx
    links_df = edit_link_property(links_df, link_idx, "lanes", {"set": 2})

    # Returns copy of links_df with price reduced by 50 for links in link_idx and raises error
    # if existing value doesn't match 100
    links_df = edit_link_properties(
        links_df, link_idx, "price",
        {"existing": 100,"change":-50},
        "existing_value_conflict_error": True
    )

    # Returns copy of links_df with geometry of links with node_ids updated based on nodes_df
    links_df = edit_link_geometry_from_nodes(links_df, nodes_df, node_ids)
"""

from __future__ import annotations

import copy

from typing import Union, Any

from pydantic import validate_call
from pandera.typing import DataFrame

from ...params import LINK_ML_OFFSET_METERS
from ...logger import WranglerLogger
from ...utils.data import validate_existing_value_in_df
from ...models._base.validate import validate_df_to_model
from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ...models.roadway.types import ScopedLinkValueItem
from ...models.projects.roadway_property_change import (
    RoadPropertyChange,
    IndivScopedPropertySetItem,
    ScopedPropertySetList,
)
from ...utils.geo import update_nodes_in_linestring_geometry, _offset_geometry_meters
from ...utils.models import default_from_datamodel
from .scopes import (
    _filter_to_matching_scope,
    InvalidScopedLinkValue,
    _filter_to_conflicting_scopes,
)


class LinkChangeError(Exception):
    """Raised when there is an error in changing a link property."""

    pass


def _initialize_links_as_managed_lanes(
    links_df: DataFrame[RoadLinksTable], link_idx: list[int]
) -> DataFrame[RoadLinksTable]:
    """Initialize links as managed lanes if they are not already."""
    links_df.loc[link_idx, "managed"] = 1
    initialize_if_missing = ["ML_geometry", "ML_access_point", "ML_egress_point"]
    for f in initialize_if_missing:
        if f not in links_df:
            links_df[f] = default_from_datamodel(RoadLinksTable, f)
    _ml_wo_geometry = links_df.loc[links_df["ML_geometry"].isna() & links_df["managed"] == 1].index
    links_df.loc[_ml_wo_geometry, "ML_geometry"] = _offset_geometry_meters(
        links_df.loc[_ml_wo_geometry, "geometry"], LINK_ML_OFFSET_METERS
    )

    return links_df


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _resolve_conflicting_scopes(
    scoped_values: list[ScopedLinkValueItem],
    scoped_item: IndivScopedPropertySetItem,
    delete_conflicting: bool = True,
) -> list[ScopedLinkValueItem]:
    conflicting_existing = _filter_to_conflicting_scopes(
        scoped_values,
        timespan=scoped_item.timespan,
        category=scoped_item.category,
    )
    if conflicting_existing:
        if delete_conflicting:
            return [i for i in scoped_values not in conflicting_existing]
        else:
            WranglerLogger.error(
                f"""Existing link value conflicts with change.  Either update to
                set overwrite_conflicting = True to overwrite with set value
                or update the scoped value to not conflict.\n
                Conflicting existing value(s): {conflicting_existing}\n
                Set value: {scoped_item} """
            )


def _valid_default_value_for_change(value: Any) -> bool:
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return True
    return False


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _update_property_for_scope(
    scoped_prop_set: IndivScopedPropertySetItem, existing_value: Any
) -> ScopedLinkValueItem:
    """Update property for a single scope."""
    if scoped_prop_set.set is not None:
        return ScopedLinkValueItem(
            category=scoped_prop_set.category,
            timespan=scoped_prop_set.timespan,
            value=scoped_prop_set.set,
        )
    elif scoped_prop_set.change is not None:
        if not _valid_default_value_for_change(existing_value):
            WranglerLogger.error(
                f"Cannot implement change from default_value of: {existing_value} of \
                type {type(existing_value)}."
            )
            raise InvalidScopedLinkValue(
                f"Cannot implement change from default_value of type {type(existing_value)}."
            )
        return ScopedLinkValueItem(
            category=scoped_prop_set.category,
            timespan=scoped_prop_set.timespan,
            value=existing_value + scoped_prop_set.change,
        )
    else:
        WranglerLogger.error(
            f"Scoped property change must have set or change. Found: {scoped_prop_set}"
        )
        raise InvalidScopedLinkValue("Scoped property change must have set or change.")


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _edit_scoped_link_property(
    scoped_prop_value_list: Union[None, list[ScopedLinkValueItem]],
    scoped_prop_set: ScopedPropertySetList,
    default_value: Any = None,
    overwrite_all: bool = False,
    overwrite_conflicting: bool = True,
) -> list[ScopedLinkValueItem]:
    """Edit scoped property on a single link.

    Args:
        scoped_prop_value_list: List of scoped property values for the link.
        scoped_prop_set: ScopedPropertySetList of changes to make.
        default_value: Default value for the property if not set.
        overwrite_all: If True, will overwrite all scoped values for link property with
            scoped_prop_value_set. Defaults to False.
        overwrite_conflicting: If True will overwrite any conflicting scopes.  Otherwise, will
            raise an Exception on conflicting, but not matching, scopes.
    """
    # If None, or asked to overwrite all scopes, and return all set items
    if overwrite_all or not scoped_prop_value_list:
        return [_update_property_for_scope(i, default_value) for i in scoped_prop_set]

    # Copy so not iterating over something that is changing
    updated_scoped_prop_value_list = copy.deepcopy(scoped_prop_value_list)

    for set_item in scoped_prop_set:
        WranglerLogger(f"Editing link for scoped item: {set_item}")

        # delete or error on conflicting scopes
        updated_scoped_prop_value_list = _resolve_conflicting_scopes(
            updated_scoped_prop_value_list,
            scoped_prop_set,
            delete_conflicting=overwrite_conflicting,
        )

        # find matching scopes
        matching_existing = _filter_to_matching_scope(
            updated_scoped_prop_value_list,
            timespan=set_item.timespan,
            category=set_item.category,
        )

        # if no matching scope, append the set-value; barf on change on wrong type
        if not matching_existing:
            updated_scoped_prop_value_list.append(
                _update_property_for_scope(set_item, default_value)
            )

        # for each matching scope, implement the set or change
        for match_i in matching_existing:
            updated_scoped_prop_value_list.append(
                _update_property_for_scope(set_item, match_i.value)
            )

    return updated_scoped_prop_value_list


def _edit_ml_access_egress_points(
    links_df: DataFrame[RoadLinksTable],
    prop_name: str,
    prop_change: RoadPropertyChange,
    link_idx: list[int],
):
    """Edit ML access or egress points on links."""
    prop_to_node_col = {"ML_access_point": "A", "ML_egress_point": "B"}
    node_col = prop_to_node_col[prop_name]
    if prop_change.set == "all":
        WranglerLogger.debug(f"Setting all {prop_name} to True")
        links_df.loc[link_idx, prop_name] = True
    elif isinstance(prop_change.set, list):
        mask = (links_df[node_col].isin(prop_change.set)) & (links_df.index.isin(link_idx))

        WranglerLogger.debug(
            f"Setting {prop_name} to True for {len(links_df.loc[mask])} links: \n\
                            {links_df.loc[mask, ['model_link_id', 'A', 'B']]}"
        )
        links_df.loc[mask, prop_name] = True
    else:
        WranglerLogger.error(
            f"Invalid value for {prop_name}. Must be list of ints or \
                                'all': {prop_change.set}"
        )
        raise ValueError(f"Invalid value for {prop_name}: {prop_change.set}")

    return links_df


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def _edit_link_property(
    links_df: DataFrame[RoadLinksTable],
    link_idx: list,
    prop_name: str,
    prop_change: RoadPropertyChange,
    existing_value_conflict_error: bool = False,
    overwrite_all_scoped: bool = False,
    overwrite_conflicting_scoped: bool = True,
) -> DataFrame[RoadLinksTable]:
    """Return edited (in place) RoadLinksTable with property changes for a list of links.

    If prop_name starts with ML, will initialize managed lane attributes if not already.
    If prop_name not in links_df, will initialize to default value from RoadLinksTable model
        if it exists.

    Does NOT validate to RoadLinksTable.

    Args:
        links_df: links to edit
        link_idx: list of link indices to change
        prop_name: property name to change
        prop_change: RoadPropertyChange instance
        existing_value_conflict_error: If True, will trigger an error if the existing
            specified value in the project card doesn't match the value in links_df.
            Otherwise, will only trigger a warning. Defaults to False.
        overwrite_all_scoped: If True, will overwrite all scoped values for link property with
            scoped_prop_value_set. Defaults to False.
        overwrite_conflicting_scoped: If True will overwrite any conflicting scopes.
            Otherwise, will raise an Exception on conflicting, but not matching, scopes.

    """
    WranglerLogger.debug(f"Editing {prop_name} on links {link_idx}")
    # WranglerLogger.debug(f"links_df | link_idx:\n {links_df.loc[link_idx].head()}")
    if prop_change.existing is not None:
        exist_ok = validate_existing_value_in_df(
            links_df, link_idx, prop_name, prop_change.existing
        )
        if not exist_ok and existing_value_conflict_error:
            raise LinkChangeError(f"No existing value defined for {prop_name}")

    # if it is a managed lane field, initialize managed lane attributes if haven't already
    if prop_name.startswith("ML_"):
        links_df = _initialize_links_as_managed_lanes(links_df, link_idx)

    # Initialize new props to None
    if prop_name not in links_df:
        links_df[prop_name] = default_from_datamodel(RoadLinksTable, prop_name)

    WranglerLogger.debug(f"Editing {prop_name} to {prop_change}")
    # WranglerLogger.debug(f"links_df idx|prop_name \
    #   Before:\n {links_df.loc[link_idx,prop_name].head()}")

    # Access and egress points are special cases.
    if prop_name in ["ML_access_point", "ML_egress_point"]:
        links_df = _edit_ml_access_egress_points(links_df, prop_name, prop_change, link_idx)
        WranglerLogger.debug(
            f"links_df.loc[link_idx, prop_name] \
            After: \n {links_df.loc[link_idx, prop_name].head()}"
        )
        return links_df

    # `set` and `change` just affect the simple property
    elif prop_change.set is not None:
        WranglerLogger.debug(f"Setting {prop_name} to {prop_change.set}")
        links_df.loc[link_idx, prop_name] = prop_change.set

    elif prop_change.change is not None:
        WranglerLogger.debug(f"Changing {prop_name} by {prop_change.change}")
        links_df.loc[link_idx, prop_name] += prop_change.change

    if prop_change.scoped is not None:
        # initialize scoped property to default value in RoadLinksTable model or None.
        sc_prop_name = f"sc_{prop_name}"
        WranglerLogger.debug(f"setting {sc_prop_name} to {prop_change.scoped}")
        if sc_prop_name not in links_df:
            links_df[sc_prop_name] = default_from_datamodel(RoadLinksTable, sc_prop_name)
        links_df.loc[link_idx, sc_prop_name] = links_df.loc[link_idx].apply(
            lambda x: _edit_scoped_link_property(
                x[sc_prop_name],
                prop_change.scoped,
                x[prop_name],
                overwrite_all=overwrite_all_scoped,
                overwrite_conflicting=overwrite_conflicting_scoped,
            ),
            axis=1,
        )

    # WranglerLogger.debug(f"links_df.loc[link_idx,prop_name] \
    #   After:\n {links_df.loc[link_idx, prop_name]}")

    return links_df


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def edit_link_property(
    links_df: DataFrame[RoadLinksTable],
    link_idx: list,
    prop_name: str,
    prop_dict: RoadPropertyChange,
    existing_value_conflict_error: bool = False,
) -> DataFrame[RoadLinksTable]:
    """Return copy of RoadLinksTable with edited link property for a list of links IDS.

    Args:
        links_df: links to edit
        link_idx: list of link indices to change
        prop_name: property name to change
        prop_dict: dictionary of value from project_card
        existing_value_conflict_error: If True, will trigger an error if the existing
            specified value in the project card doesn't match the value in links_df.
            Otherwise, will only trigger a warning. Defaults to False.

    """
    WranglerLogger.info(f"Editing Link Property {prop_name} for {len(link_idx)} links.")
    WranglerLogger.debug(f"prop_dict: /n{prop_dict}")
    prop_change = RoadPropertyChange(prop_dict)

    links_df = copy.deepcopy(links_df)
    links_df = _edit_link_property(
        links_df, link_idx, prop_name, prop_change, existing_value_conflict_error
    )
    links_df = validate_df_to_model(links_df, RoadLinksTable)
    WranglerLogger.debug(
        f"Edited links_df.loc[link_idx, property]: \
                         \n {links_df.loc[link_idx, property]}"
    )
    return links_df


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def edit_link_properties(
    links_df: DataFrame[RoadLinksTable],
    link_idx: list,
    property_changes: dict[str, RoadPropertyChange],
    existing_value_conflict_error: bool = False,
) -> DataFrame[RoadLinksTable]:
    """Return copy of RoadLinksTable with edited link properties for a list of links.

    Args:
        links_df: links to edit
        link_idx: list of link indices to change
        property_changes: dictionary of property changes
        existing_value_conflict_error: If True, will trigger an error if the existing
            specified value in the project card doesn't match the value in links_df.
            Otherwise, will only trigger a warning. Defaults to False.

    """
    links_df = copy.deepcopy(links_df)
    # WranglerLogger.debug(f"property_changes: \n{property_changes}")
    for property, prop_change in property_changes.items():
        WranglerLogger.debug(f"prop_dict: \n{prop_change}")
        links_df = _edit_link_property(
            links_df, link_idx, property, prop_change, existing_value_conflict_error
        )

    return links_df


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def edit_link_geometry_from_nodes(
    links_df: DataFrame[RoadLinksTable],
    nodes_df: DataFrame[RoadNodesTable],
    node_ids: list[int],
) -> DataFrame[RoadLinksTable]:
    """Returns a copy of links with updated geometry for given links for a given list of nodes.

    Should be called by any function that changes a node location.

    Args:
        links_df: RoadLinksTable to update
        nodes_df: RoadNodesTable to get updated node geometry from
        node_ids: list of node PKs with updated geometry
    """
    # WranglerLogger.debug(f"nodes_df.loc[node_ids]:\n {nodes_df.loc[node_ids]}")

    links_df = copy.deepcopy(links_df)

    updated_a_geometry = update_nodes_in_linestring_geometry(
        links_df.loc[links_df.A.isin(node_ids)], nodes_df, 0
    )
    links_df.update(updated_a_geometry)

    updated_b_geometry = update_nodes_in_linestring_geometry(
        links_df.loc[links_df.B.isin(node_ids)], nodes_df, -1
    )
    links_df.update(updated_b_geometry)

    _a_or_b_mask = links_df.A.isin(node_ids) | links_df.B.isin(node_ids)
    WranglerLogger.debug(f"links_df: \n{links_df.loc[_a_or_b_mask, ['A', 'B', 'geometry']]}")
    return links_df
