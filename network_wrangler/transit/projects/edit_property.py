"""Functions for editing transit properties in a TransitNetwork."""

from __future__ import annotations
import copy

from typing import TYPE_CHECKING, Optional

from ...logger import WranglerLogger

if TYPE_CHECKING:
    from pandas import DataFrame, Series
    from ...transit.network import TransitNetwork
    from ...transit.selection import TransitSelection

TABLE_TO_APPLY_BY_PROPERTY = {
    "headway_secs": "frequencies",
}

IMPLEMENTED_TABLES = ["trips", "frequencies", "stop_times"]


class TransitPropertyChangeError(Exception):
    """Error raised when applying transit property changes."""

    pass


def apply_transit_property_change(
    net: TransitNetwork,
    selection: TransitSelection,
    property_changes: dict,
    project_name: Optional[str] = None,
    existing_value_conflict_error: bool = False,
) -> TransitNetwork:
    """Apply changes to transit properties.

    Args:
        net (TransitNetwork): Network to modify.
        selection (TransitSelection): Selection of trips to modify.
        property_changes (dict): Dictionary of properties to change.
        project_name (str, optional): Name of the project. Defaults to None.
        existing_value_conflict_error (bool, optional): Whether to raise an error if the existing
            value does not match. Defaults to False.

    Returns:
        TransitNetwork: Modified network.
    """
    WranglerLogger.debug("Applying transit property change project.")
    for property, property_change in property_changes.items():
        net = _apply_transit_property_change_to_table(
            net,
            selection,
            property,
            property_change,
            project_name=project_name,
            existing_value_conflict_error=existing_value_conflict_error,
        )
    return net


def _get_table_name_for_property(net: TransitNetwork, property: str) -> str:
    table_name = TABLE_TO_APPLY_BY_PROPERTY.get(property)
    if not table_name:
        table_name = net.feed.tables_with_field(property)
        if not len(table_name == 1):
            raise TransitPropertyChangeError(
                "Found property {property} in multiple tables: {table}"
            )
        table_name = table_name[0]
    if table_name not in IMPLEMENTED_TABLES:
        raise NotImplementedError(f"{table_name} table changes not currently implemented.")
    return table_name


def _check_existing_value(existing_s: Series, expected_existing_val) -> bool:
    # Check all `existing` properties if given

    if not all(existing_s == expected_existing_val):
        WranglerLogger.warning(
            f"Existing do not all match expected value of {expected_existing_val}."
        )
        WranglerLogger.debug(
            f"Conflicting values values: {existing_s[existing_s != expected_existing_val]}"
        )
        return False
    return True


def _apply_transit_property_change_to_table(
    net: TransitNetwork,
    selection: TransitSelection,
    property: str,
    property_change: dict,
    project_name: Optional[str] = None,
    existing_value_conflict_error: bool = False,
) -> TransitNetwork:
    table_name = _get_table_name_for_property(net, property)
    WranglerLogger.debug(f"...modifying {property} in {table_name}.")

    table_df = net.feed.get_table(table_name)

    # Update records matching trip_ids or matching frequencies
    if table_name in ["trips", "stop_times"]:
        update_idx = table_df[table_df.trip_id.isin(selection.selected_trips)].index
    elif table_name == "frequencies":
        update_idx = selection.selected_frequencies_df.index
    else:
        raise NotImplementedError(f"{table_name} table changes not currently implemented.")

    if "existing" in property_change:
        existing_ok = _check_existing_value(
            table_df.loc[update_idx, property], property_change["existing"]
        )
        if not existing_ok:
            WranglerLogger.warning(f"Existing {property} != {property_change['existing']}.")
            if existing_value_conflict_error:
                raise TransitPropertyChangeError("Existing {property} does not match.")

    set_df = copy.deepcopy(table_df)

    # Calculate build value
    if "set" in property_change:
        set_df.loc[update_idx, property] = property_change["set"]
    elif "change" in property_change:
        set_df.loc[update_idx, property] = (
            set_df.loc[update_idx, property] + property_change["change"]
        )
    else:
        raise ValueError("Property change must include 'set' or 'change'.")

    if project_name is not None:
        set_df.loc[update_idx, "projects"] += f"{project_name},"

    # Update in feed
    net.feed.__dict__[table_name] = set_df

    return net
