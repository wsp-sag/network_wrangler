"""Functions for editing transit properties in a TransitNetwork."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ...logger import WranglerLogger

if TYPE_CHECKING:
    from ...transit.network import TransitNetwork
    from ...transit.selection import TransitSelection

TABLE_TO_APPLY_BY_PROPERTY = {
    "headway_secs": "frequencies",
}

# Tables which can be selected by trip_id
IMPLEMENTED_TABLES = ["trips", "frequencies", "stop_times"]


class TransitPropertyChangeError(Exception):
    """Error raised when applying transit property changes."""

    pass


def apply_transit_property_change(
    net: TransitNetwork, selection: TransitSelection, property_changes: dict
) -> TransitNetwork:
    """Apply changes to transit properties.

    Args:
        net (TransitNetwork): Network to modify.
        selection (TransitSelection): Selection of trips to modify.
        property_changes (dict): Dictionary of properties to change.

    Returns:
        TransitNetwork: Modified network.
    """
    WranglerLogger.debug("Applying transit property change project.")

    for property, property_change in property_changes.items():
        table = TABLE_TO_APPLY_BY_PROPERTY.get(property)
        if not table:
            table = net.feed.tables_with_field(property)
            if not len(table == 1):
                raise TransitPropertyChangeError(
                    "Found property {property} in multiple tables: {table}"
                )
            table = table[0]
        if not table:
            raise NotImplementedError("No table found to modify: {property}")

        if table not in IMPLEMENTED_TABLES:
            raise NotImplementedError(f"{table} table changes not currently implemented.")

        WranglerLogger.debug(f"...modifying {property} in {table}.")
        net = _apply_transit_property_change_to_table(
            net, selection, table, property, property_change
        )

    return net


def _apply_transit_property_change_to_table(
    net: TransitNetwork,
    selection: TransitSelection,
    table_name: str,
    property: str,
    property_change: dict,
) -> TransitNetwork:
    table_df = net.feed.get_table(table_name)
    # Grab only those records matching trip_ids (aka selection)
    set_df = table_df[table_df.trip_id.isin(selection.selected_trips)].copy()

    # Check all `existing` properties if given
    if "existing" in property_change:
        if not all(set_df[property] == property_change["existing"]):
            WranglerLogger.error(
                f"Existing does not match {property_change['existing']} for at least 1 trip."
            )
            raise TransitPropertyChangeError("Existing does not match.")

    # Calculate build value
    if "set" in property_change:
        set_df["_set_val"] = property_change["set"]
    else:
        set_df["_set_val"] = set_df[property] + property_change["change"]
    set_df[property] = set_df["_set_val"]
    set_df = set_df.drop(columns=["_set_val"])

    # Update in feed
    net.feed.set_by_id(table_name, set_df, id_property="trip_id", properties=[property])

    return net
