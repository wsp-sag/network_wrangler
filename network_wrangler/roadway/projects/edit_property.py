"""Functions for applying roadway property change project cards to the roadway network."""

from __future__ import annotations
from typing import Union, TYPE_CHECKING

import pandas as pd

from ...logger import WranglerLogger

from ...models.projects.roadway_property_change import (
    RoadPropertyChange,
    NodeGeometryChange,
    NodeGeometryChangeTable,
)
from ..links.edit import edit_link_properties
from ..nodes.edit import edit_node_property
from ..selection import RoadwayNodeSelection, RoadwayLinkSelection

if TYPE_CHECKING:
    from ..network import RoadwayNetwork


class RoadwayPropertyChangeError(Exception):
    """Raised when there is an issue with applying a roadway property change."""

    pass


def _node_geo_change_from_property_changes(
    property_changes: dict[str, RoadPropertyChange],
    node_idx: list[int],
) -> Union[None, NodeGeometryChangeTable]:
    """Return NodeGeometryChangeTable if property_changes includes gometry change else None."""
    geo_change_present = any(f in property_changes for f in ["X", "Y"])
    if not geo_change_present:
        return None
    if len(node_idx) > 1:
        WranglerLogger.error(
            f"! Shouldn't move >1 node to the same geography. Selected {len(node_idx)}"
        )
        raise RoadwayPropertyChangeError("Shouldn't move >1 node to the same geography.")

    if not all(f in property_changes for f in ["X", "Y"]):
        WranglerLogger.error(
            f"! Must provide both X and Y to move node to new location. Got {property_changes}"
        )
        raise RoadwayPropertyChangeError("Must provide both X and Y to move node to new location.")

    geo_changes = {
        k: v["set"] for k, v in property_changes.items() if k in NodeGeometryChange.model_fields
    }
    geo_changes["model_node_id"] = node_idx[0]
    if "in_crs" not in geo_changes:
        geo_changes["in_crs"] = None

    return NodeGeometryChangeTable(pd.DataFrame(geo_changes, index=[0]))


def apply_roadway_property_change(
    roadway_net: RoadwayNetwork,
    selection: Union[RoadwayNodeSelection, RoadwayLinkSelection],
    property_changes: dict[str, RoadPropertyChange],
) -> RoadwayNetwork:
    """Changes roadway properties for the selected features based on the project card.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        selection : roadway selection object
        property_changes : dictionary of roadway properties to change.
            e.g.

            ```yml
            #changes number of lanes 3 to 2 (reduction of 1) and adds a bicycle lane
            lanes:
                existing: 3
                change: -1
            bicycle_facility:
                set: 2
            ```
    """
    WranglerLogger.debug("Applying roadway property change project.")

    if "links" in selection.feature_types:
        roadway_net.links_df = edit_link_properties(
            roadway_net.links_df, selection.selected_links, property_changes
        )

    elif "nodes" in selection.feature_types:
        non_geo_changes = {
            k: v for k, v in property_changes.items() if k not in NodeGeometryChange.model_fields
        }
        for property, property_dict in non_geo_changes.items():
            prop_change = RoadPropertyChange(**property_dict)
            roadway_net.nodes_df = edit_node_property(
                roadway_net.nodes_df, selection.selected_nodes, property, prop_change
            )

        geo_changes_df = _node_geo_change_from_property_changes(
            property_changes, selection.selected_nodes
        )
        if geo_changes_df is not None:
            roadway_net.move_nodes(geo_changes_df)

    else:
        raise RoadwayPropertyChangeError("geometry_type must be either 'links' or 'nodes'")

    return roadway_net
