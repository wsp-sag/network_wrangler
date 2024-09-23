"""Wrapper function for applying roadway deletion project card to RoadwayNetwork."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pandas as pd

from ...logger import WranglerLogger

from ...models.projects.roadway_deletion import RoadwayDeletion

if TYPE_CHECKING:
    from ..network import RoadwayNetwork
    from ..network import TransitNetwork


class RoadwayDeletionError(Exception):
    """Raised when there is an issue with applying a roadway deletion."""

    pass


def apply_roadway_deletion(
    roadway_net: RoadwayNetwork,
    roadway_deletion: RoadwayDeletion,
) -> RoadwayNetwork:
    """Delete the roadway links or nodes defined in the project card.

    If deleting links and specified in RoadwayDeletion, will also clean up the shapes and nodes
    used by links. Defaults to not cleaning up shapes or nodes.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_deletion: dictionary conforming to RoadwayDeletion
    """
    r_del = RoadwayDeletion(**roadway_deletion)
    WranglerLogger.debug(f"Deleting Roadway Features: \n{r_del}")

    if r_del.links:
        roadway_net.delete_links(
            r_del.links.model_dump(exclude_none=True, by_alias=True),
            clean_shapes=r_del.clean_shapes,
            clean_nodes=r_del.clean_nodes,
        )

    if r_del.nodes:
        roadway_net.delete_nodes(
            r_del.nodes.model_dump(exclude_none=True, by_alias=True),
        )

    return roadway_net

def check_broken_transit_shapes(
    roadway_net: RoadwayNetwork,
    roadway_deletion: RoadwayDeletion,
    transit_net: TransitNetwork
):
    """Check if any transit shapes go on the deleted links.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_addition: dictionary conforming to RoadwayAddition model
        transit_net: input TransitNetwork

    returns: Broken shape dataframe. Empty if no broken shapes
    """
    deleted_link_id_list = roadway_deletion["links"]["model_link_id"]
    deleted_links_df = roadway_net.links_df[roadway_net.links_df["model_link_id"].isin(deleted_link_id_list)]
    shapes_df = transit_net.feed.shapes.copy()

    # sort the shapes_df by agency_raw_name, shape_id and shape_pt_sequence
    if "agency_raw_name" in shapes_df.columns:
        shapes_df = shapes_df.sort_values(["agency_raw_name", "shape_id", "shape_pt_sequence"])
    else:
        shapes_df = shapes_df.sort_values(["shape_id", "shape_pt_sequence"])
    # create shape_model_node_id_next column by using the value of the next row's shape_model_node_id
    shapes_df["shape_model_node_id_next"] = shapes_df["shape_model_node_id"].shift(-1)
    # create shape_id_next column by using the value of the next row's shape_id
    shapes_df["shape_id_next"] = shapes_df["shape_id"].shift(-1)
    # keep rows with the same shape_id_next and the shape_id
    shapes_df = shapes_df[shapes_df["shape_id_next"] == shapes_df["shape_id"]]
    # make sure shape_model_node_id_next and shape_model_node_id_next are numeric
    shapes_df["shape_model_node_id_next"] = pd.to_numeric(shapes_df["shape_model_node_id_next"])
    shapes_df["shape_model_node_id"] = pd.to_numeric(shapes_df["shape_model_node_id"])

    shapes_df = shapes_df.merge(
        deleted_links_df[["model_link_id", "A", "B"]], 
        how="left",
        left_on=["shape_model_node_id", "shape_model_node_id_next"], 
        right_on=["A", "B"]
    )
    shapes_df = shapes_df[shapes_df["model_link_id"].notna()]

    return shapes_df