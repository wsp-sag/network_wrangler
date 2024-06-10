"""Functions for applying roadway link or node addition project cards to the roadway network."""
from __future__ import annotations
from typing import TYPE_CHECKING

import pandas as pd

from ..nodes.create import data_to_nodes_df
from ..links.create import data_to_links_df
from ...logger import WranglerLogger

if TYPE_CHECKING:
    from ..network import RoadwayNetwork


class NewRoadwayError(Exception):
    pass


def apply_new_roadway(
    roadway_net: RoadwayNetwork,
    roadway_addition: dict,
) -> RoadwayNetwork:
    """
    Add the new roadway features defined in the project card.

    New nodes are added first so that links can refer to any added nodes.

    args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_addition:

    returns: updated network with new links and nodes and associated geometries
    """
    add_links, add_nodes = roadway_addition.get("links", []), roadway_addition.get(
        "nodes", []
    )
    if not add_links and not add_nodes:
        raise NewRoadwayError("No links or nodes given to add.")

    WranglerLogger.debug(
        f"Adding New Roadway Features:\n-Links:\n{add_links}\n-Nodes:\n{add_nodes}"
    )
    if add_nodes:
        _new_nodes_df = data_to_nodes_df(
            pd.DataFrame(add_nodes), nodes_params=roadway_net.nodes_df.params
        )
        roadway_net.add_nodes(_new_nodes_df)

    if add_links:
        _new_links_df = data_to_links_df(
            add_links,
            links_params=roadway_net.links_df.params,
            nodes_df=roadway_net.nodes_df,
        )

        roadway_net.add_links(_new_links_df)

    return roadway_net
