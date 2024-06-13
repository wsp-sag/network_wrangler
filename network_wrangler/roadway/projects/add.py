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
    """Raised when there is an issue with applying a new roadway."""

    pass


def apply_new_roadway(
    roadway_net: RoadwayNetwork,
    roadway_addition: dict,
) -> RoadwayNetwork:
    """Add the new roadway features defined in the project card.

    New nodes are added first so that links can refer to any added nodes.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_addition: dictionary conforming to RoadwayAddition model such as:

        ```json
            {
                "links": [
                    {
                        "model_link_id": 1000,
                        "A": 100,
                        "B": 101,
                        "lanes": 2,
                        "name": "Main St"
                    }
                ],
                "nodes": [
                    {
                        "model_node_id": 100,
                        "X": 0,
                        "Y": 0
                    },
                    {
                        "model_node_id": 101,
                        "X": 0,
                        "Y": 100
                    }
                ],
            }
        ```

    returns: updated network with new links and nodes and associated geometries
    """
    add_links, add_nodes = roadway_addition.get("links", []), roadway_addition.get("nodes", [])
    if not add_links and not add_nodes:
        raise NewRoadwayError("No links or nodes given to add.")

    WranglerLogger.debug(
        f"Adding New Roadway Features: \n-Links: \n{add_links}\n-Nodes: \n{add_nodes}"
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
