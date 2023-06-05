import pandas as pd

from ..logger import WranglerLogger


def apply_roadway_deletion(
    roadway_net: "RoadwayNetwork",
    del_links: dict = None,
    del_nodes: dict = None,
    ignore_missing=True,
) -> "RoadwayNetwork":
    """
    Delete the roadway links or nodes defined in the project card.

    Corresponding shapes to the deleted links are also deleted if they are not used elsewhere.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        del_links : dictionary of identified links to delete
        del_nodes : dictionary of identified nodes to delete
        ignore_missing: bool
            If True, will only warn about links/nodes that are missing from
            network but specified to "delete" in project card
            If False, will fail.
    """

    WranglerLogger.debug(
        f"Deleting Roadway Features:\n-Links:\n{del_links}\n-Nodes:\n{del_nodes}"
    )

    if del_links:
        roadway_net.delete_links(del_links, ignore_missing=ignore_missing)

    if del_nodes:
        roadway_net.delete_nodes(del_nodes, ignore_missing=ignore_missing)

    return roadway_net
