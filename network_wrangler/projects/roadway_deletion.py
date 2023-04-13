from typing import Collection

from ..logger import WranglerLogger

apply_roadway_deletion


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
        roadway_net = _delete_links(del_links, ignore_missing)

    if del_nodes:
        roadway_net = _delete_nodes(del_nodes, ignore_missing)

    return roadway_net


def _delete_links(
    roadway_net: "RoadwayNetwork", del_links, dict, ignore_missing=True
) -> None:
    """
    Delete the roadway links based on del_links dictionary selecting links by properties.

    Also deletes shapes which no longer have links associated with them.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        del_links: Dictionary identified shapes to delete by properties.  Links will be selected
            if *any* of the properties are equal to *any* of the values.
        ignore_missing: If True, will only warn if try to delete a links that isn't in network.
            If False, it will fail on missing links. Defaults to True.
    """
    # if RoadwayNetwork.UNIQUE_LINK_ID is used to select, flag links that weren't in network.
    if roadway_net.UNIQUE_LINK_KEY in del_links:
        _del_link_ids = pd.Series(del_links[roadway_net.UNIQUE_LINK_KEY])
        _missing_links = _del_link_ids[
            ~_del_link_ids.isin(roadway_net.links_df[roadway_net.UNIQUE_LINK_KEY])
        ]
        msg = f"Following links cannot be deleted because they are not in the network: {_missing_links}"
        if len(_missing_links) and ignore_missing:
            WranglerLogger.warning(msg)
        elif len(_missing_links):
            raise ValueError(msg)

    _del_links_mask = roadway_net.links_df.isin(del_links).any(axis=1)
    if not _del_links_mask.any():
        WranglerLogger.warning("No links found matching criteria to delete.")
        return
    WranglerLogger.debug(
        f"Deleting following links:\n{roadway_net.links_df.loc[_del_links_mask][['A','B','model_link_id']]}"
    )
    roadway_net.links_df = roadway_net.links_df.loc[~_del_links_mask]

    # Delete shapes which no longer have links associated with them
    _shapes_without_links = roadway_net._shapes_without_links()
    if len(_shapes_without_links):
        WranglerLogger.debug(f"Shapes without links:\n {_shapes_without_links}")
        roadway_net.shapes_df = roadway_net.shapes_df.loc[~_shapes_without_links]
        WranglerLogger.debug(f"self.shapes_df reduced to:\n {roadway_net.shapes_df}")
    return roadway_net


def _delete_nodes(
    roadway_net: "RoadwayNetwork", del_nodes: dict, ignore_missing: bool = True
) -> None:
    """
    Delete the roadway nodes based on del_nodes dictionary selecting nodes by properties.

    Will fail if try to delete node that is currently being used by a link.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        del_nodes : Dictionary identified nodes to delete by properties.  Nodes will be selected
            if *any* of the properties are equal to *any* of the values.
        ignore_missing: If True, will only warn if try to delete a node that isn't in network.
            If False, it will fail on missing nodes. Defaults to True.
    """
    _del_nodes_mask = self.nodes_df.isin(del_nodes).any(axis=1)
    _del_nodes_df = self.nodes_df.loc[_del_nodes_mask]

    if not _del_nodes_mask.any():
        WranglerLogger.warning("No nodes found matching criteria to delete.")
        return

    WranglerLogger.debug(f"Deleting Nodes:\n{_del_nodes_df}")
    # Check if node used in an existing link
    _links_with_nodes = roadway_net.links_with_nodes(
        roadway_net.links_df,
        _del_nodes_df[roadway_net.NODE_FOREIGN_KEY_TO_LINK].tolist(),
    )
    if len(_links_with_nodes):
        WranglerLogger.error(
            f"Node deletion failed because being used in following links:\n{_links_with_nodes[RoadwayNetwork.LINK_FOREIGN_KEY_TO_NODE]}"
        )
        raise ValueError

    # Check if node is in network
    if roadway_net.UNIQUE_NODE_KEY in del_nodes:
        _del_node_ids = pd.Series(del_nodes[roadway_net.UNIQUE_NODE_KEY])
        _missing_nodes = _del_node_ids[
            ~_del_node_ids.isin(roadway_net.nodes_df[roadway_net.UNIQUE_NODE_KEY])
        ]
        msg = f"Following nodes cannot be deleted because they are not in the network: {_missing_nodes}"
        if len(_missing_nodes) and ignore_missing:
            WranglerLogger.warning(msg)
        elif len(_missing_nodes):
            raise ValueError(msg)
    roadway_net.nodes_df = roadway_net.nodes_df.loc[~_del_nodes_mask]
    return roadway_net
