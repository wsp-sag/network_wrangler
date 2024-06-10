from typing import List

import pandas as pd

from ...logger import WranglerLogger
from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ..links.links import node_ids_in_link_ids, node_ids_in_links


def filter_nodes_to_ids(
    nodes_df: RoadNodesTable, node_ids: List[int]
) -> RoadNodesTable:
    """Filters nodes dataframe by node_ids.

    Args:
        nodes_df (pd.DataFrame): nodes dataframe
        node_ids (List[int]): list of node_ids to filter by.

    Returns:
        pd.DataFrame: filtered nodes dataframe
    """
    return nodes_df.loc[nodes_df["model_node_id"].isin(node_ids)]


def filter_nodes_to_link_ids(
    link_ids: List[int], links_df: RoadLinksTable, nodes_df: RoadNodesTable = None
) -> RoadNodesTable:
    """Returns the nodes in a list of link_ids.

    Args:
        link_ids (List[int]): list of link_ids
        links_df (RoadLinksTable): links dataframe
        nodes_df (RoadNodesTable): nodes dataframe

    Returns:
        pd.DataFrame: nodes dataframe
    """
    _node_ids = node_ids_in_link_ids(link_ids, links_df, nodes_df)
    return filter_nodes_to_ids(nodes_df, _node_ids)


def filter_nodes_to_links(
    links_df: RoadLinksTable, nodes_df: RoadNodesTable
) -> pd.DataFrame:
    _node_ids = node_ids_in_links(links_df, nodes_df)
    nodes_in_links = nodes_df.loc[nodes_df.index.isin(_node_ids)]
    WranglerLogger.debug(f"Selected {len(nodes_in_links)} of {len(nodes_df)} nodes.")
    return nodes_in_links
