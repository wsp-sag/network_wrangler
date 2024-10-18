"""Functions for working with nodes tables."""

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ..links.links import node_ids_in_links


def node_ids_without_links(
    nodes_df: DataFrame[RoadNodesTable], links_df: DataFrame[RoadLinksTable]
) -> list[int]:
    """List of node ids that don't have associated links.

    Args:
        nodes_df (DataFrame[RoadNodesTable]): nodes table
        links_df (DataFrame[RoadLinksTable]): links table
    """
    _node_ids_in_links = node_ids_in_links(links_df)
    _node_idx_in_links = nodes_df[nodes_df["model_node_id"].isin(_node_ids_in_links)].index
    return list(set(nodes_df.index) - set(_node_idx_in_links))
