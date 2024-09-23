"""Functions for working with nodes tables."""

from pandera.typing import DataFrame

from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ..links.links import node_ids_in_links


class NotNodesError(Exception):
    """Raised when the input data is not a nodes table."""

    pass


def node_ids_without_links(
    nodes_df: DataFrame[RoadNodesTable], links_df: DataFrame[RoadLinksTable]
) -> list[int]:
    """List of node ids that don't have associated links.

    Args:
        nodes_df (DataFrame[RoadNodesTable]): nodes table
        links_df (DataFrame[RoadLinksTable]): links table
    """
    return list(set(nodes_df.index) - set(node_ids_in_links(links_df)))
