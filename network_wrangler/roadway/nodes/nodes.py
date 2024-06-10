from typing import Union
from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ..links.links import node_ids_in_links


class NotNodesError(Exception):
    pass


def node_ids_without_links(
    nodes_df: RoadNodesTable, links_df: RoadLinksTable
) -> list[int]:
    """List of node ids that don't have associated links."""

    return list(set(nodes_df.index) - set(node_ids_in_links(links_df)))
