"""Functions for deleting nodes from a nodes table."""

from pandera.typing import DataFrame

from ...errors import NodeDeletionError
from ...logger import WranglerLogger
from ...models.roadway.tables import RoadNodesTable


def delete_nodes_by_ids(
    nodes_df: DataFrame[RoadNodesTable], del_node_ids: list[int], ignore_missing: bool = False
) -> DataFrame[RoadNodesTable]:
    """Delete nodes from a nodes table.

    Args:
        nodes_df: DataFrame[RoadNodesTable] to delete nodes from.
        del_node_ids: list of node ids to delete.
        ignore_missing: if True, will not raise an error if a node id to delete is not in
            the network. Defaults to False.
    """
    WranglerLogger.debug(f"Deleting nodse with ids: \n{del_node_ids}")

    _missing = set(del_node_ids) - set(nodes_df.index)
    if _missing:
        msg = "Nodes to delete are not in the network."
        WranglerLogger.warning(msg + f"\n{_missing}")
        if not ignore_missing:
            raise NodeDeletionError(msg)
    return nodes_df.drop(labels=del_node_ids, errors="ignore")
