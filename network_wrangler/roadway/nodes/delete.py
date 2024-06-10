from ...logger import WranglerLogger
from ...models.roadway.tables import RoadNodesTable


class NodeDeletionError(Exception):
    pass


def delete_nodes_by_ids(
    nodes_df: RoadNodesTable, del_node_ids: list[int], ignore_missing: bool = False
) -> RoadNodesTable:
    WranglerLogger.debug(f"Deleting nodse with ids:\n{del_node_ids}")

    _missing = set(del_node_ids) - set(nodes_df.index)
    if _missing:
        WranglerLogger.warning(f"Nodes in network not there to delete: \n{_missing}")
        if not ignore_missing:
            raise NodeDeletionError("Links to delete are not in the network.")
    return nodes_df.drop(labels=del_node_ids, errors="ignore")
