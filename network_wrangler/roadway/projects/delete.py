from ...logger import WranglerLogger


class RoadwayDeletionError(Exception):
    pass


def apply_roadway_deletion(
    roadway_net: "RoadwayNetwork",
    roadway_deletion: dict,
    ignore_missing=True,
) -> "RoadwayNetwork":
    """
    Delete the roadway links or nodes defined in the project card.

    Corresponding shapes to the deleted links are also deleted if they are not used elsewhere.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_deletion:
        ignore_missing: bool
            If True, will only warn about links/nodes that are missing from
            network but specified to "delete" in project card
            If False, will fail.
    """
    del_links, del_nodes = roadway_deletion.get("links", []), roadway_deletion.get(
        "nodes", []
    )
    if not del_links and not del_nodes:
        raise RoadwayDeletionError("No links or nodes given to add.")
    WranglerLogger.debug(
        f"Deleting Roadway Features:\n-Links:\n{del_links}\n-Nodes:\n{del_nodes}"
    )

    if del_links:
        roadway_net.delete_links(del_links, ignore_missing=ignore_missing)

    if del_nodes:
        roadway_net.delete_nodes(del_nodes, ignore_missing=ignore_missing)

    return roadway_net
