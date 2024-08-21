"""Wrapper function for applying roadway deletion project card to RoadwayNetwork."""

from __future__ import annotations
from typing import TYPE_CHECKING


from ...logger import WranglerLogger

from ...models.projects.roadway_deletion import RoadwayDeletion

if TYPE_CHECKING:
    from ..network import RoadwayNetwork


class RoadwayDeletionError(Exception):
    """Raised when there is an issue with applying a roadway deletion."""

    pass


def apply_roadway_deletion(
    roadway_net: RoadwayNetwork,
    roadway_deletion: RoadwayDeletion,
) -> RoadwayNetwork:
    """Delete the roadway links or nodes defined in the project card.

    If deleting links and specified in RoadwayDeletion, will also clean up the shapes and nodes
    used by links. Defaults to not cleaning up shapes or nodes.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_deletion: dictionary conforming to RoadwayDeletion
    """
    r_del = RoadwayDeletion(**roadway_deletion)
    WranglerLogger.debug(f"Deleting Roadway Features: \n{r_del}")

    if r_del.links:
        roadway_net.delete_links(
            r_del.links.model_dump(exclude_none=True, by_alias=True),
            clean_shapes=r_del.clean_shapes,
            clean_nodes=r_del.clean_nodes,
        )

    if r_del.nodes:
        roadway_net.delete_nodes(
            r_del.nodes.model_dump(exclude_none=True, by_alias=True),
        )

    return roadway_net
