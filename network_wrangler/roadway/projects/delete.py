"""Wrapper function for applying roadway deletion project card to RoadwayNetwork."""

from __future__ import annotations
from typing import TYPE_CHECKING, Union, Optional


from ...models.projects.roadway_changes import RoadwayDeletion


from ...logger import WranglerLogger

if TYPE_CHECKING:
    from ..network import RoadwayNetwork


class RoadwayDeletionError(Exception):
    """Raised when there is an issue with applying a roadway deletion."""

    pass


def apply_roadway_deletion(
    roadway_net: RoadwayNetwork,
    roadway_deletion: Union[dict, RoadwayDeletion],
) -> RoadwayNetwork:
    """Delete the roadway links or nodes defined in the project card.

    If deleting links and specified in RoadwayDeletion, will also clean up the shapes and nodes
    used by links. Defaults to not cleaning up shapes or nodes.

    Args:
        roadway_net: input RoadwayNetwork to apply change to
        roadway_deletion: dictionary conforming to RoadwayDeletion
    """
    if not isinstance(roadway_deletion, RoadwayDeletion):
        roadway_deletion = RoadwayDeletion(**roadway_deletion)
    WranglerLogger.debug(f"Deleting Roadway Features: \n{roadway_deletion}")

    if roadway_deletion.links:
        roadway_net.delete_links(
            roadway_deletion.links.model_dump(exclude_none=True, by_alias=True),
            clean_shapes=roadway_deletion.clean_shapes,
            clean_nodes=roadway_deletion.clean_nodes,
        )

    if roadway_deletion.nodes:
        roadway_net.delete_nodes(
            roadway_deletion.nodes.model_dump(exclude_none=True, by_alias=True),
        )

    return roadway_net
