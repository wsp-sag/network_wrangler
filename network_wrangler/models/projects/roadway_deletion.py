"""For roadway deletion project card (e.g. to delete)."""

from __future__ import annotations

from typing import Optional, ClassVar

from pydantic import ConfigDict

from .._base.types import AnyOf
from .._base.records import RecordModel
from .roadway_selection import SelectLinksDict, SelectNodesDict


class RoadwayDeletion(RecordModel):
    """Requirements for describing roadway deletion project card (e.g. to delete)."""

    require_any_of: ClassVar[AnyOf] = [["links", "nodes"]]
    model_config = ConfigDict(extra="forbid")

    links: Optional[SelectLinksDict] = None
    nodes: Optional[SelectNodesDict] = None
    clean_shapes: Optional[bool] = False
    clean_nodes: Optional[bool] = False
