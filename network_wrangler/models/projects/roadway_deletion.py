"""For roadway deletion project card (e.g. to delete)."""

from __future__ import annotations

from typing import Optional, ClassVar

from pydantic import ConfigDict, field_validator

from .._base.types import AnyOf
from .._base.records import RecordModel
from .roadway_selection import SelectLinksDict, SelectNodesDict
from ...params import DEFAULT_SEARCH_MODES, DEFAULT_DELETE_MODES


class RoadwayDeletion(RecordModel):
    """Requirements for describing roadway deletion project card (e.g. to delete)."""

    require_any_of: ClassVar[AnyOf] = [["links", "nodes"]]
    model_config = ConfigDict(extra="forbid")

    links: Optional[SelectLinksDict] = None
    nodes: Optional[SelectNodesDict] = None
    clean_shapes: Optional[bool] = False
    clean_nodes: Optional[bool] = False

    @field_validator("links")
    @classmethod
    def set_to_all_modes(cls, links: Optional[SelectLinksDict] = None):
        """Set the search mode to 'any' if not specified explicitly."""
        if links is not None:
            if links.modes == DEFAULT_SEARCH_MODES:
                links.modes = DEFAULT_DELETE_MODES
        return links
