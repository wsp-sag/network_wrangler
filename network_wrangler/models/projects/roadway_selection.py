"""Data models for selecting roadway facilities in a project card."""

from __future__ import annotations

from typing import Annotated, ClassVar, Optional

from pydantic import ConfigDict, Field

from ...logger import WranglerLogger
from ...params import DEFAULT_SEARCH_MODES
from .._base.records import RecordModel
from .._base.types import AnyOf, ConflictsWith, OneOf


class RoadwaySelectionFormatError(Exception):
    """Raised when there is an issue with the format of a selection."""


class SelectNodeDict(RecordModel):
    """Selection of a single roadway node in the `facility` section of a project card."""

    require_one_of: ClassVar[OneOf] = [["osm_node_id", "model_node_id"]]
    model_config = ConfigDict(extra="allow")

    osm_node_id: Optional[str] = None
    model_node_id: Optional[int] = None

    _examples: ClassVar[list[dict]] = [{"osm_node_id": "12345"}, {"model_node_id": 67890}]


class SelectNodesDict(RecordModel):
    """Requirements for describing multiple nodes of a project card (e.g. to delete)."""

    require_any_of: ClassVar[AnyOf] = [["osm_node_id", "model_node_id"]]
    model_config = ConfigDict(extra="forbid")

    all: Optional[bool] = False
    osm_node_id: Annotated[Optional[list[str]], Field(None, min_length=1)]
    model_node_id: Annotated[Optional[list[int]], Field(min_length=1)]
    ignore_missing: Optional[bool] = True

    _examples: ClassVar[list[dict]] = [
        {"osm_node_id": ["12345", "67890"], "model_node_id": [12345, 67890]},
        {"osm_node_id": ["12345", "67890"]},
        {"model_node_id": [12345, 67890]},
    ]


class SelectLinksDict(RecordModel):
    """requirements for describing links in the `facility` section of a project card.

    Examples:
    ```python
        {'name': ['Main St'], 'modes': ['drive']}
        {'osm_link_id': ['123456789']}
        {'model_link_id': [123456789], 'modes': ['walk']}
        {'all': 'True', 'modes': ['transit']}
        {'all': 'True', name': ['Main St']}
    ```

    """

    require_conflicts: ClassVar[ConflictsWith] = [
        ["all", "osm_link_id"],
        ["all", "model_link_id"],
        ["all", "name"],
        ["all", "ref"],
        ["osm_link_id", "model_link_id"],
        ["osm_link_id", "name"],
        ["model_link_id", "name"],
    ]
    require_any_of: ClassVar[AnyOf] = [["name", "ref", "osm_link_id", "model_link_id", "all"]]

    model_config = ConfigDict(extra="allow")

    all: Optional[bool] = False
    name: Annotated[Optional[list[str]], Field(None, min_length=1)]
    ref: Annotated[Optional[list[str]], Field(None, min_length=1)]
    osm_link_id: Annotated[Optional[list[str]], Field(None, min_length=1)]
    model_link_id: Annotated[Optional[list[int]], Field(None, min_length=1)]
    modes: list[str] = DEFAULT_SEARCH_MODES
    ignore_missing: Optional[bool] = True

    _examples: ClassVar[list[dict]] = [
        {"name": ["Main St"], "modes": ["drive"]},
        {"osm_link_id": ["123456789"]},
        {"model_link_id": [123456789], "modes": ["walk"]},
        {"all": "True", "modes": ["transit"]},
    ]


class SelectFacility(RecordModel):
    """Roadway Facility Selection."""

    require_one_of: ClassVar[OneOf] = [
        ["links", "nodes", ["links", "from", "to"]],
    ]
    model_config = ConfigDict(extra="forbid")

    links: Optional[SelectLinksDict] = None
    nodes: Optional[SelectNodesDict] = None
    from_: Annotated[Optional[SelectNodeDict], Field(None, alias="from")]
    to: Optional[SelectNodeDict] = None

    _examples: ClassVar[list[dict]] = [
        {
            "links": {"name": ["Main Street"]},
            "from": {"model_node_id": 1},
            "to": {"model_node_id": 2},
        },
        {"nodes": {"osm_node_id": ["1", "2", "3"]}},
        {"nodes": {"model_node_id": [1, 2, 3]}},
        {"links": {"model_link_id": [1, 2, 3]}},
    ]
