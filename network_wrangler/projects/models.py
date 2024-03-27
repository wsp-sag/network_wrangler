import logging

from typing import Annotated, Any, ClassVar, Dict, List, Union, Optional
from enum import Enum

from pydantic import ConfigDict, Field

from ..utils.models import RecordModel
from ..utils.types import ForcedStr

log = logging.getLogger(__name__)


class SelectRouteProperties(RecordModel):
    """Selection properties for transit routes."""

    model_config = ConfigDict(extra="allow")

    route_short_name: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    route_long_name: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    agency_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    route_type: Annotated[Optional[List[int]], Field(None, min_length=1)]


class SelectionRequire(Enum):
    """Indicator if any or all is required."""

    any = "any"
    all = "all"


class SelectTransitNodes(RecordModel):
    """Requirements for describing multiple transit nodes of a project card (e.g. to delete)."""

    require_any_of: ClassVar = [
        "stop_id",
        "model_node_id",
    ]
    model_config = ConfigDict(extra="forbid")

    stop_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    model_node_id: Annotated[List[int], Field(min_length=1)]
    require: Optional[SelectionRequire] = "any"

    _examples = [
        {"stop_id": ["stop1", "stop2"], "require": "any"},
        {"model_node_id": [1, 2], "require": "all"},
    ]


class TransitABNodesModel(RecordModel):
    """Single transit link model."""

    model_config = ConfigDict(extra="forbid")

    A: Optional[int] = None  # model_node_id
    B: Optional[int] = None  # model_node_id


class SelectTransitLinks(RecordModel):
    """Requirements for describing multiple transit links of a project card."""

    require_one_of: ClassVar = [
        "ab_nodes",
        "model_link_id",
    ]

    model_config = ConfigDict(extra="forbid")

    model_link_id: Annotated[Optional[List[int]], Field(min_length=1)] = None
    ab_nodes: Annotated[Optional[List[TransitABNodesModel]], Field(min_length=1)] = None
    require: Optional[SelectionRequire] = "any"

    _examples = [
        {
            "ab_nodes": [{"A": "75520", "B": "66380"}, {"A": "66380", "B": "75520"}],
            "type": "any",
        },
        {
            "model_link_id": [123, 321],
            "type": "all",
        },
    ]


class SelectTripProperties(RecordModel):
    """Selection properties for transit trips."""

    model_config = ConfigDict(extra="allow")

    trip_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    shape_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    direction_id: int
    service_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    route_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    trip_short_name: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]


TimeString = Annotated[
    ForcedStr,
    Field(
        examples=["12:34", "12:34:56"],
        description="Time of day in 24-hour format. Seconds are optional.",
        pattern="^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$",
    ),
]

Timespan = Annotated[List[TimeString], Field(min_length=2, max_length=2)]


class SelectTransitTrips(RecordModel):
    model_config = ConfigDict(extra="forbid")
    trip_properties: Optional[SelectTripProperties] = None
    route_properties: Optional[SelectRouteProperties] = None
    timespans: Annotated[Optional[List[Timespan]], Field(None, min_length=1)]
    nodes: Optional[SelectTransitNodes] = None
    links: Optional[SelectTransitLinks] = None
