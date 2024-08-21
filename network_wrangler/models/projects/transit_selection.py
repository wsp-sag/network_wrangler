"""Pydantic data models for transit selection properties."""

from __future__ import annotations

from typing import Annotated, ClassVar, List, Literal, Optional, Union

from pydantic import ConfigDict, Field

from .._base.records import RecordModel
from .._base.types import ForcedStr, AnyOf, OneOf
from .._base.types import TimespanString


SelectionRequire = Union[Literal["any"], Literal["all"]]


class SelectTripProperties(RecordModel):
    """Selection properties for transit trips."""

    trip_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    shape_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    direction_id: Annotated[Optional[int], Field(None)]
    service_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    route_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    trip_short_name: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]

    model_config = ConfigDict(
        extra="allow",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
    )


class TransitABNodesModel(RecordModel):
    """Single transit link model."""

    A: Optional[int] = None  # model_node_id
    B: Optional[int] = None  # model_node_id

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
    )


class SelectTransitLinks(RecordModel):
    """Requirements for describing multiple transit links of a project card."""

    require_one_of: ClassVar[OneOf] = [
        ["ab_nodes", "model_link_id"],
    ]

    model_link_id: Annotated[Optional[List[int]], Field(min_length=1)] = None
    ab_nodes: Annotated[Optional[List[TransitABNodesModel]], Field(min_length=1)] = None
    require: Optional[SelectionRequire] = "any"

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
    )
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


class SelectTransitNodes(RecordModel):
    """Requirements for describing multiple transit nodes of a project card (e.g. to delete)."""

    require_any_of: ClassVar[AnyOf] = [
        [
            # "stop_id", TODO Not implemented
            "model_node_id",
        ]
    ]

    # stop_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)] TODO Not implemented
    model_node_id: Annotated[List[int], Field(min_length=1)]
    require: Optional[SelectionRequire] = "any"

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
    )

    _examples = [
        # {"stop_id": ["stop1", "stop2"], "require": "any"},  TODO Not implemented
        {"model_node_id": [1, 2], "require": "all"},
    ]


class SelectRouteProperties(RecordModel):
    """Selection properties for transit routes."""

    route_short_name: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    route_long_name: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    agency_id: Annotated[Optional[List[ForcedStr]], Field(None, min_length=1)]
    route_type: Annotated[Optional[List[int]], Field(None, min_length=1)]

    model_config = ConfigDict(
        extra="allow",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
    )


class SelectTransitTrips(RecordModel):
    """Selection properties for transit trips."""

    trip_properties: Optional[SelectTripProperties] = None
    route_properties: Optional[SelectRouteProperties] = None
    timespans: Annotated[Optional[List[TimespanString]], Field(None, min_length=1)]
    nodes: Optional[SelectTransitNodes] = None
    links: Optional[SelectTransitLinks] = None

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        exclude_none=True,
        protected_namespaces=(),
    )
