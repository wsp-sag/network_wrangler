"""Complex roadway types defined using Pydantic models to facilitation validation."""

from __future__ import annotations

from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    PositiveInt,
    RootModel,
    conlist,
    ConfigDict,
    model_validator,
)
from datetime import datetime

from typing import Optional, Union

from ...time import Timespan
from .._base.records import RecordModel
from .._base.root import RootListMixin
from .._base.geo import LatLongCoordinates
from .._base.types import TimeString
from ...utils.time import str_to_time_list, dt_overlaps
from ...params import DEFAULT_CATEGORY, DEFAULT_TIMESPAN

from ...logger import WranglerLogger


class ScopeLinkValueError(Exception):
    """Raised when there is an issue with ScopedLinkValueList."""

    pass


class ScopedLinkValueItem(RecordModel):
    """Define a link property scoped by timespan or category."""

    require_any_of = ["category", "timespan"]
    model_config = ConfigDict(extra="forbid")
    category: Optional[Union[str, int]] = Field(default=DEFAULT_CATEGORY)
    timespan: Optional[list[TimeString]] = Field(default=DEFAULT_TIMESPAN)
    value: Union[int, float, str]

    @property
    def timespan_dt(self) -> list[list[datetime]]:
        """Convert timespan to list of datetime objects."""
        return str_to_time_list(self.timespan)


class ScopedLinkValueList(RootListMixin, RootModel):
    """List of non-conflicting ScopedLinkValueItems."""

    root: list[ScopedLinkValueItem]

    def overlapping_timespans(self, timespan: Timespan):
        """Identify overlapping timespans in the list."""
        timespan_dt = str_to_time_list(timespan)
        return [i for i in self if dt_overlaps(i.timespan_dt, timespan_dt)]

    @model_validator(mode="after")
    def check_conflicting_scopes(self):
        """Check for conflicting scopes in the list."""
        conflicts = []
        for i in self:
            if i.timespan == DEFAULT_TIMESPAN:
                continue
            overlapping_ts_i = self.overlapping_timespans(i.timespan)
            for j in overlapping_ts_i:
                if j == i:
                    continue
                if j.category == i.category:
                    conflicts.append((i, j))
        if conflicts:
            WranglerLogger.error("Found conflicting scopes in ScopedPropertySetList:\n{conflicts}")
            raise ScopeLinkValueError("Conflicting scopes in ScopedPropertySetList")

        return self


class LocationReference(BaseModel):
    """SharedStreets-defined object for location reference."""

    sequence: PositiveInt
    point: LatLongCoordinates
    bearing: float = Field(None, ge=-360, le=360)
    distanceToNextRef: NonNegativeFloat
    intersectionId: str


LocationReferences = conlist(LocationReference, min_length=2)
"""List of at least two LocationReferences which define a path.
"""
