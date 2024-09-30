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

from typing import Optional, Union, ClassVar

from ...time import Timespan
from .._base.records import RecordModel
from .._base.root import RootListMixin
from .._base.geo import LatLongCoordinates
from .._base.types import TimeString, AnyOf
from ...utils.time import str_to_time_list, dt_overlaps
from ...params import DEFAULT_CATEGORY, DEFAULT_TIMESPAN

from ...logger import WranglerLogger


class ScopeLinkValueError(Exception):
    """Raised when there is an issue with ScopedLinkValueList."""

    pass


class ScopedLinkValueItem(RecordModel):
    """Define the value of a link property for a particular timespan or category.

    Attributes:
        `category` (str): Category or link user that this scoped value applies to, ex: `HOV2`,
            `truck`, etc.  Categories are user-defined with the exception of `any` which is
            reserved as the default category. Default is `DEFAULT_CATEGORY`, which is `all`.
        `timespan` (list[TimeString]): timespan of the link property as defined as a list of
            two HH:MM(:SS) strings. Default is `DEFAULT_TIMESPAN`, which is `["00:00", "24:00"]`.
        `value` (Union[float, int, str]): Value of the link property for the given category and
            timespan.

    Conflicting or matching scopes are not allowed in a list of ScopedLinkValueItems:

    - `matching`: a scope that could be applied for a given category/timespan combination. This includes the default scopes as well as scopes that are contained within the given category AND timespan combination.
    - `overlapping`: a scope that fully or partially overlaps a given category OR timespan combination.  This includes the default scopes, all `matching` scopes and all scopes where at least one minute of timespan or one category overlap.
    - `conflicting`: a scope that is overlapping but not matching for a given category/timespan. 

    NOTE: Default scope values of `category: any` and `timespan:["00:00", "24:00"]` are **not** considered conflicting, but are applied to residual scopes.
    """

    require_any_of: ClassVar[AnyOf] = [["category", "timespan"]]
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
