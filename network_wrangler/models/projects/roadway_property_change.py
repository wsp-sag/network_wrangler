"""Pydantic models for roadway property changes which align with ProjectCard schemas."""

from __future__ import annotations

import itertools

from typing import Optional, ClassVar, Any, Union
from datetime import datetime

from pandera import DataFrameModel, Field
from pandera.typing import Series
from pydantic import (
    ConfigDict,
    model_validator,
    BaseModel,
    ValidationError,
    RootModel,
    validate_call,
)


from ...params import LAT_LON_CRS, DEFAULT_CATEGORY, DEFAULT_TIMESPAN
from .._base.records import RecordModel
from .._base.root import RootListMixin
from .._base.types import OneOf
from .._base.types import TimespanString
from ...utils.time import str_to_time_list, dt_overlaps

from ...logger import WranglerLogger


class ScopeConflictError(Exception):
    """Raised when there is a scope conflict in a list of ScopedPropertySetItems."""

    pass


class IndivScopedPropertySetItem(BaseModel):
    """Value for setting property value for a single time of day and category."""

    model_config = ConfigDict(extra="forbid", exclude_none=True)

    category: Optional[Union[str, int]] = DEFAULT_CATEGORY
    timespan: Optional[TimespanString] = DEFAULT_TIMESPAN
    set: Optional[Any] = None
    existing: Optional[Any] = None
    change: Optional[Union[int, float]] = None
    _examples = [
        {"category": "hov3", "timespan": ["6:00", "9:00"], "set": 2.0},
        {"category": "hov2", "set": 2.0},
        {"timespan": ["12:00", "2:00"], "change": -1},
    ]

    @property
    def timespan_dt(self) -> list[list[datetime]]:
        """Convert timespan to list of datetime objects."""
        return str_to_time_list(self.timespan)

    @model_validator(mode="before")
    @classmethod
    def check_set_or_change(cls, data: dict):
        """Validate that each item has a set or change value."""
        if not isinstance(data, dict):
            return data
        if data.get("set") and data.get("change"):
            WranglerLogger.warning("Both set and change are set. Ignoring change.")
            data["change"] = None

        WranglerLogger.debug(f"Data: {data}")
        if data.get("set", None) is None and data.get("change", None) is None:
            WranglerLogger.debug(
                f"Must have `set` or `change` in IndivScopedPropertySetItem. \
                           Found: {data}"
            )
            raise ValueError("Must have `set` or `change` in IndivScopedPropertySetItem")
        return data

    @model_validator(mode="before")
    @classmethod
    def check_categories_or_timespans(cls, data: Any) -> Any:
        """Validate that each item has a category or timespan value."""
        if not isinstance(data, dict):
            return data
        require_any_of = ["category", "timespan"]
        if not any([attr in data for attr in require_any_of]):
            raise ValidationError(f"Require at least one of {require_any_of}")
        return data


class GroupedScopedPropertySetItem(BaseModel):
    """Value for setting property value for a single time of day and category."""

    model_config = ConfigDict(extra="forbid", exclude_none=True)

    category: Optional[Union[str, int]] = None
    timespan: Optional[TimespanString] = None
    categories: Optional[list[Any]] = []
    timespans: Optional[list[TimespanString]] = []
    set: Optional[Any] = None
    existing: Optional[Any] = None
    change: Optional[Union[int, float]] = None
    _examples = [
        {"category": "hov3", "timespan": ["6:00", "9:00"], "set": 2.0},
        {"category": "hov2", "set": 2.0},
        {"timespan": ["12:00", "2:00"], "change": -1},
    ]

    @model_validator(mode="before")
    @classmethod
    def check_set_or_change(cls, data: dict):
        """Validate that each item has a set or change value."""
        if not isinstance(data, dict):
            return data
        if "set" in data and "change" in data:
            WranglerLogger.warning("Both set and change are set. Ignoring change.")
            data["change"] = None
        return data

    @model_validator(mode="before")
    @classmethod
    def check_categories_or_timespans(cls, data: Any) -> Any:
        """Validate that each item has a category or timespan value."""
        if not isinstance(data, dict):
            return data
        require_any_of = ["category", "timespan", "categories", "timespans"]
        if not any([attr in data for attr in require_any_of]):
            raise ValidationError(f"Require at least one of {require_any_of}")
        return data


@validate_call
def _grouped_to_indiv_list_of_scopedpropsetitem(
    scoped_prop_set_list: list[Union[GroupedScopedPropertySetItem, IndivScopedPropertySetItem]],
) -> list[IndivScopedPropertySetItem]:
    """Converts a list of ScopedPropertySetItem to a list of IndivScopedPropertySetItem.

    If neither category or categories exists, replace with DEFAULT_CATEGORY
    If neither timespan or timespans exists, replace with DEFAULT_TIMESPAN

    Because each ScopedPropertySetItem can have multiple categories and timespans, this function
    explodes the list of ScopedPropertySetItem into a list of IndivScopedPropertySetItem, where
    each IndivScopedPropertySetItem has a single category and timespan, consistent with how it is
    stored at the link-level variables.
    """
    indiv_items = []
    for item in scoped_prop_set_list:
        if isinstance(item, IndivScopedPropertySetItem):
            indiv_items.append(item)
            continue

        # Create full lists of all categories and timespans
        categories = item.categories
        if item.category:
            categories.append(item.category)
        if not categories:
            categories = [DEFAULT_CATEGORY]

        timespans = item.timespans
        if item.timespan:
            timespans.append(item.timespan)
        if not timespans:
            timespans = [DEFAULT_TIMESPAN]

        for c, t in itertools.product(categories, timespans):
            indiv_item = IndivScopedPropertySetItem(
                category=c, timespan=t, set=item.set, change=item.change
            )
            indiv_items.append(indiv_item)

    return indiv_items


class ScopedPropertySetList(RootListMixin, RootModel):
    """List of ScopedPropertySetItems used to evaluate and apply changes to roadway properties."""

    root: list[IndivScopedPropertySetItem]

    @model_validator(mode="before")
    @classmethod
    def check_set_or_change(cls, data: list):
        """Validate that each item has a set or change value."""
        data = _grouped_to_indiv_list_of_scopedpropsetitem(data)
        return data

    @model_validator(mode="after")
    def check_conflicting_scopes(self):
        """Check for conflicting scopes in the list of ScopedPropertySetItem."""
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
            raise ScopeConflictError("Conflicting scopes in ScopedPropertySetList")

        return self

    def overlapping_timespans(self, timespan: TimespanString) -> list[IndivScopedPropertySetItem]:
        """Return a list of items that overlap with the given timespan."""
        timespan_dt = str_to_time_list(timespan)
        return [i for i in self if dt_overlaps(i.timespan_dt, timespan_dt)]

    @property
    def change_items(self) -> list[IndivScopedPropertySetItem]:
        """Filter out items that do not have a change value."""
        WranglerLogger.debug(f"self.root[0]: {self.root[0]}")
        return [i for i in self if i.change is not None]

    @property
    def set_items(self):
        """Filter out items that do not have a set value."""
        return [i for i in self if i.set is not None]


class RoadPropertyChange(RecordModel):
    """Value for setting property value for a time of day and category."""

    model_config = ConfigDict(extra="forbid", exclude_none=True)

    existing: Optional[Any] = None
    change: Optional[Union[int, float]] = None
    set: Optional[Any] = None
    scoped: Optional[Union[None, ScopedPropertySetList]] = None

    require_one_of: ClassVar[OneOf] = [["change", "set"]]

    _examples = [
        {"set": 1},
        {"existing": 2, "change": -1},
        {
            "set": 0,
            "scoped": [
                {"timespan": ["6:00", "9:00"], "value": 2.0},
                {"timespan": ["9:00", "15:00"], "value": 4.0},
            ],
        },
        {
            "set": 0,
            "scoped": [
                {
                    "categories": ["hov3", "hov2"],
                    "timespan": ["6:00", "9:00"],
                    "value": 2.0,
                },
                {"category": "truck", "timespan": ["6:00", "9:00"], "value": 4.0},
            ],
        },
        {
            "set": 0,
            "scoped": [
                {"categories": ["hov3", "hov2"], "value": 2.0},
                {"category": "truck", "value": 4.0},
            ],
        },
    ]


class NodeGeometryChangeTable(DataFrameModel):
    """DataFrameModel for setting node geometry given a model_node_id."""

    model_node_id: Series[int]
    X: Series[float] = Field(coerce=True)
    Y: Series[float] = Field(coerce=True)
    in_crs: Series[int] = Field(default=LAT_LON_CRS)

    class Config:
        """Config for NodeGeometryChangeTable."""

        add_missing_columns = True


class NodeGeometryChange(RecordModel):
    """Value for setting node geometry given a model_node_id."""

    model_config = ConfigDict(extra="ignore")
    X: float
    Y: float
    in_crs: Optional[int] = LAT_LON_CRS
