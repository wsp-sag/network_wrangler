from __future__ import annotations

from datetime import time
from typing import Annotated, Any, List, TypeVar, Union, Literal
import pandas as pd
from pydantic import (
    BeforeValidator,
    Field,
)

GeoFileTypes = Union[
    Literal["json"],
    Literal["geojson"],
    Literal["shp"],
    Literal["parquet"],
    Literal["csv"],
    Literal["txt"],
]

TransitFileTypes = Union[Literal["txt"], Literal["csv"], Literal["parquet"]]


RoadwayFileTypes = Union[
    Literal["geojson"],
    Literal["shp"],
    Literal["parquet"],
    Literal["json"],
]


PandasDataFrame = TypeVar("PandasDataFrame", bound=pd.DataFrame)
PandasSeries = TypeVar("PandasSeries", bound=pd.Series)


ForcedStr = Annotated[Any, BeforeValidator(lambda x: str(x))]


OneOf = Annotated[
    List[List[Union[str, List[str]]]],
    Field(
        description=["List fields where at least one is required for the data model to be valid."]
    ),
]

ConflictsWith = Annotated[
    List[List[str]],
    Field(
        description=[
            "List of pairs of fields where if one is present, the other cannot be present."
        ]
    ),
]

AnyOf = Annotated[
    List[List[Union[str, List[str]]]],
    Field(description=["List fields where any are required for the data model to be valid."]),
]

Latitude = Annotated[float, Field(ge=-90, le=90, description="Latitude of stop.")]

Longitude = Annotated[float, Field(ge=-180, le=180, description="Longitude of stop.")]

PhoneNum = Annotated[str, Field("", description="Phone number for the specified location.")]
TimeString = Annotated[
    str,
    Field(
        description="A time string in the format HH:MM or HH:MM:SS",
        pattern=r"^(\d+):([0-5]\d)(:[0-5]\d)?$",
    ),
]
TimespanString = Annotated[
    List[TimeString],
    Field(min_length=2, max_length=2),
]
TimeType = Union[time, str, int]
