from __future__ import annotations

from datetime import time
from typing import Annotated, Any, Literal, TypeVar, Union

import pandas as pd
from pydantic import (
    BeforeValidator,
    Field,
)

GeoFileTypes = Literal["json", "geojson", "shp", "parquet", "csv", "txt"]

TransitFileTypes = Literal["txt", "csv", "parquet"]


RoadwayFileTypes = Literal["geojson", "shp", "parquet", "json"]


PandasDataFrame = TypeVar("PandasDataFrame", bound=pd.DataFrame)
PandasSeries = TypeVar("PandasSeries", bound=pd.Series)


ForcedStr = Annotated[Any, BeforeValidator(lambda x: str(x))]


OneOf = Annotated[
    list[list[Union[str, list[str]]]],
    Field(
        description=["List fields where at least one is required for the data model to be valid."]
    ),
]

ConflictsWith = Annotated[
    list[list[str]],
    Field(
        description=[
            "List of pairs of fields where if one is present, the other cannot be present."
        ]
    ),
]

AnyOf = Annotated[
    list[list[Union[str, list[str]]]],
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
    list[TimeString],
    Field(min_length=2, max_length=2),
]
TimeType = Union[time, str, int]
