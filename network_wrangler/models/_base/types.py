from __future__ import annotations
from typing import Annotated, Any, List, TypeVar, Union
from pydantic import (
    BeforeValidator,
    Field,
)

PandasDataFrame = TypeVar("pandas.core.frame.DataFrame")


PandasSeries = TypeVar("pandas.core.series.Series")


ForcedStr = Annotated[Any, BeforeValidator(lambda x: str(x))]


AllOf = Annotated[
    List[str],
    Field(
        description=[
            "List fields where all are required for the data model to be valid."
        ]
    ),
]


OneOf = Annotated[
    List[Union[str, AllOf]],
    Field(
        description=[
            "List fields where at least one is required for the data model to be valid."
        ]
    ),
]


AnyOf = Annotated[
    List[Union[str, AllOf]],
    Field(
        description=[
            "List fields, AllOf where any are required for the data model to be valid."
        ]
    ),
]

Latitude = Annotated[float, Field(ge=-90, le=90, description="Latitude of stop.")]

Longitude = Annotated[float, Field(ge=-180, le=180, description="Longitude of stop.")]

PhoneNum = Annotated[
    str, Field("", description="Phone number for the specified location.")
]

TimeString = Annotated[
    str,
    Field(
        examples=["12:34", "12:34:56"],
        description="Time of day in 24-hour format. Seconds are optional.",
        pattern="^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$",
    ),
]
