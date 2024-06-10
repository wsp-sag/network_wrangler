from __future__ import annotations

from typing import Annotated, List

from pydantic import Field

from ...models._base.types import ForcedStr


TimeString = Annotated[
    ForcedStr,
    Field(
        examples=["12:34", "12:34:56"],
        description="Time of day in 24-hour format. Seconds are optional.",
        pattern="^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$",
    ),
]

Timespan = Annotated[List[TimeString], Field(min_length=2, max_length=2)]
CategoryList = list[str]
