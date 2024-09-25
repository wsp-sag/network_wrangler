from typing import Union

from pydantic import (
    RootModel,
    conlist,
    confloat,
    field_validator,
)

Latitude = confloat(ge=-90, le=90)

Longitude = confloat(ge=-180, le=180)


class LatLongCoordinates(RootModel):
    root: list[float]

    @field_validator("root")
    @classmethod
    def check_lat_long(cls, v):
        assert len(v) == 2, f"Expected two values for latitude and longitude, got {len(v)}"
        latitude, longitude = v
        return [Latitude(latitude), Longitude(longitude)]
