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
    root: conlist(Union[Latitude, Longitude], min_length=2, max_length=2)

    @field_validator("root")
    @classmethod
    def check_lat_long(cls, v):
        latitude, longitude = v
        return [Latitude(latitude), Longitude(longitude)]
