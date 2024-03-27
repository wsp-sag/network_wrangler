from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    PositiveInt,
    conlist,
)

from .._base.geo import LatLongCoordinates

def LocationReference(BaseModel):
    sequence: PositiveInt
    point: LatLongCoordinates
    bearing: float = Field(None, ge=-360, le=360)
    distanceToNextRef: NonNegativeFloat
    intersectionId: str


LocationReferences = conlist(LocationReference, min_length=2)
