"""
This module contains pydantic data models for GTFS records.

1. Validates input or output of function matches the data model.

```python

import pydantic
from networks.gtfs import StopTimeRecord

@pydantic.validate_call
def process_table(stoptime: StopTimeRecord):

    # Perform operations on the table
    # The table will be automatically validated against its data model
    pass
```

2. Coerces types and adds default values.

```python
stop_data = {
    'stop_id': 123,
    'stop_lat': ...,
    'stop_long': ...,
}

stop = StopRecord(**stop_data)
print(stop)
# Transforms stop_id to string
# > StopRecord stop_id="123" stop_lat = ... stop_long = ...
```

"""

from typing import Optional

from pydantic import BaseModel
from pydantic.networks import HttpUrl

from .._base.geo import Longitude, Latitude

from .types import (
    AgencyID,
    AgencyName,
    AgencyPhone,
    AgencyFareUrl,
    AgencyEmail,
    StopID,
    StopCode,
    StopName,
    TTSStopName,
    StopDesc,
    ZoneID,
    StopUrl,
    LocationType,
    ParentStation,
    WheelchairAccessible,
    RouteID,
    RouteType,
    RouteShortName,
    RouteLongName,
    RouteDesc,
    RouteUrl,
    RouteColor,
    RouteTextColor,
    ShapeID,
    Language,
    ShapePtSequence,
    ShapeDistTraveled,
    TripID,
    Timezone,
    StartTime,
    EndTime,
    HeadwaySecs,
    ArrivalTime,
    DepartureTime,
    StopSequence,
    StopHeadsign,
    PickupType,
    DropoffType,
    Timepoint,
    ServiceID,
    TripHeadsign,
    TripShortName,
    DirectionID,
    BlockID,
    BikesAllowed,
)


class AgencyRecord(BaseModel):
    """
    Represents a transit agency.
    """

    agency_id: AgencyID
    agency_name: Optional[AgencyName]
    agency_url: Optional[HttpUrl]
    agency_timezone: Timezone
    agency_lang: Optional[Language]
    agency_phone: Optional[AgencyPhone]
    agency_fare_url: Optional[AgencyFareUrl]
    agency_email: Optional[AgencyEmail]


class FrequencyRecord(BaseModel):
    trip_id: TripID
    start_time: StartTime
    end_time: EndTime
    headway_secs: HeadwaySecs


class StopRecord(BaseModel):
    """
    Represents a stop or station where vehicles pick up or drop off passengers.
    """

    stop_id: StopID
    stop_lat: Latitude
    stop_lon: Longitude

    # wrangler specific
    trip_id: TripID

    # Optional
    stop_code: Optional[StopCode]
    stop_name: Optional[StopName]
    tts_stop_name: Optional[TTSStopName]
    stop_desc: Optional[StopDesc]
    zone_id: Optional[ZoneID]
    stop_url: Optional[StopUrl]
    location_type: Optional[LocationType]
    parent_station: Optional[ParentStation]
    stop_timezone: Optional[Timezone]
    wheelchair_boarding: Optional[WheelchairAccessible]


class RouteRecord(BaseModel):
    """
    Represents a transit route.
    """

    route_id: RouteID
    agency_id: AgencyID
    route_type: RouteType
    route_short_name: RouteShortName
    route_long_name: RouteLongName

    # Optional
    route_desc: Optional[RouteDesc]
    route_url: Optional[RouteUrl]
    route_color: Optional[RouteColor]
    route_text_color: Optional[RouteTextColor]


class ShapeRecord(BaseModel):
    """
    Represents a point on a path (shape) that a transit vehicle takes.
    """

    shape_id: ShapeID
    shape_pt_lat: Latitude
    shape_pt_lon: Longitude
    shape_pt_sequence: ShapePtSequence

    # Wrangler Specific
    shape_model_node_id: int

    # Optional
    shape_dist_traveled: Optional[ShapeDistTraveled]


class StopTimeRecord(BaseModel):
    """
    Times that a vehicle arrives at and departs from stops for each trip.
    """

    trip_id: TripID
    arrival_time: ArrivalTime
    departure_time: DepartureTime
    stop_id: StopID
    stop_sequence: StopSequence

    # Wrangler Specific
    model_node_id: int

    # Optional
    stop_headsign: Optional[StopHeadsign]
    pickup_type: Optional[PickupType]
    drop_off_type: Optional[DropoffType]
    shape_dist_traveled: Optional[ShapeDistTraveled]
    timepoint: Optional[Timepoint]


class TripRecord(BaseModel):
    """
    Describes trips which are sequences of two or more stops that occur at specific time.
    """

    route_id: RouteID
    service_id: ServiceID
    trip_id: TripID
    trip_headsign: TripHeadsign
    trip_short_name: TripShortName
    direction_id: DirectionID
    block_id: BlockID
    shape_id: ShapeID
    wheelchair_accessible: WheelchairAccessible
    bikes_allowed: BikesAllowed


def validate_agency_record(record: AgencyRecord) -> AgencyRecord:
    # Validate and process the agency record
    # ...
    return record


def validate_stop_record(record: StopRecord) -> StopRecord:
    # Validate and process the stop record
    # ...
    return record


# Usage example
agency_data = {
    "agency_id": "1",
    "agency_name": "Transit Agency",
    "agency_url": "https://example.com",
    "agency_timezone": "America/New_York",
    "agency_lang": "en",
    "agency_phone": "123-456-7890",
    "agency_fare_url": "https://example.com/fares",
    "agency_email": "info@example.com",
}

agency_record = AgencyRecord(**agency_data)
validated_agency_record = validate_agency_record(agency_record)
print(validated_agency_record)