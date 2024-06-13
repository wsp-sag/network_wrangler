"""Field types for GTFS data."""

from enum import IntEnum
from typing import Annotated
from pydantic import Field, HttpUrl


from .._base.types import TimeString


class BikesAllowed(IntEnum):
    """Indicates whether bicycles are allowed."""

    NO_INFORMATION = 0
    ALLOWED = 1
    NOT_ALLOWED = 2


class DirectionID(IntEnum):
    """Indicates the direction of travel for a trip."""

    OUTBOUND = 0
    INBOUND = 1


class LocationType(IntEnum):
    """Indicates the type of node the stop record represents.

    Full documentation: https://gtfs.org/schedule/reference/#stopstxt
    """

    STOP_PLATFORM = 0
    STATION = 1
    ENTRANCE_EXIT = 2
    GENERIC_NODE = 3
    BOARDING_AREA = 4


class PickupDropoffType(IntEnum):
    """Indicates the pickup method for passengers at a stop.

    Full documentation: https://gtfs.org/schedule/reference
    """

    REGULAR = 0
    NONE = 1
    PHONE_AGENCY = 2
    COORDINATE_WITH_DRIVER = 3


class RouteType(IntEnum):
    """Indicates the type of transportation used on a route.

    Full documentation: https://gtfs.org/schedule/reference
    """

    TRAM = 0
    SUBWAY = 1
    RAIL = 2
    BUS = 3
    FERRY = 4
    CABLE_TRAM = 5
    AERIAL_LIFT = 6
    FUNICULAR = 7
    TROLLEYBUS = 11
    MONORAIL = 12


class TimepointType(IntEnum):
    """Indicates whether the specified time is exact or approximate.

    Full documentation: https://gtfs.org/schedule/reference
    """

    APPROXIMATE = 0
    EXACT = 1


class WheelchairAccessible(IntEnum):
    """Indicates whether the trip is wheelchair accessible.

    Full documentation: https://gtfs.org/schedule/reference
    """

    NO_INFORMATION = 0
    POSSIBLE = 1
    NOT_POSSIBLE = 2


RouteID = Annotated[str, Field(None, description="Uniquely identifies a route.")]
ServiceID = Annotated[
    str,
    Field(
        None,
        description="Uniquely identifies a set of dates when service is available for \
            one or more routes.",
    ),
]
TripID = Annotated[str, Field(None, description="Uniquely identifies a trip.")]
StopID = Annotated[str, Field(None, description="Uniquely identifies a stop.")]
AgencyID = Annotated[str, Field(None, description="Agency or brand for the specified route.")]
ShapeID = Annotated[str, Field(None, description="Uniquely identifies a shape.")]
ZoneID = Annotated[str, Field("", description="The fare zone for a stop ID.")]

AgencyName = Annotated[str, Field("", description="Name of the transit agency.")]

AgencyPhone = Annotated[
    str,
    Field(None, description="A single voice telephone number for the specified agency."),
]
AgencyFareUrl = Annotated[
    HttpUrl,
    Field(
        None,
        description="URL of a web page that allows a rider to purchase tickets or other fare \
            instruments for that agency online.",
    ),
]
AgencyEmail = Annotated[
    str,
    Field(
        None,
        description="Single valid email address actively monitored by the agency’s \
            customer service department.",
    ),
]

ArrivalTime = Annotated[
    TimeString,
    Field(
        "",
        description="Arrival time at a specific stop for a specific trip on a route.",
    ),
]
DepartureTime = Annotated[
    TimeString,
    Field(
        "",
        description="Departure time from a specific stop for a specific trip on a route.",
    ),
]
DropoffType = Annotated[PickupDropoffType, Field(0, description="Indicates dropoff method.")]

BlockID = Annotated[
    str,
    Field(None, description="Uniquely identifies the block to which the trip belongs."),
]
HeadwaySecs = Annotated[
    int,
    Field(
        None,
        ge=1,
        description="Time, in seconds, between departures from the same stop (headway) \
            for the trip, during the time interval specified by start_time and end_time. \
                Multiple headways may be defined for the same trip, but must not overlap. \
                    New headways may start at the exact time the previous headway ends.",
    ),
]
Language = Annotated[str, Field("", description="Language spoken at agency.")]
ParentStation = Annotated[
    StopID,
    Field(
        None,
        description="For stops that are physically located inside stations, the parent_station \
            field identifies the station associated with the stop using the Stop_ID.",
    ),
]
PickupType = Annotated[PickupDropoffType, Field(0, description="Indicates pickup method.")]

RouteColor = Annotated[str, Field("", description="Color that corresponds to a route.")]
RouteDesc = Annotated[str, Field("", description="Description of a route.")]
RouteLongName = Annotated[str, Field("", description="Full name of a route.")]
RouteShortName = Annotated[str, Field("", description="Short name of a route.")]
RouteTextColor = Annotated[
    str,
    Field(
        "",
        description="Legible color to use for text drawn against a background of route_color.",
    ),
]
RouteUrl = Annotated[
    HttpUrl, Field("", description="T URL of a web page about a particular route.")
]

ShapeDistTraveled = Annotated[
    float,
    Field(
        None,
        ge=0,
        description="Actual distance traveled along the shape from the first shape point to \
            the point specified in this record.",
    ),
]
ShapePtSequence = Annotated[
    int, Field(..., ge=0, description="Order of the shape point in the shape.")
]

StartTime = Annotated[
    TimeString,
    Field(
        "",
        description="Time at which the first vehicle departs from the first stop of the trip \
            with the specified headway.",
    ),
]
EndTime = Annotated[
    TimeString,
    Field(
        "",
        description="Time at which service changes to a different headway (or ceases) at \
            the first stop in the trip.",
    ),
]

StopCode = Annotated[
    str,
    Field(
        "",
        description="Short text or a number that uniquely identifies the stop for passengers.",
    ),
]
StopDesc = Annotated[str, Field("", description="Description of a stop.")]
StopHeadsign = Annotated[
    str,
    Field(
        "",
        description="Text that appears on signage identifying the trip's \
            destination to passengers.",
    ),
]
StopName = Annotated[str, Field("", description="Name of a stop or station.")]
StopSequence = Annotated[
    int,
    Field(
        ...,
        ge=0,
        description="Order of stops for a particular trip. The values for stop_sequence must \
            be non-negative and increase along the trip.",
    ),
]
StopUrl = Annotated[HttpUrl, Field("", description="URL of a web page about a particular stop.")]


Timepoint = Annotated[
    TimepointType,
    Field(
        1,
        description="Exact or approximate time that a vehicle arrives at or departs from a stop.",
    ),
]
Timezone = Annotated[
    str,
    Field(
        "",
        description="The timezone from \
            https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.",
    ),
]
TTSStopName = Annotated[
    str,
    Field(
        "",
        description="Name of a stop or station in a text-to-speech compatible format.",
    ),
]

TripHeadsign = Annotated[
    str,
    Field(
        "",
        description="Text that appears on signage to id the trip's destination to passengers.",
    ),
]

TripShortName = Annotated[str, Field("", description="Publicly visible short name of a trip.")]
