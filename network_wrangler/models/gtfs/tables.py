"""
This module defines the data models for various GTFS tables using pandera library.

The module includes the following classes:
- AgencyTable: Represents the Agency table in the GTFS dataset.
- StopsTable: Represents the Stops table in the GTFS dataset.
- RoutesTable: Represents the Routes table in the GTFS dataset.
- ShapesTable: Represents the Shapes table in the GTFS dataset.
- StopTimesTable: Represents the Stop Times table in the GTFS dataset.
- TripsTable: Represents the Trips table in the GTFS dataset.

Each table model leverages the Pydantic data models defined in the records module to define the
data model for the corresponding table. The classes also include additional configurations for, 
such as uniqueness constraints.

There is NOT any foreign key validation in the data models. 

Additionally, the module includes a custom check method called "uniqueness" that can be used to 
check for uniqueness of values in a DataFrame.

For more examples of how to use Pandera DataModels, see the Pandera documentation at:
https://pandera.readthedocs.io/en/stable/dataframe-models.html

Usage examples:

1. Using a type decorator to automatically validate incoming table:

    ``` python
    import pandera as pa
    @pa.check_types
    def process_table(table: pa.DataFrameModel):
        # Perform operations on the table
        # The table will be automatically validated against its data model
        pass
    ```

2. Creating an instance of AgencyTable:

    ```python
    agency_table = AgencyTable(pd.from_csv("agency.csv")
    ```

2. Validating the StopsTable instance:

    ```python
    is_valid = stops_df.validate()
    ```

3. Checking uniqueness of values in a DataFrame:
    df = pd.DataFrame(...)  # DataFrame to check uniqueness
    is_unique = uniqueness(df, cols=["column1", "column2"])
"""

import pandera as pa
from pandera.typing import Series
from pydantic import HttpUrl
from typing import Optional

from .._base.geo import Latitude, Longitude
from .._base.time import TimeString
from .types import (
    LocationType, WheelchairAccessible, RouteType, DirectionID, PickupDropoffType, BikesAllowed
)


class AgenciesTable(pa.DataFrameModel):
    """
    Represents the Agency table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(AgencyRecord)
    - uniqueness: ["agency_id"]
    """
    agency_id: Series[str] = pa.Field(coerce=True, nullable=False, unique=True)
    agency_name: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_url: Series[HttpUrl] = pa.Field(coerce=True, nullable=True)
    agency_timezone: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_lang: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_phone: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_fare_url: Series[HttpUrl] = pa.Field(coerce=True, nullable=True)
    agency_email: Series[str] = pa.Field(coerce=True, nullable=True)

    class Config:
        coerce = True
        _pk = ["agency_id"]
        _fk = {}


class StopsTable(pa.DataFrameModel):
    """
    Represents the Stops table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(StopRecord)
    - uniqueness: ["stop_id"]
    """
    stop_id: Series[str] = pa.Field(coerce=True, nullable=False, unique=True)
    model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)
    stop_lat: Series[Latitude] = pa.Field(coerce=True, nullable=False)
    stop_lon: Series[Longitude] = pa.Field(coerce=True, nullable=False)

    # Optional Fields
    wheelchair_boarding: Series[WheelchairAccessible] = pa.Field(
        coerce=True, nullable=True, default=0
    )
    stop_code: Series[str] = pa.Field(nullable=True, coerce=True)
    stop_name: Series[str] = pa.Field(nullable=True, coerce=True)
    tts_stop_name: Series[str] = pa.Field(nullable=True, coerce=True)
    stop_desc: Series[str] = pa.Field(nullable=True, coerce=True)
    zone_id: Series[str] = pa.Field(nullable=True, coerce=True)
    stop_url: Series[HttpUrl] = pa.Field(nullable=True, coerce=True)
    location_type: Series[LocationType] = pa.Field(nullable=True, coerce=True, default = 0)
    parent_station: Series[str] = pa.Field(nullable=True, coerce=True)
    stop_timezone: Series[str] = pa.Field(nullable=True, coerce=True)

    class Config:
        coerce = True
        _pk = ["stop_id"]
        _fk = {"parent_station": ["stops", "stop_id"]}


class RoutesTable(pa.DataFrameModel):
    """
    Represents the Routes table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(RouteRecord)
    - uniqueness: ["route_id"]
    """
    route_id: Series[str] = pa.Field(nullable=False, unique=True, coerce=True)
    route_short_name: Series[str] = pa.Field(nullable=True, coerce=True)
    route_long_name: Series[str] = pa.Field(nullable=True, coerce=True)
    route_type: Series[RouteType] = pa.Field(coerce=True)

    # Optional Fields
    agency_id: Series[str] = pa.Field(nullable=True, coerce=True)
    route_desc: Series[str] = pa.Field(nullable=True, coerce=True)
    route_url: Series[str] = pa.Field(nullable=True, coerce=True)
    route_color: Series[str] = pa.Field(nullable=True, coerce=True)
    route_text_color: Series[str] = pa.Field(nullable=True, coerce=True)

    class Config:
        coerce = True
        _pk = ["route_id"]
        _fk = {"agency_id": ["agencies", "agency_id"]}


class ShapesTable(pa.DataFrameModel):
    """
    Represents the Shapes table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(ShapeRecord)
    - uniqueness: ["shape_id", "shape_pt_sequence"]
    """
    shape_id: Series[str] = pa.Field(nullable=False, coerce=True)
    shape_pt_lat: Series[Latitude] = pa.Field(coerce=True, nullable=False)
    shape_pt_lon: Series[Longitude] = pa.Field(coerce=True, nullable=False)
    shape_pt_sequence: Series[int] = pa.Field(coerce=True, nullable=False, ge=0)

    # Wrangler Specific
    shape_model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)

    # Optional
    shape_dist_traveled: Series[float] = pa.Field(coerce=True, nullable=True, ge=0)

    class Config:
        coerce = True
        _pk = ["shape_id", "shape_pt_sequence"]
        _fk = {"shape_id": ["routes", "field"]}
        uniqueness = _pk


class TripsTable(pa.DataFrameModel):
    """
    Represents the Trips table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(TripRecord)
    """
    trip_id: Series[str] = pa.Field(nullable=False, unique=True, coerce=True)
    shape_id: Series[str] = pa.Field(nullable=False, coerce=True)
    direction_id: Series[DirectionID] = pa.Field(coerce=True, nullable=False)
    service_id: Series[str] = pa.Field(nullable=False, coerce=True)
    route_id: Series[str] = pa.Field(nullable=False, coerce=True)

    # Optional Fields
    trip_short_name: Series[str] = pa.Field(nullable=True, coerce=True)
    trip_headsign: Series[str] = pa.Field(nullable=True, coerce=True)
    block_id: Series[str] = pa.Field(nullable=True, coerce=True)
    wheelchair_accessible: Series[WheelchairAccessible] = pa.Field(coerce=True, nullable=True, default=0)
    bikes_allowed: Series[BikesAllowed] = pa.Field(coerce=True, nullable=True, default=0)

    class Config:
        coerce = True
        _pk = ["trip_id"]
        _fk = {"route_id": ["routes", "field"]}


class FrequenciesTable(pa.DataFrameModel):
    """
    Represents the Agency table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(FrequencyRecord)
    - uniqueness: "trip_id","start_time"]
    """
    trip_id: Series[str] = pa.Field(nullable=False, coerce=True)
    start_time: Series[TimeString] = pa.Field(nullable=False, coerce=True)
    end_time: Series[TimeString] = pa.Field(nullable=False, coerce=True)
    headway_secs: Series[int] = pa.Field(
        coerce=True,
        ge=1,
        nullable=False,
    )

    class Config:
        coerce = True
        _pk = ["trip_id", "start_time"]
        _fk = {"trip_id": ["routes", "trip_id"]}
        uniqueness = _pk


class StopTimesTable(pa.DataFrameModel):
    """
    Represents the Stop Times table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(StopTimeRecord)
    - uniqueness: ["trip_id", "stop_sequence"]
    """
    trip_id: Series[str] = pa.Field(nullable=False, coerce=True)
    stop_id: Series[str] = pa.Field(nullable=False, coerce=True)
    stop_sequence: Series[int] = pa.Field(nullable=False, coerce=True, ge=0)
    pickup_type: Series[PickupDropoffType] = pa.Field(nullable=True, coerce=True)
    drop_off_type: Series[PickupDropoffType] = pa.Field(nullable=True, coerce=True)
    arrival_time: Series[TimeString] = pa.Field(nullable=False, coerce=True)
    departure_time: Series[TimeString] = pa.Field(nullable=False, coerce=True)
    shape_dist_traveled: Series[float] = pa.Field(coerce=True, nullable=True, ge=0)
    timepoint: Series[int] = pa.Field(coerce=True, nullable=True)

    # wrangler specific
    model_node_id: Optional[Series[int]] = pa.Field(nullable=True, coerce=True)

    class Config:
        coerce = True
        _pk = ["trip_id", "stop_sequence"]
        uniqueness = _pk
        _fk = {
            "trip_id": ["trips", "trip_id"],
            "stop_id": ["stops", "stop_id"],
        }
