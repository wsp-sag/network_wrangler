"""This module defines the data models for various GTFS tables using pandera library.

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

from typing import Optional

import pandera as pa

from pandas import Timestamp
from pandera.typing import Series, Category

from .types import (
    LocationType,
    WheelchairAccessible,
    RouteType,
    DirectionID,
    PickupDropoffType,
    BikesAllowed,
    TimepointType,
)
from .table_types import HttpURL


class AgenciesTable(pa.DataFrameModel):
    """Represents the Agency table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(AgencyRecord)
    - uniqueness: ["agency_id"]
    """

    agency_id: Series[str] = pa.Field(coerce=True, nullable=False, unique=True)
    agency_name: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_url: Series[HttpURL] = pa.Field(coerce=True, nullable=True)
    agency_timezone: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_lang: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_phone: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_fare_url: Series[str] = pa.Field(coerce=True, nullable=True)
    agency_email: Series[str] = pa.Field(coerce=True, nullable=True)

    class Config:
        """Config for the AgenciesTable data model."""

        coerce = True
        add_missing_columns = True
        _pk = ["agency_id"]


class StopsTable(pa.DataFrameModel):
    """Represents the Stops table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(StopRecord)
    - uniqueness: ["stop_id"]
    """

    stop_id: Series[str] = pa.Field(coerce=True, nullable=False, unique=True)
    # TODO why is that here?
    model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)
    stop_lat: Series[float] = pa.Field(coerce=True, nullable=False, ge=-90, le=90)
    stop_lon: Series[float] = pa.Field(coerce=True, nullable=False, ge=-180, le=180)

    # Optional Fields
    wheelchair_boarding: Optional[Series[Category]] = pa.Field(
        dtype_kwargs={"categories": WheelchairAccessible}, coerce=True, default=0
    )
    stop_code: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    stop_name: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    tts_stop_name: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    stop_desc: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    zone_id: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    stop_url: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    location_type: Optional[Series[Category]] = pa.Field(
        dtype_kwargs={"categories": LocationType},
        nullable=True,
        coerce=True,
        default=0,
    )
    parent_station: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    stop_timezone: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)

    class Config:
        """Config for the StopsTable data model."""

        coerce = True
        add_missing_columns = True
        _pk = ["stop_id"]
        _fk = {"parent_station": ["stops", "stop_id"]}


class WranglerStopsTable(StopsTable):
    """Wrangler flavor of GTFS StopsTable."""

    model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)
    # TODO should this be in base
    stop_lat: Series[float] = pa.Field(coerce=True, nullable=True, ge=-90, le=90)
    stop_lon: Series[float] = pa.Field(coerce=True, nullable=True, ge=-180, le=180)


class RoutesTable(pa.DataFrameModel):
    """Represents the Routes table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(RouteRecord)
    - uniqueness: ["route_id"]
    """

    route_id: Series[str] = pa.Field(nullable=False, unique=True, coerce=True)
    route_short_name: Series[str] = pa.Field(nullable=True, coerce=True)
    route_long_name: Series[str] = pa.Field(nullable=True, coerce=True)
    route_type: Series[Category] = pa.Field(
        dtype_kwargs={"categories": RouteType}, coerce=True, nullable=False
    )

    # Optional Fields
    agency_id: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    route_desc: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    route_url: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    route_color: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    route_text_color: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)

    class Config:
        """Config for the RoutesTable data model."""

        coerce = True
        add_missing_columns = True
        _pk = ["route_id"]
        _fk = {"agency_id": ["agencies", "agency_id"]}


class ShapesTable(pa.DataFrameModel):
    """Represents the Shapes table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(ShapeRecord)
    - uniqueness: ["shape_id", "shape_pt_sequence"]
    """

    shape_id: Series[str] = pa.Field(nullable=False, coerce=True)
    shape_pt_lat: Series[float] = pa.Field(coerce=True, nullable=False, ge=-90, le=90)
    shape_pt_lon: Series[float] = pa.Field(coerce=True, nullable=False, ge=-180, le=180)
    shape_pt_sequence: Series[int] = pa.Field(coerce=True, nullable=False, ge=0)

    # Optional
    shape_dist_traveled: Optional[Series[float]] = pa.Field(coerce=True, nullable=True, ge=0)

    class Config:
        """Config for the ShapesTable data model."""

        coerce = True
        add_missing_columns = True
        _pk = ["shape_id", "shape_pt_sequence"]
        _fk = {}
        unique = ["shape_id", "shape_pt_sequence"]


class WranglerShapesTable(ShapesTable):
    """Wrangler flavor of GTFS ShapesTable."""

    shape_model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)


class TripsTable(pa.DataFrameModel):
    """Represents the Trips table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(TripRecord)
    """

    trip_id: Series[str] = pa.Field(nullable=False, unique=True, coerce=True)
    shape_id: Series[str] = pa.Field(nullable=False, coerce=True)
    direction_id: Series[Category] = pa.Field(
        dtype_kwargs={"categories": DirectionID}, coerce=True, nullable=False, default=0
    )
    service_id: Series[str] = pa.Field(nullable=False, coerce=True)
    route_id: Series[str] = pa.Field(nullable=False, coerce=True)

    # Optional Fields
    trip_short_name: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    trip_headsign: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    block_id: Optional[Series[str]] = pa.Field(nullable=True, coerce=True)
    wheelchair_accessible: Optional[Series[Category]] = pa.Field(
        dtype_kwargs={"categories": WheelchairAccessible}, coerce=True, default=0
    )
    bikes_allowed: Optional[Series[Category]] = pa.Field(
        dtype_kwargs={"categories": BikesAllowed},
        coerce=True,
        default=0,
    )

    class Config:
        """Config for the TripsTable data model."""

        coerce = True
        add_missing_columns = True
        _pk = ["trip_id"]
        _fk = {"route_id": ["routes", "route_id"]}


class FrequenciesTable(pa.DataFrameModel):
    """Represents the Frequency table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(FrequencyRecord)
    - uniqueness: "trip_id","start_time"]
    """

    trip_id: Series[str] = pa.Field(nullable=False, coerce=True)
    start_time: Series[Timestamp] = pa.Field(nullable=False, coerce=True)
    end_time: Series[Timestamp] = pa.Field(nullable=False, coerce=True)
    headway_secs: Series[int] = pa.Field(
        coerce=True,
        ge=1,
        nullable=False,
    )

    class Config:
        """Config for the FrequenciesTable data model."""

        coerce = True
        add_missing_columns = True
        unique = ["trip_id", "start_time"]
        _pk = ["trip_id", "start_time"]
        _fk = {"trip_id": ["trips", "trip_id"]}


class StopTimesTable(pa.DataFrameModel):
    """Represents the Stop Times table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(StopTimeRecord)
    - uniqueness: ["trip_id", "stop_sequence"]
    """

    trip_id: Series[str] = pa.Field(nullable=False, coerce=True)
    stop_id: Series[str] = pa.Field(nullable=False, coerce=True)
    stop_sequence: Series[int] = pa.Field(nullable=False, coerce=True, ge=0)
    pickup_type: Series[Category] = pa.Field(
        dtype_kwargs={"categories": PickupDropoffType},
        nullable=True,
        coerce=True,
    )
    drop_off_type: Series[Category] = pa.Field(
        dtype_kwargs={"categories": PickupDropoffType},
        nullable=True,
        coerce=True,
    )
    arrival_time: Series[Timestamp] = pa.Field(nullable=False, coerce=True)
    departure_time: Series[Timestamp] = pa.Field(nullable=False, coerce=True)

    # Optional
    shape_dist_traveled: Optional[Series[float]] = pa.Field(coerce=True, nullable=True, ge=0)
    timepoint: Optional[Series[Category]] = pa.Field(
        dtype_kwargs={"categories": TimepointType}, coerce=True, default=0
    )

    class Config:
        """Config for the StopTimesTable data model."""

        coerce = True
        add_missing_columns = True
        _pk = ["trip_id", "stop_sequence"]
        _fk = {
            "trip_id": ["trips", "trip_id"],
            "stop_id": ["stops", "stop_id"],
        }
        unique = ["trip_id", "stop_sequence"]


class WranglerStopTimesTable(StopTimesTable):
    """Wrangler flavor of GTFS StopTimesTable."""

    model_node_id: Series[int] = pa.Field(coerce=True)
    arrival_time: Optional[Series[Timestamp]] = pa.Field(
        coerce=True, nullable=True, default=Timestamp("00:00:00")
    )
    departure_time: Optional[Series[Timestamp]] = pa.Field(
        coerce=True, nullable=True, default=Timestamp("00:00:00")
    )
