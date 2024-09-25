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
    is_valid = validate_df_to_model(stops_df, StopsTable)
    ```

3. Checking uniqueness of values in a DataFrame:
    df = pd.DataFrame(...)  # DataFrame to check uniqueness
    is_unique = uniqueness(df, cols=["column1", "column2"])
"""

from typing import Optional

import pandera as pa
import pandas as pd

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
from .._base.types import TimeString
from .._base.db import TableForeignKeys, TablePrimaryKeys
from ...utils.time import str_to_time_series, str_to_time
from ...params import DEFAULT_TIMESPAN
from ...logger import WranglerLogger


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
        _pk: TablePrimaryKeys = ["agency_id"]


class StopsTable(pa.DataFrameModel):
    """Represents the Stops table in the GTFS dataset."""

    stop_id: Series[str] = pa.Field(coerce=True, nullable=False, unique=True)
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
        _pk: TablePrimaryKeys = ["stop_id"]
        _fk: TableForeignKeys = {"parent_station": ("stops", "stop_id")}


class WranglerStopsTable(StopsTable):
    """Wrangler flavor of GTFS StopsTable."""

    stop_id: Series[int] = pa.Field(
        coerce=True, nullable=False, unique=True, description="The model_node_id."
    )
    stop_id_GTFS: Series[str] = pa.Field(
        coerce=True,
        nullable=True,
        description="The stop_id from the GTFS data",
    )
    stop_lat: Series[float] = pa.Field(coerce=True, nullable=True, ge=-90, le=90)
    stop_lon: Series[float] = pa.Field(coerce=True, nullable=True, ge=-180, le=180)
    projects: Series[str] = pa.Field(coerce=True, default="")


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
        _pk: TablePrimaryKeys = ["route_id"]
        _fk: TableForeignKeys = {"agency_id": ("agencies", "agency_id")}


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
        _pk: TablePrimaryKeys = ["shape_id", "shape_pt_sequence"]
        _fk: TableForeignKeys = {}
        unique = ["shape_id", "shape_pt_sequence"]


class WranglerShapesTable(ShapesTable):
    """Wrangler flavor of GTFS ShapesTable."""

    shape_model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)
    projects: Series[str] = pa.Field(coerce=True, default="")


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
    service_id: Series[str] = pa.Field(nullable=False, coerce=True, default="1")
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
        _pk: TablePrimaryKeys = ["trip_id"]
        _fk: TableForeignKeys = {"route_id": ("routes", "route_id")}


class WranglerTripsTable(TripsTable):
    """Represents the Trips table in the Wrangler feed, adding projects list."""

    projects: Series[str] = pa.Field(coerce=True, default="")

    class Config:
        """Config for the WranglerTripsTable data model."""

        coerce = True
        add_missing_columns = True
        _pk: TablePrimaryKeys = ["trip_id"]
        _fk: TableForeignKeys = {"route_id": ("routes", "route_id")}


class FrequenciesTable(pa.DataFrameModel):
    """Represents the Frequency table in the GTFS dataset.

    Configurations:
    - dtype: PydanticModel(FrequencyRecord)
    - uniqueness: "trip_id","start_time"]
    """

    trip_id: Series[str] = pa.Field(nullable=False, coerce=True)
    start_time: Series[TimeString] = pa.Field(
        nullable=False, coerce=True, default=DEFAULT_TIMESPAN[0]
    )
    end_time: Series[TimeString] = pa.Field(
        nullable=False, coerce=True, default=DEFAULT_TIMESPAN[1]
    )
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
        _pk: TablePrimaryKeys = ["trip_id", "start_time"]
        _fk: TableForeignKeys = {"trip_id": ("trips", "trip_id")}


class WranglerFrequenciesTable(FrequenciesTable):
    """Wrangler flavor of GTFS FrequenciesTable."""

    projects: Series[str] = pa.Field(coerce=True, default="")
    start_time: Series = pa.Field(
        nullable=False, coerce=True, default=str_to_time(DEFAULT_TIMESPAN[0])
    )
    end_time: Series = pa.Field(
        nullable=False, coerce=True, default=str_to_time(DEFAULT_TIMESPAN[1])
    )

    class Config:
        """Config for the FrequenciesTable data model."""

        coerce = True
        add_missing_columns = True
        unique = ["trip_id", "start_time"]
        _pk: TablePrimaryKeys = ["trip_id", "start_time"]
        _fk: TableForeignKeys = {"trip_id": ("trips", "trip_id")}

    @pa.parser("start_time")
    def st_to_timestamp(cls, series: Series) -> Series[Timestamp]:
        """Check that start time is timestamp."""
        series = series.fillna(str_to_time(DEFAULT_TIMESPAN[0]))
        if series.dtype == "datetime64[ns]":
            return series
        series = str_to_time_series(series)
        return series.astype("datetime64[ns]")

    @pa.parser("end_time")
    def et_to_timestamp(cls, series: Series) -> Series[Timestamp]:
        """Check that start time is timestamp."""
        series = series.fillna(str_to_time(DEFAULT_TIMESPAN[1]))
        if series.dtype == "datetime64[ns]":
            return series
        return str_to_time_series(series)


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
    arrival_time: Series[TimeString] = pa.Field(nullable=True, coerce=True)
    departure_time: Series[TimeString] = pa.Field(nullable=True, coerce=True)

    # Optional
    shape_dist_traveled: Optional[Series[float]] = pa.Field(coerce=True, nullable=True, ge=0)
    timepoint: Optional[Series[Category]] = pa.Field(
        dtype_kwargs={"categories": TimepointType}, coerce=True, default=0
    )

    class Config:
        """Config for the StopTimesTable data model."""

        coerce = True
        add_missing_columns = True
        _pk: TablePrimaryKeys = ["trip_id", "stop_sequence"]
        _fk: TableForeignKeys = {
            "trip_id": ("trips", "trip_id"),
            "stop_id": ("stops", "stop_id"),
        }
        unique = ["trip_id", "stop_sequence"]


class WranglerStopTimesTable(StopTimesTable):
    """Wrangler flavor of GTFS StopTimesTable."""

    stop_id: Series[int] = pa.Field(nullable=False, coerce=True, description="The model_node_id.")
    arrival_time: Series[Timestamp] = pa.Field(nullable=True, default=pd.NaT, coerce=False)
    departure_time: Series[Timestamp] = pa.Field(nullable=True, default=pd.NaT, coerce=False)
    projects: Series[str] = pa.Field(coerce=True, default="")

    @pa.dataframe_parser
    def parse_times(cls, df):
        """Parse arrival and departure times.

        - Check that all times are timestamps <24h.
        - Check that arrival_time and departure_time are not both "00:00:00".  If so, set
            them to NaT.

        """
        # if arrival_time and departure_time are not set or are both set to "00:00:00", set them to NaT
        if "arrival_time" not in df.columns:
            df["arrival_time"] = pd.NaT
        if "departure_time" not in df.columns:
            df["departure_time"] = pd.NaT
        msg = f"stop_times before parsing: \n {df[['arrival_time', 'departure_time']]}"
        # WranglerLogger.debug(msg)
        filler_timestrings = (df["arrival_time"] == Timestamp("00:00:00")) & (
            df["departure_time"] == Timestamp("00:00:00")
        )

        df.loc[filler_timestrings, "arrival_time"] = pd.NaT
        df.loc[filler_timestrings, "departure_time"] = pd.NaT
        msg = f"stop_times after filling with NaT: \n {df[['arrival_time', 'departure_time']]}"
        # WranglerLogger.debug(msg)
        df["arrival_time"] = str_to_time_series(df["arrival_time"])
        df["departure_time"] = str_to_time_series(df["departure_time"])
        msg = f"stop_times after parsing: \n{df[['arrival_time', 'departure_time']]}"
        # WranglerLogger.debug(msg)
        return df

    class Config:
        """Config for the StopTimesTable data model."""

        coerce = True
        add_missing_columns = True
        _pk: TablePrimaryKeys = ["trip_id", "stop_sequence"]
        _fk: TableForeignKeys = {
            "trip_id": ("trips", "trip_id"),
            "stop_id": ("stops", "stop_id"),
        }
        unique = ["trip_id", "stop_sequence"]
