from typing import Optional

import pandera as pa

from pandas import DataFrame
from pandera.typing import Series
from pandera import DataFrameModel

from ..logger import WranglerLogger


class FrequenciesSchema(DataFrameModel):
    headway_secs: Series[int] = pa.Field(
        coerce=True,
        ge=1,
        nullable=False,
    )


class StopsSchema(DataFrameModel):
    stop_id: Series[str] = pa.Field(nullable=False, unique=True)
    model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)
    stop_lat: Series[float] = pa.Field(coerce=True, nullable=False)
    stop_lon: Series[float] = pa.Field(coerce=True, nullable=False)
    wheelchair_boarding: Series[float] = pa.Field(coerce=True, nullable=True)


class ShapesSchema(DataFrameModel):
    shape_id: Series[str] = pa.Field(nullable=False)
    shape_model_node_id: Series[int] = pa.Field(coerce=True, nullable=False)
    shape_pt_lat: Series[float] = pa.Field(coerce=True, nullable=False)
    shape_pt_lon: Series[float] = pa.Field(coerce=True, nullable=False)
    shape_pt_sequence: Series[int] = pa.Field(coerce=True, nullable=False)


class TripsSchema(DataFrameModel):
    trip_id: Series[str] = pa.Field(nullable=False, unique=True)
    shape_id: Series[str] = pa.Field(nullable=False)


class StopTimesSchema(DataFrameModel):
    stop_id: Series[str] = pa.Field(nullable=False, coerce=True)
    stop_sequence: Series[int] = pa.Field(nullable=False, coerce=True)
    pickup_type: Series[int] = pa.Field(nullable=True, coerce=True)
    drop_off_type: Series[int] = pa.Field(nullable=True, coerce=True)
    # model_node_id: Optional[Series[int]] = pa.Field(nullable=True, coerce=True)


class RoutesSchema(DataFrameModel):
    route_id: Series[str] = pa.Field(nullable=False, unique=True)
