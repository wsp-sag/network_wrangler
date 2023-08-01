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


class TripsSchema(DataFrameModel):
    trip_id: Series[str] = pa.Field(nullable=False, unique=True)


class RoutesSchema(DataFrameModel):
    route_id: Series[str] = pa.Field(nullable=False, unique=True)


def validate_df(df: DataFrame, schema: DataFrameModel):
    try:
        FrequenciesSchema.validate(df, lazy=True)

    except pa.errors.SchemaErrors as err:
        WranglerLogger.error(
            f"Schema errors and failure cases:{err.failure_cases}\n\
                             DataFrame object that failed validation:{err.data}"
        )
