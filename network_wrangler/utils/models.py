"""Helper functions for data models."""

from typing import Any, Union, get_type_hints

import pandas as pd
import geopandas as gpd
import pandera as pa

from pandera import DataFrameModel
from pydantic import ValidationError, BaseModel
from pandera.errors import SchemaError

from .data import coerce_dict_to_df_types
from ..params import LAT_LON_CRS
from ..logger import WranglerLogger


def empty_df_from_datamodel(
    model: DataFrameModel, crs: int = LAT_LON_CRS
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """Create an empty DataFrame or GeoDataFrame with the specified columns.

    Args:
        schema (BaseModel): A pandera schema to create an empty [Geo]DataFrame from.
        crs: if schema has geometry, will use this as the geometry's crs. Defaults to LAT_LONG_CRS
    Returns:
        An empty [Geo]DataFrame that validates to the specified model.
    """
    schema = model.to_schema()
    data = {col: [] for col in schema.columns.keys()}

    if "geometry" in data:
        return model(gpd.GeoDataFrame(data, crs=crs))

    return model(pd.DataFrame(data))


def default_from_datamodel(data_model: pa.DataFrameModel, field: str):
    """Returns default value from pandera data model for a given field name."""
    if field in data_model.__fields__:
        return data_model.__fields__[field][1].default
    return None


def identify_model(
    data: Union[pd.DataFrame, dict], models: list[DataFrameModel, BaseModel]
) -> Union[DataFrameModel, BaseModel]:
    """Identify the model that the input data conforms to.

    Args:
        data (Union[pd.DataFrame, dict]): The input data to identify.
        models (list[DataFrameModel,BaseModel]): A list of models to validate the input data against.
    """
    for m in models:
        try:
            if isinstance(data, pd.DataFrame):
                model_instance = m.validate(data)
            else:
                model_instance = m(**data)
            return m
        except ValidationError as e:
            continue
        except SchemaError as e:
            continue

    WranglerLogger.error(
        f"The input data isn't consistant with any provided data model.\
                         \nInput data: {data}\
                         \nData Models: {models}"
    )
    raise ValueError(
        "The input dictionary does not conform to any of the provided models."
    )


class DatamodelDataframeIncompatableError(Exception):
    pass


def coerce_extra_fields_to_type_in_df(
    data: dict, model: BaseModel, df: pd.DataFrame
) -> dict:
    """Coerce extra fields in data that aren't specified in Pydantic model to the type in the df.

    Note: will not coerce lists of submodels, etc.

    Args:
        data (dict): The data to coerce.
        model (BaseModel): The Pydantic model to validate the data against.
        df (pd.DataFrame): The DataFrame to coerce the data to.
    """
    out_data = data.copy()
    types = get_type_hints(model)
    for key, value in data.items():
        # if key is in the model, then it will be coerced by the model - but we want to look for subclasses
        if key in model.model_fields:
            if isinstance(types[key], BaseModel):
                # Recursively coerce sub-models
                out_data[key] = coerce_extra_fields_to_type_in_df(
                    value, model.__annotations__[key], df
                )
            else:
                # don't need to coerce values in themodel
                continue
        else:
            if key not in df.columns:
                WranglerLogger.error(
                    f"Dataframe missing requested data property: {key}"
                )
                raise DatamodelDataframeIncompatableError(
                    f"Dataframe missing requested data property: {key}"
                )
            updated_d = coerce_dict_to_df_types(
                {key: value}, df, skip_keys=model.model_fields.keys()
            )
            out_data.update(updated_d)
    return out_data
