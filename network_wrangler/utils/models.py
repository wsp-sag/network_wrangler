"""Helper functions for data models."""

import copy
from typing import Union, get_type_hints, Optional
from pathlib import Path

import pandas as pd
import geopandas as gpd
from pandas import DataFrame
import pandera as pa

from pandera import DataFrameModel
from pydantic import ValidationError, BaseModel, validate_call
from pydantic._internal._model_construction import ModelMetaclass
from pandera.errors import SchemaErrors, SchemaError

from .data import coerce_val_to_df_types
from ..params import LAT_LON_CRS
from ..logger import WranglerLogger


def empty_df_from_datamodel(
    model: DataFrameModel, crs: int = LAT_LON_CRS
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """Create an empty DataFrame or GeoDataFrame with the specified columns.

    Args:
        model (BaseModel): A pandera data model to create empty [Geo]DataFrame from.
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
        if hasattr(data_model.__fields__[field][1], "default"):
            return data_model.__fields__[field][1].default
    return None


class TableValidationError(Exception):
    """Raised when a table validation fails."""

    pass


@validate_call(config=dict(arbitrary_types_allowed=True))
def validate_df_to_model(
    df: DataFrame, model: DataFrameModel, output_file: Path = "validation_failure_cases.csv"
) -> DataFrame:
    """Wrapper to validate a DataFrame against a Pandera DataFrameModel with better logging.

    Args:
        df: DataFrame to validate.
        model: Pandera DataFrameModel to validate against.
        output_file: Optional file to write validation errors to. Defaults to
            validation_failure_cases.csv.
    """
    try:
        model_df = model.validate(df, lazy=True)
        for c in model_df.columns:
            default_value = default_from_datamodel(model, c)
            if default_value is None:
                model_df[c] = model_df[c].where(pd.notna(model_df[c]), None)
            else:
                model_df[c] = model_df[c].fillna(default_value)

        return model_df

    except SchemaErrors or TypeError or ValueError as e:
        # Log the summary of errors
        WranglerLogger.error(
            f"Validation to {model.__name__} failed with {len(e.failure_cases)} \
            errors: \n{e.failure_cases}"
        )

        # If there are many errors, save them to a file
        if len(e.failure_cases) > 5:
            error_file = output_file
            e.failure_cases.to_csv(error_file)
            WranglerLogger.info(f"Detailed error cases written to {error_file}")
        else:
            # Otherwise log the errors directly
            WranglerLogger.error("Detailed failure cases:\n%s", e.failure_cases)
        raise TableValidationError(f"Validation to {model.__name__} failed.")
    except SchemaError as e:
        WranglerLogger.error(f"Validation to {model.__name__} failed with error: {e}")
        WranglerLogger.error(f"Failure Cases:\n{e.failure_cases}")
        raise TableValidationError(f"Validation to {model.__name__} failed.")


def identify_model(
    data: Union[pd.DataFrame, dict], models: list[DataFrameModel, BaseModel]
) -> Union[DataFrameModel, BaseModel]:
    """Identify the model that the input data conforms to.

    Args:
        data (Union[pd.DataFrame, dict]): The input data to identify.
        models (list[DataFrameModel,BaseModel]): A list of models to validate the input
          data against.
    """
    for m in models:
        try:
            if isinstance(data, pd.DataFrame):
                validate_df_to_model(data, m)
            else:
                m(**data)
            return m
        except ValidationError:
            continue
        except SchemaError:
            continue

    WranglerLogger.error(
        f"The input data isn't consistant with any provided data model.\
                         \nInput data: {data}\
                         \nData Models: {models}"
    )
    raise ValueError("The input dictionary does not conform to any of the provided models.")


class DatamodelDataframeIncompatableError(Exception):
    """Raised when a data model and a dataframe are not compatable."""

    pass


def extra_attributes_undefined_in_model(instance: BaseModel, model: BaseModel) -> list:
    """Find the extra attributes in a pydantic model that are not defined in the model."""
    defined_fields = model.model_fields
    all_attributes = list(instance.model_dump(exclude_none=True, by_alias=True).keys())
    extra_attributes = [a for a in all_attributes if a not in defined_fields]
    return extra_attributes


def submodel_fields_in_model(model: BaseModel, instance: Optional[BaseModel] = None) -> list:
    """Find the fields in a pydantic model that are submodels."""
    types = get_type_hints(model)
    model_type = Union[ModelMetaclass, BaseModel]
    submodels = [f for f in model.model_fields if isinstance(types.get(f), model_type)]
    if instance is not None:
        defined = list(instance.model_dump(exclude_none=True, by_alias=True).keys())
        return [f for f in submodels if f in defined]
    return submodels


def coerce_extra_fields_to_type_in_df(
    data: BaseModel, model: BaseModel, df: pd.DataFrame
) -> BaseModel:
    """Coerce extra fields in data that aren't specified in Pydantic model to the type in the df.

    Note: will not coerce lists of submodels, etc.

    Args:
        data (dict): The data to coerce.
        model (BaseModel): The Pydantic model to validate the data against.
        df (pd.DataFrame): The DataFrame to coerce the data to.
    """
    out_data = copy.deepcopy(data)

    # Coerce submodels
    for field in submodel_fields_in_model(model, data):
        out_data.__dict__[field] = coerce_extra_fields_to_type_in_df(
            data.__dict__[field], model.__annotations__[field], df
        )

    for field in extra_attributes_undefined_in_model(data, model):
        try:
            v = coerce_val_to_df_types(field, data.model_extra[field], df)
        except ValueError as e:
            raise DatamodelDataframeIncompatableError(e)
        out_data.model_extra[field] = v
    return out_data
