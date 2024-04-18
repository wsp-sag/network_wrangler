import json
import time

from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Any, Literal

import geopandas as gpd
import pandera as pa

from pandera import check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ..logger import WranglerLogger
from ..utils import read_table, write_table
from ..utils.models import empty_df


@dataclass
class ShapesParams:
    primary_key: str = field(default="shape_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: [])
    table_type: Literal["shapes"] = field(default="shapes")
    source_file: str = field(default=None)

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def unique_ids(self):
        return list(set(self._addtl_unique_ids.append(self.primary_key)))


class ShapesSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    shape_id: Series[Any] = pa.Field(unique=True)
    geometry: GeoSeries = pa.Field()

    class Config:
        name = "ShapesSchema"
        coerce = True


@check_output(ShapesSchema, inplace=True)
def read_shapes(
    filename: Union[str, Path],
    crs: int = 4326,
    shapes_params: Union[dict, ShapesParams] = None,
) -> ShapesSchema:
    """Reads shapes and returns a geodataframe of shapes if filename is found. Otherwise, returns
        empty GeoDataFrame conforming to ShapesSchema.

    Sets index to be a copy of the primary key.
    Validates output dataframe using ShapesSchema.

    Args:
        filename (str): file to read shapes in from.
        crs: coordinate reference system number. Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.
    """
    if not Path(filename).exists():
        WranglerLogger.warning(
            f"Shapes file {filename} not found, but is optional. \
                               Returning empty shapes dataframe."
        )
        return empty_df(ShapesSchema)

    start_time = time.time()
    WranglerLogger.debug(f"Reading shapes from {filename}.")

    shapes_df = read_table(filename)
    WranglerLogger.debug(
        f"Read {len(shapes_df)} shapes from file in {round(time.time() - start_time,2)}."
    )
    shapes_df = df_to_shapes_df(shapes_df, crs=crs, shapes_params=shapes_params)
    shapes_df.params.source_file = filename
    WranglerLogger.info(
        f"Read {len(shapes_df)} shapes from {filename} in {round(time.time() - start_time,2)}."
    )
    return shapes_df


@check_output(ShapesSchema, inplace=True)
def df_to_shapes_df(
    shapes_df: gpd.GeoDataFrame, crs: int = 4326, shapes_params: ShapesParams = None
) -> gpd.GeoDataFrame:
    """

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        shapes_df (gpd.GeoDataFrame): _description_
        crs: coordinate reference system number. Defaults to 4323.
        shapes_params: a ShapesParams instance. Defaults to a default ShapesParams instance.

    Returns:
        pd.DataFrame: _description_
    """
    # Set dataframe-level variables
    shapes_df.crs = crs
    shapes_df.gdf_name = "network_shapes"

    # Validate and coerce to schema
    shapes_df = ShapesSchema.validate(shapes_df, lazy=True)

    # Add parameters so that they can be accessed as dataframe variables
    if shapes_params is None:
        shapes_params = ShapesParams()

    shapes_df.__dict__["params"] = shapes_params

    shapes_df[shapes_df.params.idx_col] = shapes_df[shapes_df.params.primary_key]
    shapes_df.dropna(subset=["geometry", shapes_df.params.primary_key], inplace=True)
    shapes_df.set_index(shapes_df.params.idx_col, inplace=True)
    shapes_df._metadata += ["params"]

    return shapes_df


def write_shapes(
    shapes_df: gpd.GeoDataFrame,
    out_dir: Union[str, Path],
    prefix: str,
    format: str,
    overwrite: bool,
) -> None:
    shapes_file = Path(out_dir) / f"{prefix}shape.{format}"
    write_table(shapes_df, shapes_file, overwrite=overwrite)
