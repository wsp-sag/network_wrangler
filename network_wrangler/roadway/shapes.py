import json
import os

from dataclasses import dataclass, field
from typing import Union, Any

import geopandas as gpd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ..logger import WranglerLogger


@dataclass
class ShapesParams:
    primary_key: str = field(default="shape_id")
    _addtl_unique_ids: list[str] = field(default_factory= lambda: [])
    source_file: str = field(default = None)

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def unique_ids(self):
        return list(set(self._addtl_unique_ids.append(self.primary_key)))


class ShapesSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    shape_id: Series[Any] = pa.Field(unique=True)
    geometry: GeoSeries


@check_output(ShapesSchema)
def read_shapes(
    filename: str, crs: int = 4326, shapes_params: Union[dict, ShapesParams] = None
) -> gpd.GeoDataFrame:
    """Reads shapes and returns a geodataframe of shapes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using ShapesSchema.

    Args:
        filename (str): file to read shapes in from.
        crs: coordinate reference system number. Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.
    """
    WranglerLogger.info(f"Reading shapes from {filename}.")
    with open(filename) as f:
        shapes_df = gpd.read_file(f)
    shapes_df.dropna(subset=["geometry", "id"], inplace=True)
    shapes_df = df_to_shapes_df(shapes_df, crs=crs, shapes_params=shapes_params)
    shapes_params.source_file = filename

    return shapes_df


@check_input(ShapesSchema)
@check_output(ShapesSchema)
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
    shapes_df.crs = crs
    shapes_df.gdf_name = "network_shapes"

    # make shapes parameters available as a dataframe property
    if shapes_params is None:
        shapes_params = ShapesParams()

    shapes_df.__dict__["params"] = shapes_params

    shapes_df[shapes_df.params.idx_col] = shapes_df[shapes_df.primary_key]
    shapes_df.set_index(shapes_df.params.idx_col, inplace=True)


def validate_wrangler_shapes_file(
    shapes_file: str, schema_location: str = "roadway_network_shape.json"
) -> bool:
    """
    Validate roadway network data node schema and output a boolean
    """
    if not os.path.exists(schema_location):
        base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "schemas")
        schema_location = os.path.join(base_path, schema_location)

    with open(schema_location) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(shapes_file) as node_json_file:
        json_data = json.load(node_json_file)

    try:
        validate(json_data, schema)
        return True

    except ValidationError as exc:
        WranglerLogger.error("Failed Shapes schema validation: Validation Error")
        WranglerLogger.error("Shapes File Loc:{}".format(shapes_file))
        WranglerLogger.error("Shapes Schema Loc:{}".format(schema_location))
        WranglerLogger.error(exc.message)

    except SchemaError as exc:
        WranglerLogger.error("Invalid Shape Schema")
        WranglerLogger.error("Shape Schema Loc:{}".format(schema_location))
        WranglerLogger.error(json.dumps(exc.message, indent=2))

    return False
