import json
import os

from dataclasses import asdict, dataclass, field
from typing import Union

import geopandas as gpd
import pandas as pd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from pandas import Series
from pandera import check_input, check_output, DataFrameModel
from shapely.geometry import Point

from ..logger import WranglerLogger
from ..utils import line_string_from_location_references


@dataclass
class NodesParams:
    primary_key: str = field(default="model_node_id")
    _addtl_unique_ids: list[str] = field(default_factory=list, default=["osm_node_id"])
    source_file: str

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def fks_to_nodes(self):
        return self.from_node, self.to_node

    @property
    def unique_ids(self):
        return list(set(self._addtl_unique_ids.append(self.primary_key)))


class NodesSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_node_id: Series[int] = pa.Field(coerce=True, unique=True)
    transit_node: Series[bool] = pa.Field(coerce=True)
    drive_node: Series[bool] = pa.Field(coerce=True)
    walk_node: Series[bool] = pa.Field(coerce=True)
    bike_node: Series[bool] = pa.Field(coerce=True)
    geometry: Series = pa.Field(required=True)
    X: Series[float] = pa.Field(required=True)
    Y: Series[float] = pa.Field(required=True)

    osm_node_id: Series[str] = pa.Field(coerce=True, required=False, unique=True)

    @pa.dataframe_check
    def unique_ab(cls, df: pd.DataFrame) -> bool:
        return ~df[["A", "B"]].duplicated()


@check_output(NodesSchema)
def read_nodes(
    filename: str, crs: int = 4326, nodes_params: Union[dict, NodesParams] = None
) -> gpd.GeoDataFrame:
    """Reads nodes and returns a geodataframe of links.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        filename (str): file to read links in from.
        crs: coordinate reference system number. Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.
    """
    WranglerLogger.info(f"Reading node from {filename}.")
    with open(filename) as f:
        node_geojson = json.load(f)
    node_properties = pd.DataFrame([g["properties"] for g in node_geojson["features"]])

    node_geometries = [
        Point(g["geometry"]["coordinates"]) for g in node_geojson["features"]
    ]
    nodes_df = gpd.GeoDataFrame(node_properties, geometry=node_geometries)
    nodes_params.source_file = filename
    nodes_df = df_to_nodes_df(nodes_df, crs=crs, nodes_params=nodes_params)

    return nodes_df


@check_input(NodesSchema)
@check_output(NodesSchema)
def df_to_nodes_df(
    nodes_df: gpd.GeoDataFrame, crs: int = 4326, nodes_params: NodesParams = None
) -> gpd.GeoDataFrame:
    """

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        links_df (pd.DataFrame): _description_
        crs: coordinate reference system number. Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.

    Returns:
        pd.DataFrame: _description_
    """
    nodes_df.crs = crs
    nodes_df.gdf_name = "network_nodes"
    nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
    nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

    # make link parameters available as a dataframe property
    if nodes_params is None:
        nodes_params = NodesParams()

    nodes_df.__dict__["params"] = nodes_params

    nodes_df[nodes_df.params.idx_col] = nodes_df[nodes_df.primary_key]
    nodes_df.set_index(nodes_df.params.idx_col, inplace=True)


def validate_wrangler_nodes_file(
    node_file: str, schema_location: str = "roadway_network_node.json"
) -> bool:
    """
    Validate roadway network data node schema and output a boolean
    """
    if not os.path.exists(schema_location):
        base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "schemas")
        schema_location = os.path.join(base_path, schema_location)

    with open(schema_location) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(node_file) as node_json_file:
        json_data = json.load(node_json_file)

    try:
        validate(json_data, schema)
        return True

    except ValidationError as exc:
        WranglerLogger.error("Failed Node schema validation: Validation Error")
        WranglerLogger.error("Node File Loc:{}".format(node_file))
        WranglerLogger.error("Node Schema Loc:{}".format(schema_location))
        WranglerLogger.error(exc.message)

    except SchemaError as exc:
        WranglerLogger.error("Invalid Node Schema")
        WranglerLogger.error("Node Schema Loc:{}".format(schema_location))
        WranglerLogger.error(json.dumps(exc.message, indent=2))

    return False
