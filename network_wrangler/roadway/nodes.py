import json
import os

from dataclasses import dataclass, field
import hashlib
from typing import Union, Optional

import geopandas as gpd
import pandas as pd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries
from shapely.geometry import Point

from ..logger import WranglerLogger


@dataclass
class NodesParams:
    primary_key: str = field(default="model_node_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: ["osm_node_id"])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: [])
    source_file: str = field(default=None)

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def unique_ids(self):
        _uids = self._addtl_unique_ids + [self.primary_key]
        return list(set(_uids))
    
    @property
    def explicit_ids(self):
        _eids = self._addtl_unique_ids + self.unique_ids
        return list(set(_eids))



class NodesSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_node_id: Series[int] = pa.Field(coerce=True, unique=True, nullable=False)

    geometry: GeoSeries
    X: Series[float] = pa.Field(coerce=True, nullable=False)
    Y: Series[float] = pa.Field(coerce=True, nullable=False)

    # optional fields
    osm_node_id: Optional[Series[str]] = pa.Field(
        coerce=True, unique=True, nullable=True
    )

    # TODO not sure we even need these anymore since we pull modal networks based on connections
    # to links now.
    transit_node: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True)
    drive_node: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True)
    walk_node: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True)
    bike_node: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True)


@check_output(NodesSchema, inplace=True)
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
    nodes_df = df_to_nodes_df(nodes_df, crs=crs, nodes_params=nodes_params)
    nodes_df.params.source_file = filename
    
    return nodes_df


@check_output(NodesSchema, inplace=True)
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
    nodes_df._metadata += ['params']

    nodes_df[nodes_df.params.idx_col] = nodes_df[nodes_df.params.primary_key]
    nodes_df.set_index(nodes_df.params.idx_col, inplace=True)
    return nodes_df


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


@check_input(NodesSchema, inplace=True)
def nodes_df_to_geojson(nodes_df: pd.DataFrame, properties: list):
    """
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    """
    from ..roadwaynetwork import RoadwayNetwork

    geojson = {"type": "FeatureCollection", "features": []}
    for _, row in nodes_df.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Point", "coordinates": []},
        }
        feature["geometry"]["coordinates"] = [row["geometry"].x, row["geometry"].y]
        feature["properties"][nodes_df.params.primary_key] = row.name
        for prop in properties:
            feature["properties"][prop] = row[prop]
        geojson["features"].append(feature)
    return geojson
