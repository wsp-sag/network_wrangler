import json
import os

from dataclasses import dataclass, field
from typing import Union, Mapping, Any, Optional, List

import geopandas as gpd
import pandas as pd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

import pandera as pa
from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ..logger import WranglerLogger
from ..utils import line_string_from_location_references

MODES_TO_NETWORK_LINK_VARIABLES = {
    "drive": ["drive_access"],
    "bus": ["bus_only", "drive_access"],
    "rail": ["rail_only"],
    "transit": ["bus_only", "rail_only", "drive_access"],
    "walk": ["walk_access"],
    "bike": ["bike_access"],
}


@dataclass
class LinksParams:
    primary_key: str = field(default="model_link_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: [])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: ["osm_link_id"])
    from_node: str = field(default="A")
    to_node: str = field(default="B")
    fk_to_shape: str = field(default="shape_id")
    source_file: str = field(default=None)
    modes_to_network_link_variables: dict = field(
        default_factory=lambda: MODES_TO_NETWORK_LINK_VARIABLES
    )

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def fks_to_nodes(self):
        return [self.from_node, self.to_node]

    @property
    def unique_ids(self):
        _uids = self._addtl_unique_ids + [self.primary_key]
        return list(set(_uids))

    @property
    def explicit_ids(self):
        return list(set(self.unique_ids + self._addtl_explicit_ids))


class LinksSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    A: Series[Any] = pa.Field(nullable=False)
    B: Series[Any] = pa.Field(nullable=False)
    geometry: GeoSeries = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=False)
    rail_only: Series[bool] = pa.Field(coerce=True, nullable=False)
    bus_only: Series[bool] = pa.Field(coerce=True, nullable=False)
    drive_access: Series[bool] = pa.Field(coerce=True, nullable=False)
    bike_access: Series[bool] = pa.Field(coerce=True, nullable=False)
    walk_access: Series[bool] = pa.Field(coerce=True, nullable=False)

    roadway: Series[str] = pa.Field(nullable=False)
    lanes: Series[int] = pa.Field(coerce=True, nullable=False)

    # Optional Fields
    truck_access: Optional[Series[bool]] = pa.Field(coerce=True, nullable=True)
    osm_link_id: Optional[Series[str]] = pa.Field(coerce=True, nullable=True)
    locationReferences: Optional[Series[Any]] = pa.Field(nullable=True)
    shape_id: Optional[Series[Any]] = pa.Field(nullable=True)

    @pa.dataframe_check
    def unique_ab(cls, df: pd.DataFrame) -> bool:
        return ~df[["A", "B"]].duplicated()


@check_output(LinksSchema, inplace=True)
def read_links(
    filename: str, crs: int = 4326, links_params: Union[dict, LinksParams] = None
) -> gpd.GeoDataFrame:
    """Reads links and returns a geodataframe of links.

    Sets index to be a copy of the primary key.
    Validates output dataframe using LinksSchema.

    Args:
        filename (str): file to read links in from.
        crs: coordinate reference system number. Defaults to 4323.
        link_params: a LinkParams instance. Defaults to a default LinkParams instance.
    """
    WranglerLogger.info(f"Reading links from {filename}.")
    with open(filename) as f:
        link_json = json.load(f)
    link_properties = pd.DataFrame(link_json)
    link_geometries = [
        line_string_from_location_references(g["locationReferences"]) for g in link_json
    ]
    links_df = gpd.GeoDataFrame(link_properties, geometry=link_geometries)
    links_df = df_to_links_df(links_df, crs=crs, links_params=links_params)
    links_df.params.source_file = filename
    # need to add params to _metadata in order to make sure it is copied. 
    # see: https://stackoverflow.com/questions/50372509/why-are-attributes-lost-after-copying-a-pandas-dataframe/50373364#50373364
    
    return links_df


@check_output(LinksSchema, inplace=True)
def df_to_links_df(
    links_df: gpd.GeoDataFrame, crs: int = 4326, links_params: LinksParams = None
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
    links_df.crs = crs
    links_df.gdf_name = "network_links"

    # make link parameters available as a dataframe property
    if links_params is None:
        links_params = LinksParams()

    links_df.__dict__["params"] = links_params
    
    links_df[links_df.params.idx_col] = links_df[links_df.params.primary_key]
    links_df.set_index(links_df.params.idx_col, inplace=True)

    links_df._metadata += ['params']
    links_df._metadata += ['crs']
    return links_df


def validate_wrangler_links_file(
    link_file, schema_location: str = "roadway_network_link.json"
):
    """
    Validate roadway network data link schema and output a boolean
    """

    if not os.path.exists(schema_location):
        base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "schemas")
        schema_location = os.path.join(base_path, schema_location)

    with open(schema_location) as schema_json_file:
        schema = json.load(schema_json_file)

    with open(link_file) as link_json_file:
        json_data = json.load(link_json_file)

    try:
        validate(json_data, schema)
        return True

    except ValidationError as exc:
        WranglerLogger.error("Failed Link schema validation: Validation Error")
        WranglerLogger.error("Link File Loc:{}".format(link_file))
        WranglerLogger.error("Path:{}".format(exc.path))
        WranglerLogger.error(exc.message)

    except SchemaError as exc:
        WranglerLogger.error("Invalid Link Schema")
        WranglerLogger.error("Link Schema Loc: {}".format(schema_location))
        WranglerLogger.error(json.dumps(exc.message, indent=2))

    return False


@pd.api.extensions.register_dataframe_accessor("mode_query")
class ModeLinkAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def __call__(self, modes: List[str]):
        # filter the rows where drive_access is True
        if isinstance(modes, str):
            modes = [modes]
        _mode_link_props = list(
            set(
                [
                    m
                    for m in modes
                    for m in self._obj.params.modes_to_network_link_variables[m]
                ]
            )
        )

        modal_links_df = self._obj.loc[self._obj[_mode_link_props].any(axis=1)]
        return modal_links_df


@check_input(LinksSchema, inplace=True)
def links_df_to_json(links_df: pd.DataFrame, properties: list):
    """Export pandas dataframe as a json object.

    Modified from: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/

    Args:
        df: Dataframe to export
        properties: list of properties to export
    """

    # can't remember why we need this?
    if "distance" in properties:
        links_df["distance"].fillna(0)

    json = []
    for _, row in links_df.iterrows():
        feature = {}
        for prop in properties:
            feature[prop] = row[prop]
        json.append(feature)

    return json
