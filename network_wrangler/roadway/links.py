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

from ..logger import WranglerLogger
from ..utils import line_string_from_location_references


@dataclass
class LinksParams:
    primary_key: str = field(default="model_link_id")
    _addtl_unique_ids: list[str] = field(default_factory=list, default=[])
    _addtl_explicit_ids: list[str] = field(
        default_factory=list, default=["osm_link_id"]
    )
    from_node: str = field(default="A")
    to_node: str = field(default="B")
    fk_to_shape: str = field(default="shape_id")
    source_file: str

    @property
    def idx_col(self):
        return self.primary_key + "_idx"

    @property
    def fks_to_nodes(self):
        return [self.from_node, self.to_node]

    @property
    def unique_ids(self):
        return list(set(self._addtl_unique_ids.append(self.primary_key)))

    @property
    def explicit_ids(self):
        return list(set(self.unique_ids + self.p_addtl_explicit_ids))


class LinksSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    A: Series = pa.Field()
    B: Series = pa.Field()
    geometry: Series = pa.Field(required=True)
    name: Series[str] = pa.Field(required=True)
    rail_only: Series[bool] = pa.Field(coerce=True)
    bus_only: Series[bool] = pa.Field(coerce=True)
    drive_access: Series[bool] = pa.Field(coerce=True)
    bike_access: Series[bool] = pa.Field(coerce=True)
    walk_access: Series[bool] = pa.Field(coerce=True)
    truck_access: Series[bool] = pa.Field(coerce=True)

    roadway: Series[str] = pa.Field()
    lanes: Series[int] = pa.Field(coerce=True)

    osm_link_id: Series[str] = pa.Field(coerce=True, required=False)
    locationReferences: Series[list] = pa.Field(required=False)
    shape_id: Series[bool] = pa.Field(coerce=True, required=False, unique=True)

    @pa.dataframe_check
    def unique_ab(cls, df: pd.DataFrame) -> bool:
        return ~df[["A", "B"]].duplicated()


@check_output(LinksSchema)
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
    links_params.source_file = filename
    links_df = df_to_links_df(links_df, crs=crs, links_params=links_params)

    return links_df


@check_input(LinksSchema)
@check_output(LinksSchema)
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
    if link_params is None:
        link_params = LinksParams()

    links_df.__dict__["params"] = link_params

    links_df[links_df.params.idx_col] = links_df[links_df.primary_key]
    links_df.set_index(links_df.params.idx_col, inplace=True)


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
