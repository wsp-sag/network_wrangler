import json
import os

from dataclasses import dataclass, field
from typing import Union, Mapping, Any, Optional

import geopandas as gpd
import pandas as pd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ..logger import WranglerLogger
from ..utils import line_string_from_location_references


@dataclass
class LinksParams:
    primary_key: str = field(default="model_link_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: [])
    _addtl_explicit_ids: list[str] = field(
        default_factory= lambda: ["osm_link_id"]
    )
    from_node: str = field(default="A")
    to_node: str = field(default="B")
    fk_to_shape: str = field(default="shape_id")
    source_file: str = field(default=None)

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

MODES_TO_NETWORK_LINK_VARIABLES = {
    "drive": ["drive_access"],
    "bus": ["bus_only", "drive_access"],
    "rail": ["rail_only"],
    "transit": ["bus_only", "rail_only", "drive_access"],
    "walk": ["walk_access"],
    "bike": ["bike_access"],
}

class LinksSchema(DataFrameModel):
    """Datamodel used to validate if links_df is of correct format and types."""

    model_link_id: Series[int] = pa.Field(coerce=True, unique=True)
    A: Series[Any] = pa.Field()
    B: Series[Any] = pa.Field()
    geometry: GeoSeries = pa.Field()
    name: Series[str] = pa.Field()
    rail_only: Series[bool] = pa.Field(coerce=True)
    bus_only: Series[bool] = pa.Field(coerce=True)
    drive_access: Series[bool] = pa.Field(coerce=True)
    bike_access: Series[bool] = pa.Field(coerce=True)
    walk_access: Series[bool] = pa.Field(coerce=True)
    truck_access: Series[bool] = pa.Field(coerce=True)

    roadway: Series[str] = pa.Field()
    lanes: Series[int] = pa.Field(coerce=True)

    osm_link_id: Optional[Series[str]] = pa.Field(coerce=True)
    locationReferences: Optional[Series]
    shape_id: Optional[Series[bool]] = pa.Field(coerce=True, unique=True)

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
    links_df = df_to_links_df(links_df, crs=crs, links_params=links_params)
    links_params.source_file = filename

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
    if links_params is None:
        links_params = LinksParams()

    links_df.__dict__["params"] = links_params

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

@pd.api.extensions.register_dataframe_accessor("mode_query")
class ModeLinkAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def __call__(self,modes:list,str):
        # filter the rows where drive_access is True
        if isinstance(modes,str):
            modes = [modes]
        _mode_link_props = list(
            set([m for m in modes for m in MODES_TO_NETWORK_LINK_VARIABLES[m]])
        )
        modal_links_df = self._obj.loc[self._obj[_mode_link_props].any(axis=1)]
        return  modal_links_df

@pd.api.extensions.register_dataframe_accessor("dict_query")
class DictQueryAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def __call__(self,selection_dict:dict):
        # filter the rows where drive_access is True
        _sel_query = dict_to_query(selection_dict)
        q_links_df = self.net.links_df.query(_sel_query, engine="python")
        if len(q_links_df) == 0:
            WranglerLogger.warning(f"No links found using selection: {selection_dict}")
        return q_links_df


def dict_to_query(
    selection_dict: Mapping[str, Any],
) -> str:
    """Generates the query of from selection_dict.

    Args:
        selection_dict: selection dictionary

    Returns:
        _type_: Query value
    """
    WranglerLogger.debug("Building selection query")

    def _kv_to_query_part(k, v, _q_part=""):
        if isinstance(v, list):
            _q_part += "(" + " or ".join([_kv_to_query_part(k, i) for i in v]) + ")"
            return _q_part
        if isinstance(v, str):
            return k + '.str.contains("' + v + '")'
        else:
            return k + "==" + str(v)

    query = (
        "("
        + " and ".join([_kv_to_query_part(k, v) for k, v in selection_dict.items()])
        + ")"
    )
    WranglerLogger.debug(f"Selection query:\n{query}")
    return query


