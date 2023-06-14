import json
import os

from dataclasses import dataclass, field
from typing import Union, Optional, List

import geopandas as gpd
import numpy as np
import pandas as pd
import pandera as pa

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries
from shapely.geometry import Point

from ..utils import findkeys, coerce_val_to_series_type, point_from_xy
from ..logger import WranglerLogger


@dataclass
class NodesParams:
    primary_key: str = field(default="model_node_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: ["osm_node_id"])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: [])
    source_file: str = field(default=None)
    x_field: str = field(default="X")
    y_field: str = field(default="Y")

    @property
    def geometry_props(self) -> List[str]:
        return [self.x_field, self.y_field, "geometry"]

    @property
    def idx_col(self) -> str:
        return self.primary_key + "_idx"

    @property
    def unique_ids(self) -> List[str]:
        _uids = self._addtl_unique_ids + [self.primary_key]
        return list(set(_uids))

    @property
    def explicit_ids(self) -> List[str]:
        _eids = self._addtl_unique_ids + self.unique_ids
        return list(set(_eids))

    @property
    def display_cols(self):
        return self.explicit_ids


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


def read_nodes(
    filename: str, crs: int = 4326, nodes_params: Union[dict, NodesParams] = None
) -> gpd.GeoDataFrame:
    """Reads nodes and returns a geodataframe of nodes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using NodsSchema.

    Args:
        filename (str): file to read links in from.
        crs: coordinate reference system number. Defaults to 4323.
        nodes_params: a NodesParams instance. Defaults to a default odesParams instance.
    """
    WranglerLogger.info(f"Reading node from {filename}.")
    with open(filename) as f:
        node_geojson = json.load(f)
    nodes_data = [g["properties"] for g in node_geojson["features"]]
    node_geometries = [
        Point(g["geometry"]["coordinates"]) for g in node_geojson["features"]
    ]
    nodes_df = nodes_data_to_nodes_df(
        nodes_data, nodes_params=nodes_params, crs=crs, node_geometries=node_geometries
    )
    nodes_df.params.source_file = filename

    return nodes_df


@check_output(NodesSchema, inplace=True)
def nodes_data_to_nodes_df(
    nodes_data: List[dict],
    nodes_params: NodesParams = None,
    crs: int = 4326,
    node_geometries: List = None,
) -> gpd.GeoDataFrame:
    """Turn list of nodes data into nodes dataframe given either node_geometries or X,Y properties.

    Validates output to NodesSchema.

    Args:
        nodes_data (List[dict]): List of dictionaries with node properties.
        nodes_params (NodesParams, optional): NodesParams instance. Defaults to Default NodeParams
            properties.
        crs: Coordinate references system id. Defaults to 4326.
        node_geometries (List, optional): List of node_geometries. If None, will calculate from X
            and Y properties.

    Returns:
        gpd.GeoDataFrame: _description_
    """
    if len(nodes_data) < 25:
        WranglerLogger.debug(
            f"Coercing following data to nodes_df:\n{pd.DataFrame(nodes_data)}"
        )

    if nodes_params is None:
        nodes_params = NodesParams()

    if node_geometries is None:
        temp_nodes_df = pd.DataFrame(nodes_data)
        if (
            not set(temp_nodes_df.columns).issubset(["X", "Y"])
            and temp_nodes_df[["X", "Y"]].isnull().values.any()
        ):
            raise NodeAddError(
                "Must have X and Y data for all nodes in order to compute geometry"
            )

        node_geometries = temp_nodes_df.apply(
            lambda x: point_from_xy(
                x["X"],
                x["Y"],
                xy_crs=crs,
                point_crs=crs,
            ),
            axis=1,
        )
    nodes_df = gpd.GeoDataFrame(nodes_data, geometry=node_geometries)
    if len(nodes_df) < 25:
        WranglerLogger.debug(f"nodes_df:\n{nodes_df[['model_node_id','geometry']]}")
    nodes_df.crs = crs
    nodes_df.gdf_name = "network_nodes"

    if "X" not in nodes_df.columns or "Y" not in nodes_df.columns:
        if nodes_df[["geometry"]].isnull().values.any():
            raise NodeAddError("Must have geometry data for all nodes.")
        nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
        nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

    nodes_df.__dict__["params"] = nodes_params
    nodes_df._metadata += ["params"]

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


@pd.api.extensions.register_dataframe_accessor("set_node_prop")
class ModeNodeAccessor:
    def __init__(self, nodes_df):
        self._nodes_df = nodes_df

    def __call__(
        self,
        node_idx: list,
        prop_name: str,
        prop_dict: dict,
        existing_value_conflict_error: bool = False,
        _geometry_ok: bool = False,
    ) -> pd.DataFrame:
        """Sets the value of a node property.

        args:
            node_idx: list of node indices to change
            prop_name: property name to change
            prop_dict: dictionary of value from project_card
            existing_value_conflict_error: If True, will trigger an error if the existing
                specified value in the project card doesn't match the value in nodes_df.
                Otherwise, will only trigger a warning. Defaults to False.
            _geometry_ok: if False, will not let you change geometry-related fields. Should
                only be changed to True by internal processes that know that geometry is changing
                and will update it in appropriate places in network. Defaults to False.

        """
        # Should not be used to update node geometry fields unless explicity set to OK:
        if prop_name in self._nodes_df.params.geometry_props and not _geometry_ok:
            raise NodeChangeError("Cannot unilaterally change geometry property.")
        # check existing if necessary
        if "existing" in prop_dict:
            if self._nodes_df.loc[node_idx, prop_name].eq(prop_dict["existing"]).all():
                WranglerLogger.warning(
                    f"Existing value defined for {prop_name} in project card \
                    does not match the value in the roadway network for the selected links. \n\
                    Specified Existing:{prop_dict['existing']}\n\
                    Actual Existing:\n {self._nodes_df.loc[self._node_idx,prop_name]}."
                )

                if existing_value_conflict_error:
                    raise NodeChangeError(
                        "Conflict between specified existing and actual existing values"
                    )

        _nodes_df = self._nodes_df.copy()
        # if it is a new attribute then initialize with NaN values + set to type of "set" value
        if prop_name not in _nodes_df:
            _nodes_df[prop_name] = np.NaN

            _set_vals = list(findkeys(prop_dict, "set")) + list(
                findkeys(prop_dict, "change")
            )
            WranglerLogger.debug(
                f"Setting new node property type to {type(_set_vals[0])}"
            )
            _nodes_df[prop_name] = _nodes_df[prop_name].astype(type(_set_vals[0]))

        elif "set" in prop_dict:
            _nodes_df.loc[node_idx, prop_name] = coerce_val_to_series_type(
                prop_dict["set"], _nodes_df[prop_name]
            )

        elif "change" in prop_dict:
            _nodes_df.loc[node_idx, prop_name] = _nodes_df.loc[
                node_idx, prop_name
            ].apply(lambda x: x + float(prop_dict["change"]))

        else:
            raise NodeChangeError(
                "Couldn't find correct node change spec in: {prop_dict}"
            )

        return _nodes_df


class NodeChangeError(Exception):
    pass


class NodeAddError(Exception):
    pass
