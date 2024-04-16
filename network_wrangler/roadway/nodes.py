import json
import time

from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Optional, List, Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import pandera as pa

from pandera import check_input, check_output, DataFrameModel
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries

from ..utils import (
    findkeys,
    coerce_val_to_series_type,
    point_from_xy,
    read_table,
    write_table,
)
from ..logger import WranglerLogger


# Remove the unused import statement
# from network_wrangler.roadway.nodes_params import NodesParams


@dataclass
class NodesParams:
    primary_key: str = field(default="model_node_id")
    _addtl_unique_ids: list[str] = field(default_factory=lambda: ["osm_node_id"])
    _addtl_explicit_ids: list[str] = field(default_factory=lambda: [])
    source_file: str = field(default=None)
    table_type: Literal["nodes"] = field(default="nodes")
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
        coerce=True,
        unique=True,
        nullable=True,
        default="",
    )

    inboundReferenceIds: Optional[Series[List[str]]] = pa.Field(
        coerce=True, nullable=True
    )
    outboundReferenceIds: Optional[Series[List[str]]] = pa.Field(
        coerce=True, nullable=True
    )


def read_nodes(
    filename: Union[Path, str],
    crs: int = 4326,
    nodes_params: Union[dict, NodesParams] = None,
) -> gpd.GeoDataFrame:
    """Reads nodes and returns a geodataframe of nodes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using NodesSchema.

    Args:
        filename (Path,str): file to read links in from.
        crs: coordinate reference system number. Defaults to 4323.
        nodes_params: a NodesParams instance. Defaults to a default odesParams instance.
    """
    WranglerLogger.debug(f"Reading nodes from {filename}.")

    start_time = time.time()

    nodes_df = read_table(filename)
    WranglerLogger.debug(
        f"Read {len(nodes_df)} nodes from file in {round(time.time() - start_time,2)}."
    )

    nodes_df = _nodes_data_to_nodes_df(nodes_df, nodes_params=nodes_params, crs=crs)
    nodes_df.params.source_file = filename
    WranglerLogger.info(
        f"Read {len(nodes_df)} nodes from {filename} in {round(time.time() - start_time,2)}."
    )
    return nodes_df


def _create_node_geometries_from_xy(
    nodes_df: pd.DataFrame, crs: int
) -> gpd.GeoDataFrame:
    """Fixes geometries in nodes_df if necessary using X and Y columns

    Args:
        nodes_df: nodes dataframe to fix geometries in.

    Returns:
        gpd.GeoDataFrame: nodes dataframe with fixed geometries.
    """
    if not isinstance(nodes_df, pd.DataFrame):
        nodes_df = pd.DataFrame(nodes_df)
    if "X" not in nodes_df.columns or "Y" not in nodes_df.columns:
        raise NodeAddError("Must have X and Y properties to create geometries from.")

    geo_start_time = time.time()
    if "geometry" in nodes_df:
        nodes_df["geometrys"] = nodes_df["geometry"].fillna(
            lambda x: point_from_xy(x["X"], x["Y"], xy_crs=crs, point_crs=crs),
        )
        WranglerLogger.debug(
            f"Filled missing geometry from X and Y in {round(time.time() - geo_start_time,2)}."
        )
        return nodes_df

    node_geometries = nodes_df.apply(
        lambda x: point_from_xy(x["X"], x["Y"], xy_crs=crs, point_crs=crs),
        axis=1,
    )
    WranglerLogger.debug(
        f"Created node geometries from X and Y in {round(time.time() - geo_start_time,2)}."
    )
    nodes_gdf = gpd.GeoDataFrame(nodes_df, geometry=node_geometries)
    return nodes_gdf


@check_output(NodesSchema, inplace=True)
def _nodes_data_to_nodes_df(
    nodes_df: gpd.GeoDataFrame,
    nodes_params: NodesParams = None,
    crs: int = 4326,
) -> gpd.GeoDataFrame:
    """Turn nodes data into official nodes dataframe.

    Adds missing geometry.
    Makes sure X and Y are consistent with geometry GeoSeries.
    Adds `params` as a _metadata attribute of nodes_df.
    Adds CRS.
    Copies and sets idx to primary_key.
    Validates output to NodesSchema.

    Args:
        nodes_df : Nodes dataframe
        nodes_params (NodesParams, optional): NodesParams instance. Defaults to Default NodeParams
            properties.
        crs: Coordinate references system id. Defaults to 4326.

    Returns:
        gpd.GeoDataFrame: _description_
    """
    WranglerLogger.debug("Turning node data into official nodes_df")

    if isinstance(nodes_df, gpd.GeoDataFrame) and nodes_df.crs != crs:
        nodes_df = nodes_df.to_crs(crs)

    if (
        not isinstance(nodes_df, gpd.GeoDataFrame)
        or nodes_df.geometry.isnull().values.any()
    ):
        nodes_df = _create_node_geometries_from_xy(nodes_df, crs=crs)

    # Make sure values are consistent
    nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
    nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

    if len(nodes_df) < 5:
        WranglerLogger.debug(
            f"nodes_df:\n{nodes_df[['model_node_id','geometry','X','Y']]}"
        )

    nodes_df.gdf_name = "network_nodes"

    if nodes_params is None:
        nodes_params = NodesParams()
    nodes_df.__dict__["params"] = nodes_params
    nodes_df._metadata += ["params"]

    nodes_df[nodes_df.params.idx_col] = nodes_df[nodes_df.params.primary_key]
    nodes_df.set_index(nodes_df.params.idx_col, inplace=True)

    return nodes_df


def get_nodes(
    transit_net: "TransitNetwork" = None,
    roadway_net: "RoadwayNetwork" = None,
    roadway_path: Union[str, Path] = None,
) -> gpd.GeoDataFrame:
    if transit_net is not None and transit_net.road_net is not None:
        return transit_net.road_net.nodes_df
    if roadway_net is not None:
        return roadway_net.nodes_df
    elif roadway_path is not None:
        nodes_path = Path(roadway_net)
        if nodes_path.is_dir():
            nodes_path = next(nodes_path.glob("*node*."))
        return read_nodes(nodes_path)
    else:
        raise ValueError(
            "nodes_df must either be given or provided via an associated \
                            road_net or by providing a roadway_net path or instance."
        )


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
        if not nodes_df.params.table_type == "nodes":
            raise NotNodesError(
                "`set_node_prop` is only available to nodes dataframes."
            )

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


@check_input(NodesSchema, inplace=True)
def write_nodes(
    nodes_df: gpd.GeoDataFrame,
    out_dir: Union[str, Path],
    prefix: str,
    format: str,
    overwrite: bool,
) -> None:
    nodes_file = Path(out_dir) / f"{prefix}node.{format}"
    write_table(nodes_df, nodes_file, overwrite=overwrite)


class NotNodesError(Exception):
    pass


class NodeChangeError(Exception):
    pass


class NodeAddError(Exception):
    pass
