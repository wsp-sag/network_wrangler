"""Functions for reading and writing nodes data."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Union, TYPE_CHECKING, Optional

import geopandas as gpd

from pydantic import validate_call
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...utils.io import read_table, write_table
from ...params import NodesParams, LAT_LON_CRS
from ...models.roadway.tables import RoadNodesTable
from ...models._base.types import GeoFileTypes
from .create import data_to_nodes_df

if TYPE_CHECKING:
    from ...transit.network import TransitNetwork
    from ..network import RoadwayNetwork


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def read_nodes(
    filename: Union[Path, str],
    in_crs: int = LAT_LON_CRS,
    nodes_params: Optional[Union[dict, NodesParams, None]] = None,
) -> DataFrame[RoadNodesTable]:
    """Reads nodes and returns a geodataframe of nodes.

    Sets index to be a copy of the primary key.
    Validates output dataframe using NodesSchema.

    Args:
        filename (Path,str): file to read links in from.
        in_crs: coordinate reference system number that node data is in. Defaults to 4323.
        nodes_params: a NodesParams instance. Defaults to a default odesParams instance.
    """
    WranglerLogger.debug(f"Reading nodes from {filename}.")

    start_time = time.time()

    nodes_df = read_table(filename)
    WranglerLogger.debug(
        f"Read {len(nodes_df)} nodes from file in {round(time.time() - start_time, 2)}."
    )

    nodes_df = data_to_nodes_df(nodes_df, nodes_params=nodes_params, in_crs=in_crs)
    nodes_df.params.source_file = filename
    WranglerLogger.info(
        f"Read {len(nodes_df)} nodes from {filename} in {round(time.time() - start_time, 2)}."
    )
    return nodes_df


@validate_call(config=dict(arbitrary_types_allowed=True))
def nodes_df_to_geojson(nodes_df: DataFrame[RoadNodesTable], properties: list[str]):
    """Converts a nodes dataframe to a geojson.

    Attribution: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/.
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


@validate_call(config=dict(arbitrary_types_allowed=True))
def write_nodes(
    nodes_df: DataFrame[RoadNodesTable],
    out_dir: Union[str, Path],
    prefix: str,
    file_format: GeoFileTypes = "geojson",
    overwrite: bool = True,
) -> None:
    """Writes RoadNodesTable to file.

    Args:
        nodes_df: nodes dataframe
        out_dir: directory to write nodes to
        prefix: prefix to add to nodes file name
        file_format: format to write nodes in. e.g. "geojson" shp" "parquet" "csv" "txt". Defaults
            to "geojson".
        overwrite: whether to overwrite existing nodes file. Defaults to True.
    """
    nodes_file = Path(out_dir) / f"{prefix}node.{file_format}"
    write_table(nodes_df, nodes_file, overwrite=overwrite)


def get_nodes(
    transit_net: Optional[TransitNetwork] = None,
    roadway_net: Optional[RoadwayNetwork] = None,
    roadway_path: Optional[Union[str, Path]] = None,
) -> gpd.GeoDataFrame:
    """Get nodes from a transit network, roadway network, or roadway file.

    Args:
        transit_net: TransitNetwork instance
        roadway_net: RoadwayNetwork instance
        roadway_path: path to roadway network file
    """
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
