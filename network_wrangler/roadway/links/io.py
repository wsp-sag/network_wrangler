"""Functions to read in and write out a RoadLinksTable."""

from __future__ import annotations
import time
from typing import Union, TYPE_CHECKING
from pathlib import Path

import pandas as pd

from pydantic import validate_call
from pandera.typing import DataFrame

from ...logger import WranglerLogger
from ...models.roadway.tables import RoadLinksTable, RoadNodesTable
from ...models._base.types import GeoFileTypes
from ...params import LinksParams, LAT_LON_CRS
from ...utils.io import read_table, write_table
from .create import data_to_links_df


@validate_call(config=dict(arbitrary_types_allowed=True), validate_return=True)
def read_links(
    filename: Union[Path, str],
    in_crs: int = LAT_LON_CRS,
    links_params: Union[dict, LinksParams, None] = None,
    nodes_df: DataFrame[RoadNodesTable] = None,
) -> DataFrame[RoadLinksTable]:
    """Reads links and returns a geodataframe of links conforming to RoadLinksTable.

    Sets index to be a copy of the primary key.
    Validates output dataframe using RoadLinksTable

    Args:
        filename (str): file to read links in from.
        in_crs: coordinate reference system number any link geometries are stored in.
            Defaults to 4323.
        links_params: a LinkParams instance. Defaults to a default LinkParams instance.
        nodes_df: a RoadNodesTable to gather geometry from. Necesary if geometry is not
            provided. Defaults to None.
    """
    WranglerLogger.info(f"Reading links from {filename}.")
    start_t = time.time()
    links_df = read_table(filename)
    WranglerLogger.debug(f"Read {len(links_df)} links in {round(time.time() - start_t, 2)}.")
    links_df = data_to_links_df(
        links_df, in_crs=in_crs, links_params=links_params, nodes_df=nodes_df
    )
    links_df.params.source_file = filename
    WranglerLogger.info(
        f"Read + transformed {len(links_df)} links from \
            {filename} in {round(time.time() - start_t, 2)}."
    )
    return links_df


@validate_call(config=dict(arbitrary_types_allowed=True))
def write_links(
    links_df: DataFrame[RoadLinksTable],
    out_dir: Union[str, Path] = ".",
    prefix: str = "",
    file_format: GeoFileTypes = "json",
    overwrite: bool = False,
    include_geometry: bool = False,
) -> None:
    """Writes links to a file.

    Args:
        links_df: DataFrame[RoadLinksTable] to write out.
        out_dir: directory to write files to. Defaults to ".".
        prefix: prefix to add to the filename. Defaults to "".
        file_format: file format to write out to. Defaults to "json".
        overwrite: if True, will overwrite existing files. Defaults to False.
        include_geometry: if True, will include geometry in the output. Defaults to False.
    """
    if not include_geometry and file_format == "geojson":
        file_format = "json"

    links_file = Path(out_dir) / f"{prefix}link.{file_format}"

    if not include_geometry:
        geo_cols = links_df.select_dtypes(include=["geometry"]).columns.tolist()
        links_df = pd.DataFrame(links_df)
        links_df = links_df.drop(columns=geo_cols)

    write_table(links_df, links_file, overwrite=overwrite)
