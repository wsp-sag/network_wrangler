"""Functions for reading and writing roadway networks."""

from __future__ import annotations

from pathlib import Path
from typing import Union, TYPE_CHECKING, Optional

import pandas as pd

from geopandas import GeoDataFrame

from ..logger import WranglerLogger

from ..params import LAT_LON_CRS

from ..utils.io import read_table
from .nodes.io import read_nodes, write_nodes
from .links.io import read_links, write_links
from .shapes.io import read_shapes, write_shapes


if TYPE_CHECKING:
    from .network import RoadwayNetwork
    from .model_roadway import ModelRoadwayNetwork
    from ..params import LinksParams, NodesParams, ShapesParams
    from ..models._base.types import GeoFileTypes


def load_roadway(
    links_file: Union[Path, str],
    nodes_file: Union[Path, str],
    shapes_file: Union[Path, str] = None,
    links_params: Optional[LinksParams] = None,
    nodes_params: Optional[NodesParams] = None,
    shapes_params: Optional[ShapesParams] = None,
    crs: int = LAT_LON_CRS,
    read_in_shapes: bool = False,
    boundary_gdf: Optional[GeoDataFrame] = None,
    boundary_geocode: Optional[str] = None,
    boundary_file: Optional[Path] = None,
) -> RoadwayNetwork:
    """Reads a network from the roadway network standard.

    Validates that it conforms to the schema.

    Args:
        links_file: full path to the link file
        nodes_file: full path to the node file
        shapes_file: full path to the shape file. NOTE if not found, it will defaul to None and not
            raise an error.
        links_params: LinkParams instance to use. Will default to default
            values for LinkParams
        nodes_params: NodeParames instance to use. Will default to default
            values for NodeParams
        shapes_params: ShapeParames instance to use. Will default to default
            values for ShapeParams
        crs: coordinate reference system. Defaults to LAT_LON_CRS which defaults to 4326
            which is WGS84 lat/long.
        read_in_shapes: if True, will read shapes into network instead of only lazily
            reading them when they are called. Defaults to False.
        boundary_gdf: GeoDataFrame to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_geocode: Geocode to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_file: File to load as a boundary to filter the input data to. Only used for
            geographic data. Defaults to None.

    Returns: a RoadwayNetwork instance
    """
    from .network import RoadwayNetwork

    nodes_file = Path(nodes_file)
    links_file = Path(links_file)
    shapes_file = Path(shapes_file) if shapes_file else None
    if read_in_shapes and shapes_file is not None and shapes_file.exists():
        shapes_df = read_shapes(
            shapes_file,
            in_crs=crs,
            shapes_params=shapes_params,
            boundary_gdf=boundary_gdf,
            boundary_geocode=boundary_geocode,
            boundary_file=boundary_file,
        )
    else:
        shapes_df = None
    nodes_df = read_nodes(
        nodes_file,
        in_crs=crs,
        nodes_params=nodes_params,
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
    )
    filter_links_to_nodes = False
    if any([boundary_file, boundary_geocode, boundary_gdf is not None]):
        filter_links_to_nodes = True
    links_df = read_links(
        links_file,
        in_crs=crs,
        links_params=links_params,
        nodes_df=nodes_df,
        filter_to_nodes=filter_links_to_nodes,
    )

    roadway_network = RoadwayNetwork(
        links_df=links_df,
        nodes_df=nodes_df,
        shapes_df=shapes_df,
    )
    if shapes_file and shapes_file.exists():
        roadway_network._shapes_file = shapes_file
    roadway_network._links_file = links_file
    roadway_network._nodes_file = nodes_file

    return roadway_network


def id_roadway_file_paths_in_dir(
    dir: Union[Path, str], suffix: GeoFileTypes = "geojson"
) -> tuple[Path, Path, Path]:
    """Identifies the paths to the links, nodes, and shapes files in a directory."""
    network_path = Path(dir)
    if not network_path.is_dir():
        raise FileNotFoundError(f"Directory {network_path} does not exist")

    _link_suffix = suffix
    if suffix == "geojson":
        _link_suffix = "json"

    try:
        links_file = next(network_path.glob(f"*link*.{_link_suffix}"))
    except StopIteration:
        raise FileNotFoundError(
            f"No links file with {_link_suffix} suffix found in {network_path}"
        )

    try:
        nodes_file = next(network_path.glob(f"*node*.{suffix}"))
    except StopIteration:
        raise FileNotFoundError(f"No nodes file with {suffix} suffix found in {network_path}")

    try:
        shapes_file = next(network_path.glob(f"*shape*.{suffix}"))
    except StopIteration:
        # Shape file is optional so if not found, its ok.
        shapes_file = None

    return links_file, nodes_file, shapes_file


def load_roadway_from_dir(
    dir: Union[Path, str],
    suffix: GeoFileTypes = "geojson",
    read_in_shapes: bool = False,
    boundary_gdf: Optional[GeoDataFrame] = None,
    boundary_geocode: Optional[str] = None,
    boundary_file: Optional[Path] = None,
) -> RoadwayNetwork:
    """Reads a network from the roadway network standard.

    Validates that it conforms to the schema.

    Args:
        dir: the directory where the network files are located
        suffix: the suffix of the files. Defaults to "geojson"
        read_in_shapes: if True, will read shapes into network instead of only lazily
            reading them when they are called. Defaults to False.
        boundary_gdf: GeoDataFrame to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_geocode: Geocode to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_file: File to load as a boundary to filter the input data to. Only used for
            geographic data. Defaults to None.

    Returns: a RoadwayNetwork instance
    """
    links_file, nodes_file, shapes_file = id_roadway_file_paths_in_dir(dir, suffix)

    return load_roadway(
        links_file=links_file,
        nodes_file=nodes_file,
        shapes_file=shapes_file,
        read_in_shapes=read_in_shapes,
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
    )


def write_roadway(
    net: Union[RoadwayNetwork, ModelRoadwayNetwork],
    out_dir: Union[Path, str] = ".",
    prefix: str = "",
    file_format: GeoFileTypes = "geojson",
    overwrite: bool = True,
    true_shape: bool = False,
) -> None:
    """Writes a network in the roadway network standard.

    Args:
        net: RoadwayNetwork or ModelRoadwayNetwork instance to write out
        out_dir: the path were the output will be saved
        prefix: the name prefix of the roadway files that will be generated
        file_format: the format of the output files. Defaults to "geojson"
        overwrite: if True, will overwrite the files if they already exist. Defaults to True
        true_shape: if True, will write the true shape of the links as found from shapes.
            Defaults to False
    """
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        if out_dir.parent.is_dir():
            out_dir.mkdir()
        else:
            raise FileNotFoundError(
                f"Output directory {out_dir} ands its parent path does not exist"
            )

    prefix = f"{prefix}_" if prefix else ""

    links_df = net.links_df
    if true_shape:
        links_df = links_df.true_shape(net.shapes_df)

    write_links(
        net.links_df,
        out_dir=out_dir,
        prefix=prefix,
        file_format=file_format,
        overwrite=overwrite,
        include_geometry=true_shape,
    )
    write_nodes(net.nodes_df, out_dir, prefix, file_format, overwrite)

    if not true_shape and not net.shapes_df.empty:
        write_shapes(net.shapes_df, out_dir, prefix, file_format, overwrite)


def convert_roadway_file_serialization(
    in_path: Union[str, Path],
    in_format: GeoFileTypes = "geojson",
    out_dir: Union[str, Path] = ".",
    out_format: GeoFileTypes = "parquet",
    out_prefix: str = "",
    overwrite: bool = True,
    boundary_gdf: Optional[GeoDataFrame] = None,
    boundary_geocode: Optional[str] = None,
    boundary_file: Optional[Path] = None,
    chunk_size: Optional[int] = None,
):
    """Converts a files in a roadway from one serialization format to another without parsing.

    Does not do any validation.

    Args:
        in_path: the path to the input directory.
        in_format: the suffix of the input files. Defaults to "geojson".
        out_dir: the path were the output will be saved.
        out_format: the format of the output files. Defaults to "parquet".
        out_prefix: the name prefix of the roadway files that will be generated. Defaults to "".
        overwrite: if True, will overwrite the files if they already exist. Defaults to True.
        boundary_gdf: GeoDataFrame to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_geocode: Geocode to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_file: File to load as a boundary to filter the input data to. Only used for
            geographic data. Defaults to None.
        chunk_size: Size of chunk to process if want to force chunking. Defaults to None.
            Chunking will only apply to converting from json to parquet files.
    """
    links_in_file, nodes_in_file, shapes_in_file = id_roadway_file_paths_in_dir(in_path, in_format)
    from ..utils.io import convert_file_serialization

    nodes_out_file = Path(out_dir / f"{out_prefix}_nodes.{out_format}")
    convert_file_serialization(
        nodes_in_file,
        nodes_out_file,
        overwrite=overwrite,
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
        chunk_size=chunk_size,
    )

    if any([boundary_file, boundary_geocode, boundary_gdf is not None]):
        node_filter_s = read_table(nodes_out_file).model_node_id
    else:
        node_filter_s = None

    links_out_file = Path(out_dir / f"{out_prefix}_links.{out_format}")
    if out_format == "geojson":
        links_out_file = links_out_file.with_suffix(".json")

    convert_file_serialization(
        links_in_file,
        links_out_file,
        overwrite=overwrite,
        node_filter_s=node_filter_s,
        chunk_size=chunk_size,
    )

    if shapes_in_file:
        shapes_out_file = Path(out_dir / f"{out_prefix}_shapes.{out_format}")
        convert_file_serialization(
            shapes_in_file,
            shapes_out_file,
            overwrite=overwrite,
            boundary_gdf=boundary_gdf,
            boundary_geocode=boundary_geocode,
            boundary_file=boundary_file,
            chunk_size=chunk_size,
        )


def convert_roadway_network_serialization(
    input_path: Union[str, Path],
    output_format: GeoFileTypes = "geojson",
    out_dir: Union[str, Path] = ".",
    input_suffix: GeoFileTypes = "geojson",
    out_prefix: str = "",
    overwrite: bool = True,
    boundary_gdf: Optional[GeoDataFrame] = None,
    boundary_geocode: Optional[str] = None,
    boundary_file: Optional[Path] = None,
    filter_links_to_nodes: bool = False,
):
    """Converts a roadway network from one serialization format to another with parsing.

    Performs validation and parsing.

    Args:
        input_path: the path to the input directory.
        output_format: the format of the output files. Defaults to "geojson".
        out_dir: the path were the output will be saved.
        input_suffix: the suffix of the input files. Defaults to "geojson".
        out_prefix: the name prefix of the roadway files that will be generated. Defaults to "".
        overwrite: if True, will overwrite the files if they already exist. Defaults to True.
        boundary_gdf: GeoDataFrame to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_geocode: Geocode to filter the input data to. Only used for geographic data.
            Defaults to None.
        boundary_file: File to load as a boundary to filter the input data to. Only used for
            geographic data. Defaults to None.
        filter_links_to_nodes: if True, will filter the links to only those that have nodes.
            Defaults to False unless boundary_gdf, boundary_geocode, or boundary_file are provided.
    """
    if input_suffix is None:
        input_suffix = "geojson"
    WranglerLogger.info(f"Loading roadway network from {input_path} with suffix {input_suffix}")
    net = load_roadway_from_dir(
        input_path,
        suffix=input_suffix,
        boundary_gdf=boundary_gdf,
        boundary_geocode=boundary_geocode,
        boundary_file=boundary_file,
        filter_links_to_nodes=filter_links_to_nodes,
    )
    WranglerLogger.info(f"Writing roadway network to {out_dir} in {output_format} format.")
    write_roadway(
        net,
        prefix=out_prefix,
        out_dir=out_dir,
        file_format=output_format,
        overwrite=overwrite,
    )
