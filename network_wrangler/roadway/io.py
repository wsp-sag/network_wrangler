from pathlib import Path
from typing import Union

from .nodes import NodesParams, read_nodes, write_nodes
from .links import LinksParams, read_links, write_links
from .shapes import ShapesParams, ShapesSchema, read_shapes, write_shapes
from ..roadwaynetwork import RoadwayNetwork
from ..utils.models import empty_df

DEFAULT_CRS = 4326


def load_roadway(
    links_file: Union[Path, str],
    nodes_file: Union[Path, str],
    shapes_file: Union[Path, str] = None,
    links_params: LinksParams = None,
    nodes_params: NodesParams = None,
    shapes_params: ShapesParams = None,
    crs: int = DEFAULT_CRS,
    read_in_shapes: bool = False,
) -> "RoadwayNetwork":
    """
    Reads a network from the roadway network standard
    Validates that it conforms to the schema

    args:
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
        crs: coordinate reference system. Defaults to DEFAULT_CRS which defaults to 4326
            which is WGS84 lat/long.
        read_in_shapes: if True, will read shapes into network instead of only lazily
            reading them when they are called. Defaults to False.

    Returns: a RoadwayNetwork instance
    """
    nodes_df = read_nodes(nodes_file, crs=crs, nodes_params=nodes_params)
    links_df = read_links(
        links_file, crs=crs, links_params=links_params, nodes_df=nodes_df
    )

    shapes_df = empty_df(ShapesSchema)
    if read_in_shapes:
        shapes_df = read_shapes(shapes_file, crs=crs, shapes_params=shapes_params)

    roadway_network = RoadwayNetwork(
        links_df,
        nodes_df,
        shapes_df=shapes_df,
        shapes_file=shapes_file,
    )

    roadway_network._links_file = links_file
    roadway_network._nodes_file = nodes_file

    return roadway_network


def load_roadway_from_dir(dir: Union[Path, str], suffix="geojson") -> "RoadwayNetwork":
    """
    Reads a network from the roadway network standard
    Validates that it conforms to the schema

    args:
        dir: the directory where the network files are located

    Returns: a RoadwayNetwork instance
    """
    network_path = Path(dir)
    if not network_path.is_dir():
        raise FileNotFoundError(f"Directory {network_path} does not exist")

    _link_suffix = suffix
    if suffix == "geojson":
        _link_suffix = "json"

    try:
        links_file = next(network_path.glob(f"*link.{_link_suffix}"))
    except StopIteration:
        raise FileNotFoundError(
            f"No links file with {_link_suffix} suffix found in {network_path}"
        )

    try:
        nodes_file = next(network_path.glob(f"*node.{suffix}"))
    except StopIteration:
        raise FileNotFoundError(
            f"No nodes file with {suffix} suffix found in {network_path}"
        )

    try:
        shapes_file = next(network_path.glob(f"*shape.{suffix}"))
    except StopIteration:
        # Shape file is optional so if not found, its ok.
        shapes_file = None

    return load_roadway(links_file, nodes_file, shapes_file)


def write_roadway(
    net,
    out_dir: Union[Path, str] = ".",
    prefix: str = "",
    format: str = "geojson",
    overwrite: bool = True,
    true_shape: bool = False,
) -> None:
    """
    Writes a network in the roadway network standard

    args:
        out_dir: the path were the output will be saved
        prefix: the name prefix of the roadway files that will be generated
        format: the format of the output files. Defaults to "geojson"
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
        format=format,
        overwrite=overwrite,
        include_geometry=true_shape,
    )
    write_nodes(net.nodes_df, out_dir, prefix, format, overwrite)

    if not true_shape:
        write_shapes(net.shapes_df, out_dir, prefix, format, overwrite)
