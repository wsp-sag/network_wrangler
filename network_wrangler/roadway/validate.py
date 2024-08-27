"""Validates a roadway network to the wrangler data model specifications."""
from pathlib import Path
from typing import Optional

from network_wrangler import WranglerLogger
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.roadway.io import id_roadway_file_paths_in_dir
from network_wrangler.utils.io import read_table
from network_wrangler.roadway.links.validate import validate_links_df
from network_wrangler.roadway.nodes.validate import validate_nodes_df
from network_wrangler.roadway.shapes.validate import validate_shapes_df


def validate_roadway_in_dir(directory: Path, suffix: str, strict: bool = False, output_dir: Path = "."):
    """Validates a roadway network in a directory to the wrangler data model specifications.

    Args:
        directory (str): The roadway network file directory.
        suffix (str): The suffices of roadway network file name.
        strict (bool): If True, will validate the roadway network strictly without 
            parsing and filling in data.
        output_dir (str): The output directory for the validation report. Defaults to ".".
    """
    links_file, nodes_file, shapes_file = id_roadway_file_paths_in_dir(directory, suffix)
    validate_roadway_files(
        links_file,
        nodes_file,
        shapes_file,
        strict=strict,
        output_dir=output_dir
    )


def validate_roadway_files(
    links_file: Path,
    nodes_file: Path,
    shapes_file: Optional[Path] = None,
    strict: bool = False,
    output_dir: Path = "."
):
    """Validates the roadway network files strictly to the wrangler data model specifications.

    Args:
        links_file (str): The path to the links file.
        nodes_file (str): The path to the nodes file.
        shapes_file (str): The path to the shapes file.
        strict (bool): If True, will validate the roadway network strictly without 
            parsing and filling in data.
        output_dir (str): The output directory for the validation report. Defaults to ".".
    """
    valid = {
        "net": True,
        "links": True,
        "nodes": True
    }

    nodes_df = read_table(nodes_file)
    valid["links"] = validate_nodes_df(
        nodes_df,
        strict=strict,
        output_file=Path(output_dir) / "node_errors.csv"
    )

    links_df = read_table(links_file)
    valid["links"] = validate_links_df(
        links_df,
        nodes_df=nodes_df,
        strict=strict,
        output_file=Path(output_dir) / "link_errors.csv"
    )

    if shapes_file:
        valid["shapes"] = True
        shapes_df = read_table(shapes_file)
        valid["shapes"] = validate_shapes_df(
            shapes_df,
            strict=strict,
            output_file=Path(output_dir) / "shape_errors.csv"
        )

    try:
        net = RoadwayNetwork(links_df=links_df, nodes_df=nodes_df, shapes_df=shapes_df)
    except Exception as e:
        WranglerLogger.error(f"!!! [Network invalid] - Failed Loading to object\n{e}")
        valid["net"] = False
