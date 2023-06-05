import hashlib

from typing import List, Union

import pandas as pd

from shapely import LineString


def compare_networks(
    nets: List[Union["RoadwayNetwork", "ModelRoadwayNetwork"]],
    names: List[str] = None,
) -> pd.DataFrame:
    if names is None:
        names = ["net" + str(i) for i in range(1, len(nets) + 1)]
    df = pd.DataFrame({name: net.summary for name, net in zip(names, nets)})
    return df


def compare_links(
    links: List[pd.DataFrame],
    names: List[str] = None,
) -> pd.DataFrame:
    if names is None:
        names = ["links" + str(i) for i in range(1, len(links) + 1)]
    df = pd.DataFrame({name: link.of_type.summary for name, link in zip(names, links)})
    return df


def create_unique_shape_id(line_string: LineString):
    """
    Creates a unique hash id using the coordinates of the geometry using first and last locations.

    Args:
    line_string: Line Geometry as a LineString

    Returns: string
    """

    x1, y1 = list(line_string.coords)[0]  # first coordinate (A node)
    x2, y2 = list(line_string.coords)[-1]  # last coordinate (B node)

    message = "Geometry {} {} {} {}".format(x1, y1, x2, y2)
    unhashed = message.encode("utf-8")
    hash = hashlib.md5(unhashed).hexdigest()

    return hash
