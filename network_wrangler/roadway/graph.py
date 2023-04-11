import copy

from typing import Collection

import networkx as nx
import osmnx as ox

from pandas import DataFrame
from geopandas import GeoDataFrame
from ..logger import WranglerLogger


"""
(str): default column to use as weights in the shortest path calculations.
"""
SP_WEIGHT_COL = "i"

"""
Union(int, float)): default penalty assigned for each
    degree of distance between a link and a link with the searched-for
    name when searching for paths between A and B node
"""
SP_WEIGHT_FACTOR = 100

"""
(int): default for initial number of links from name-based
    selection that are traveresed before trying another shortest
    path when searching for paths between A and B node
"""
SEARCH_BREADTH = 5

"""
(int): default for maximum number of links traversed between
    links that match the searched name when searching for paths
    between A and B node
"""
MAX_SEARCH_BREADTH = 10


ox_major_version = int(ox.__version__.split(".")[0])


def _drop_complex_df_columns(df: DataFrame) -> DataFrame:
    "Returns dataframe without columns with lists, tuples or dictionaries types."
    # column types which could have complex types (i.e. lists, dicts, etc)
    _drop_cols = list(
        df.applymap(
            lambda x: isinstance(x, list).any()
            or isinstance(x, dict).any()
            or isinstance(x, tuple).any()
        )
    )

    if _drop_cols:
        WranglerLogger.debug(
            f"Dropping following columns from df becasue found complex \
            types which osmnx wouldn't like: {_drop_cols}"
        )
        df = df.drop(_drop_cols, axis=1)
    return df


def _nodes_to_graph_nodes(nodes_df: GeoDataFrame) -> GeoDataFrame:
    """Transformes RoadwayNetwork nodes_df into format osmnx is expecting.

    OSMNX is expecting:
    - columns: id, x, y
    - gdf attribute:  gdf_name
    - property values which are simple int or strings

    Args:
        nodes_df (GeoDataFrame): nodes geodataframe from RoadwayNetwork instance and index of
            the UNIQUE_NODE_KEY
    """

    graph_nodes_df = copy.deepcopy(nodes_df)
    graph_nodes_df.gdf_name = "network_nodes"

    # drop column types which could have complex types (i.e. lists, dicts, etc)
    graph_nodes_df = _drop_complex_df_columns(graph_nodes_df)

    # OSMNX is expecting id, x, y
    graph_nodes_df["id"] = graph_nodes_df.index
    graph_nodes_df = graph_nodes_df.rename({"X": "x", "Y": "y"})

    return graph_nodes_df


def _links_to_graph_links(
    links_df: GeoDataFrame,
    link_foreign_key_to_node: Collection,
    sp_weight_col: str = SP_WEIGHT_COL,
    sp_weight_factor: float = SP_WEIGHT_FACTOR,
) -> GeoDataFrame:
    """Transformes RoadwayNetwork links_df into format osmnx is expecting.

    OSMNX is expecting:


    Args:
        links_df (GeoDataFrame): links geodataframe from RoadwayNetwork instance
    """
    graph_links_df = copy.deepcopy(links_df)

    # drop column types which could have complex types (i.e. lists, dicts, etc)
    graph_links_df = _drop_complex_df_columns(graph_links_df)

    # have to add in weights to use for shortest paths before doing the conversion to a graph
    if sp_weight_col not in graph_links_df.columns:
        WranglerLogger.warning(
            f"{sp_weight_col} not in graph_links_df so adding and initializing to 1."
        )
        graph_links_df[sp_weight_col] = 1

    graph_links_df["weight"] = graph_links_df[sp_weight_col] * sp_weight_factor

    # have to change this over into u,v b/c this is what osm-nx is expecting
    graph_links_df = graph_links_df.rename(
        {link_foreign_key_to_node[0]: "u", link_foreign_key_to_node[1]: "v"}
    )

    graph_links_df["key"] = graph_links_df.index

    # Per osmnx u,v,key should be a multi-index;
    #     https://osmnx.readthedocs.io/en/stable/osmnx.html#osmnx.utils_graph.graph_from_gdfs
    # However - if the index is set before hand in osmnx version <1.0 then it fails
    #     on the set_index line *within* osmnx.utils_graph.graph_from_gdfs():
    #           `for (u, v, k), row in gdf_edges.set_index(["u", "v", "key"]).iterrows()
    if ox_major_version >= 1:
        graph_links_df = graph_links_df.set_index(keys=["u", "v", "key"], drop=True)

    return graph_links_df


def links_nodes_to_ox_graph(
    links_df: GeoDataFrame,
    nodes_df: GeoDataFrame,
    link_foreign_key_to_node=("A", "B"),
    sp_weight_col: str = SP_WEIGHT_COL,
    sp_weight_factor: float = SP_WEIGHT_FACTOR,
):
    """
    create an osmnx-flavored network graph from nodes and links dfs

    osmnx doesn't like values that are arrays, so remove the variables
    that have arrays.  osmnx also requires that certain variables
    be filled in, so do that too.

    Args:
        links_df: links_df from RoadwayNetwork
        nodes_df: nodes_df from RoadwayNetwork
        link_foreign_key_to_node: Tuple specifying link properties with A and B node foreign keys.
            Defaults to (A,B)
        sp_weight_col: column to use for weights. Defaults to "i".
        sp_weight_factor: multiple to apply to the weights. Defaults to SP_WEIGHT_FACTOR.

    Returns: a networkx multidigraph
    """
    WranglerLogger.debug("starting ox_graph()")
    graph_nodes_df = _nodes_to_graph_nodes(nodes_df)
    graph_links_df = _links_to_graph_links(
        links_df,
        link_foreign_key_to_node=link_foreign_key_to_node,
        sp_weight_col=sp_weight_col,
        sp_weight_factor=sp_weight_factor,
    )

    try:
        WranglerLogger.debug("starting ox.gdfs_to_graph()")
        G = ox.graph_from_gdfs(graph_nodes_df, graph_links_df)

    except AttributeError as attr_error:
        if attr_error.args[0] == "module 'osmnx' has no attribute 'graph_from_gdfs'":
            # This is the only exception for which we have a workaround
            # Does this still work given the u,v,key multi-indexing?
            #
            WranglerLogger.warn(
                "Please upgrade your OSMNX package. For now, using deprecated\
                        osmnx.gdfs_to_graph because osmnx.graph_from_gdfs not found"
            )
            G = ox.gdfs_to_graph(graph_nodes_df, graph_links_df)
        else:
            # for other AttributeErrors, raise further
            raise attr_error
    except Exception as e:
        raise e

    WranglerLogger.debug("Created osmnx graph from RoadwayNetwork")
    return G


def shortest_path(
    self, G: ox.MultiDiGraph, O_id, D_id, sp_weight_property="weight"
) -> tuple:
    """

    Args:
        G: osmnx MultiDiGraph, created using links_nodes_to_ox_graph
        O_id: foreign key for start node
        D_id: foreign key for end node
        sp_weight_property: link property to use as weight in finding shortest path.
            Defaults to "weight".

    Returns: tuple with length of four
    - Boolean if shortest path found
    - nx Directed graph of graph links
    - route of shortest path nodes as List
    - links in shortest path selected from links_df
    """

    try:
        sp_route = nx.shortest_path(G, O_id, D_id, weight=sp_weight_property)
        WranglerLogger.debug("Shortest path successfully routed")
    except nx.NetworkXNoPath:
        WranglerLogger.debug("No SP from {} to {} Found.".format(O_id, D_id))
        return False
    except Exception as e:
        raise e

    return sp_route
