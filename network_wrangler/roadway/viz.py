import randint

import folium
import osmnx as ox

from .logger import WranglerLogger
from .graph import links_nodes_to_ox_graph


def selection_map(
    selection: "Union[RoadwayNodeSelection,RoadwayLinkSelection]",
) -> folium.map:
    """
    Shows which links are selected based on a selection instance.

    Args:
        selection: RoadwayNodeSelection or RoadwayLink seleciton instance
    """
    WranglerLogger.debug("Creating selection map.")
    if selection.selection_type == "segment":
        G = selection.segment.graph
    else:
        G = links_nodes_to_ox_graph(
            selection.selected_links_df, selection.selected_nodes_df
        )

    # base map plot with whole graph
    m = ox.plot_graph_folium(
        G, edge_color=None, tiles="cartodbpositron", width="300px", height="250px"
    )

    # plot selection
    for _, row in selection.selected_links_df.iterrows():
        pl = ox.folium._make_folium_polyline(
            geom=row["geometry"],
            edge=row,
            edge_color="blue",
            edge_width=5,
            edge_opacity=0.8,
        )
        pl.add_to(m)

    # if have A and B node add them to base map
    def _folium_node(node_row, color="white", icon=""):
        node_marker = folium.Marker(
            location=[node_row["Y"], node_row["X"]],
            icon=folium.Icon(icon=icon, color=color),
        )
        return node_marker

    if selection.selection_type == "segment":
        _folium_node(
            selection.selected_nodes_df.loc[selection.segment.O_pk],
            color="green",
            icon="play",
        ).add_to(m)

        _folium_node(
            selection.selected_nodes_df.loc[selection.segment.D_pk],
            color="red",
            icon="star",
        ).add_to(m)

    return m


def network_connection_plot(G, disconnected_subgraph_nodes: list):
    """Plot a graph to check for network connection.

    Args:
        G: OSMNX flavored networkX graph.
        disconnected_subgraph_nodes: List of disconnected subgraphs described by the list
            of their member nodes (as described by their `model_node_id`).

    returns: fig, ax : tuple
    """
    WranglerLogger.debug("Creating network connection plot.")
    colors = []
    for i in range(len(disconnected_subgraph_nodes)):
        colors.append("#%06X" % randint(0, 0xFFFFFF))

    fig, ax = ox.plot_graph(
        G,
        figsize=(16, 16),
        show=False,
        close=True,
        edge_color="black",
        edge_alpha=0.1,
        node_color="black",
        node_alpha=0.5,
        node_size=10,
    )
    i = 0
    for nodes in disconnected_subgraph_nodes:
        for n in nodes:
            size = 100
            ax.scatter(G.nodes[n]["X"], G.nodes[n]["Y"], c=colors[i], s=size)
        i = i + 1

    return fig, ax
