import pytest

from network_wrangler import WranglerLogger


def test_dependencies_api(request):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    import pandera
    import pydantic
    from folium import Circle, Icon, Marker
    from geopandas.geodataframe import GeoDataFrame
    from networkx import (
        DiGraph,
        MultiDiGraph,
        NetworkXNoPath,
        is_strongly_connected,
        shortest_path,
        strongly_connected_components,
    )
    from numpy import arange, float64, int64
    from osmnx import graph_from_gdfs, plot_graph, plot_graph_folium
    from osmnx.folium import _make_folium_polyline
    from pandas.core.frame import DataFrame
    from shapely.geometry import LineString, Point
    WranglerLogger.info(f"--Finished: {request.node.name}")