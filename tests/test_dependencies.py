import os
import pytest


@pytest.mark.travis
@pytest.mark.dependencies
def test_dependencies_api(request):
    print("\n--Starting:", request.node.name)
    from jsonschema import validate
    from jsonschema.exceptions import ValidationError
    from jsonschema.exceptions import SchemaError
    from folium import Icon, Circle, Marker
    from networkx import (
        shortest_path,
        NetworkXNoPath,
        is_strongly_connected,
        strongly_connected_components,
        DiGraph,
        MultiDiGraph,
    )
    from numpy import float64, int64, arange
    from osmnx import graph_from_gdfs, plot_graph, plot_graph_folium
    from osmnx.folium import _make_folium_polyline
    from geopandas.geodataframe import GeoDataFrame
    from pandas.core.frame import DataFrame
    from shapely.geometry import Point, LineString
    from partridge import load_feed
    from partridge.config import default_config
