
import pandas as pd
def point_df_to_geojson(df: pd.DataFrame, properties: list):
    '''
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    '''
    from .RoadwayNetwork import RoadwayNetwork
    geojson = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'Point',
                               'coordinates':[]}}
        feature['geometry']['coordinates'] = [row['geometry'].y,row['geometry'].x]
        feature['properties'][RoadwayNetwork.NODE_FOREIGN_KEY] = row.name
        for prop in properties:
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)
    return geojson

def link_df_to_json(df: pd.DataFrame, properties: list):
    '''
    Modified from: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    '''

    json = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {}
        for prop in properties:
            feature[prop] = row[prop]
        json['features'].append(feature)
    return json


def topological_sort(adjacency_list, visited_list):
    '''
    Topological sorting for Acyclic Directed Graph
    '''

    output_stack = []

    def topology_sort_util(vertex):
        if not visited_list[vertex]:
            visited_list[vertex] = True
            for neighbor in adjacency_list[vertex]:
                topology_sort_util(neighbor)
            output_stack.insert(0, vertex)

    for vertex in visited_list:
        topology_sort_util(vertex)

    return output_stack
