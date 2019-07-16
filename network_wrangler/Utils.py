
import pandas as pd
def point_df_to_geojson(df: pd.DataFrame, properties: list):
    '''
    Author: Geoff Boeing:
    https://geoffboeing.com/2015/10/exporting-python-data-geojson/
    '''
    geojson = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'Point',
                               'coordinates':[]}}
        feature['geometry']['coordinates'] = [row['geometry'].y,row['geometry'].x]
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
