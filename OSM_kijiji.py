# -*- coding: utf-8 -*-
"""
Created on Sat Feb 16 10:42:08 2019

@author: julien
"""

from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass
import pandas as pd
import math





def get_OSM():
    nominatim = Nominatim()
    overpass = Overpass()
    areaId = nominatim.query('Montreal, Canada').areaId()
    query = overpassQueryBuilder(area=areaId, elementType='node', selector=['shop'])+overpassQueryBuilder(area=areaId, elementType='node', selector='amenity')
    result = overpass.query(query)
    db = db_from_OSM(result)
    return db



def db_from_OSM(osm_result):
    
    row = {}
    rows = []
    for result in osm_result.elements():
        row={}
        row['id'] = result.id()
        row['name'] = result.tag('name')
        if result.tag('shop') is not None:          
            row['type'] = result.tag('shop')
            row['category'] = 'shop'
        else:
            row['type'] = result.tag('amenity')
            row['category'] = 'amenity'
        row['lat'] = result.lat()
        row['long'] = result.lon()
        rows.append(row)
    db = pd.DataFrame(rows)
    return db
        


def calc_gcd(lat_1,long_1,lat_2,long_2):
    earth_radius = 6371
    lat_1 = lat_1 * math.pi / 180
    lat_2 = lat_2 * math.pi / 180
    long_1 = long_1 * math.pi / 180
    long_2 = long_2 * math.pi / 180
    delta_angle = math.acos(math.sin(lat_1)*math.sin(lat_2)+math.cos(lat_1)*math.cos(lat_2)*math.cos(abs(long_1-long_2)))
    distance_km = delta_angle * earth_radius
    return distance_km

def select_in_range(lat_kj, long_kj,df):
    distance = 0.005
    lat_OSM = [lat_kj - distance,lat_kj + distance]
    long_OSM = [long_kj - distance,long_kj + distance]
    
    db_select = df.loc[(df['lat'] >= lat_OSM[0]) & (df['lat'] <= lat_OSM[1]) \
                       & (df['long'] >= long_OSM[0]) & (df['long'] <= long_OSM[1])]
    for i,row in db_select.iterrows():
        lat_line = row['lat']
        long_line = row['long']
        db_select.loc[i,'distance'] = calc_gcd(lat_kj,long_kj,lat_line,long_line)
    return db_select

