# -*- coding: utf-8 -*-
"""
Created on Sat Feb 16 10:42:08 2019

@author: julien
"""

from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass
import pandas as pd
import math
import numpy as np
import os
from bs4 import BeautifulSoup
import requests
import ast
from collections import OrderedDict





json_name = 'PMR_db_records.json'
list_tags = ["highway","lanes","oneway","surface","cycleway:left","cycleway:right"\
             ,"sidewalk","cycleway"",bridge","amenity","building","building:levels"]

#,"lanes","oneway","surface","cycleway:left","cycleway:right"\
#             ,"sidewalk","cycleway"",bridge","amenity","building","building:levels"

def get_OSM():
    nominatim = Nominatim()
    overpass = Overpass()
    areaId = nominatim.query('Le Plateau Mont Royal').areaId()
    query = overpassQueryBuilder(area=areaId, elementType=['way'])
    result = overpass.query(query)
    db = db_from_OSM(result)
    return db


def load_json():
    data = pd.read_json('.\data\{}'.format(json_name))
    return data

def save_json(osm_db):
    osm_db.to_json('.\data\{}'.format(json_name),orient='records')


def db_from_OSM(osm_result):
    
    row = {}
    rows = []
    for result in osm_result.elements():
        ways_nodes=[]
        row={}
        row['id'] = result.id()
        row['type'] = result.type()
        row['lat'] = result.lat()
        row['long'] = result.lon()
        row['tags'] = result.tags()
        if result.type() == 'way':
            nodes = result.nodes()
            for node in nodes:
                name_node = node.id()
                ways_nodes.append(name_node)
            row['way_nodes'] = ways_nodes
        else:
            row['elevation'] = get_elevation(result.lat(),result.lon())
        rows.append(row)
    db = pd.DataFrame(rows)
    return db


def find_intersections(osm_db):
    nodes = []
    count_ways = []
    intersection=[]
    for i in range(osm_db.shape[0]):     
        if osm_db.iloc[i]['type'] == 'way':
            nodes.append(osm_db.iloc[i]['way_nodes'])
    u_nodes = [x for l in nodes for x in l]
    unique_nodes = list(OrderedDict.fromkeys(u_nodes))
    for i in range(len(unique_nodes)):
        count = 0
        for j in range(len(nodes)):
            count += nodes[j].count(unique_nodes[i])
        count_ways.append(count)
    intersection = list(zip(unique_nodes,count_ways))
    data = pd.DataFrame(intersection)
    data.columns = ['id','num_ways']
    osm_db = osm_db.merge(data, on='id',how='left')
    
    return osm_db
      
        
def list_buildings(osm_db):
    row = {}
    rows = []
    for i in range(osm_db.shape[0]):
        row={}
        row['id'] = osm_db.iloc[i]['id']
        if osm_db.iloc[i]['tags'] != None:
            if 'building' in osm_db.iloc[i]['tags'].keys():
                row['building_bool'] = 1
                try:
                    row['building_type'] = osm_db.iloc[i]['tags']['building']
                except:
                    pass
                try:
                    row['num_levels'] = osm_db.iloc[i]['tags']['building:levels']
                except:
                    pass
            else :
                row['building_bool'] = 0
        else:
            row['building_bool'] = 0    
        rows.append(row)
    data = pd.DataFrame(rows)
    osm_db = osm_db.merge(data, on='id',how='left')
    return osm_db        
                         
    
    
#def add_tags_columns(osm_db):
##    for tag in list_tags:
##        osm_db[tag]= np.nan
#    for tag in list_tags:
#        Newcol = []
#        for i,row in osm_db.iterrows():
#            if row['tags'] != None:
#                if tag in row['tags'].keys():
#                    value = row['tags'].get(tag)
#                else:
#                    value = None
#            else:
#                value = None                                   
#            Newcol.append(value)
#        osm_db = osm_db.assign('tag_{tag}' = Newcol)                    
#    return osm_db

def add_tags_columns(osm_db,tags_allowed):
    row_dd = {}
    rows = []  
    for i in range(osm_db.shape[0]):
        row_dd = {}
        row = osm_db.iloc[i]
        for tag in tags_allowed:
            if row['tags'] != None:
                if tag in row['tags']:
                    row_dd[tag] = row['tags'][tag]
                else:
                    row_dd[tag] = None                                           
            else:
                row_dd[tag] = None                                           
        rows.append(row_dd)
    db = pd.DataFrame(rows)
    osm_db = pd.concat([osm_db,db],axis=1)
    return osm_db
                    
        



def add_elevation(osm_db):
    Newcol = []
    for i,row in osm_db.iterrows():
        if row['type']=='way':
            elevation = None
        else:
            elevation = get_elevation(row['lat'],row['long'])
        Newcol.append(elevation)
    osm_db = osm_db.assign(elevation = Newcol)
    return osm_db

def get_elevation(node_lat,node_lon): 
    url = 'http://elevationapi-env.tgu5kjprmq.us-east-1.elasticbeanstalk.com/api/v1/lookup?locations={},{}'.format(node_lat,node_lon)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    soup = ast.literal_eval(soup.text)
    elevation = soup['results'][0]['elevation']
    return elevation

def db_ways(osm_db):
    list_nodes = []
    unique_nodes = []
    db_ways = osm_db.loc[osm_db['type']=='way'].copy()
    for i,way in db_ways.iterrows():
        list_nodes += way['way_nodes']
    [unique_nodes.append(x) for x in list_nodes if x not in unique_nodes]
    db_nodes = osm_db.loc[osm_db['id'].isin(unique_nodes)].copy()
    return db_ways, db_nodes
            
def node_ways(osm_db):
    db_way, db_nodes = db_ways(osm_db)
    for i,node in db_nodes.iterrows():
        list_ways=[]
        for j,way in db_way.iterrows():
            way_nodes = db_way.loc[j,'way_nodes']
            if db_nodes.loc[i,'id'] in way_nodes:
                list_ways.append(db_way.loc[j,'id'])
        db_nodes.at[i,'way_nodes'] = list_ways
        return db_way, db_nodes

def node_way(osm_db):
    db_way, db_nodes = db_ways(osm_db)
    
    for i,row_i in db_nodes.iterrows():
        list_ways=[]
        for j,row_j in db_way.iterrows():
            way_nodes = []
            way_nodes = row_j['way_nodes']
            if int(row_i['id']) in way_nodes:
                list_ways.append(int(row_j['id']))
        row_i['way_nodes'] = list_ways
        return db_way, db_nodes    


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

def list_tags(osm_db):
    table = pd.pivot_table(osm_db, values='lat', index=['category', 'type'],\
                           aggfunc=np.sum,fill_value=1)
    table = list(table.index.values)
    table = pd.DataFrame(table,columns = ['category','type'])
    return table

def add_osm_data(db_osm):
    path = os.getcwd() # only absolute paths on spyder!
    df = pd.read_csv(path + r'/data/housing.csv', index_col=0)
    tags_selection = ['supermarket','greengrocer','school','subway_station']
    for i,row in df.iterrows():           
        db_ad = select_in_range(float(row['latitude']),float(row['longitude']),db_osm)
        for tag in tags_selection:        
            if 'distance' in db_ad.loc[db_ad['type'] == tag] :                
                df.loc[i,f'num_{tag}'] = db_ad.loc[(db_ad['type'] == tag)].count()['id']
                df.loc[i,f'dist_{tag}'] = db_ad.loc[(db_ad['type'] == tag)].min()['distance']
            else:
                df.loc[i,f'num_{tag}'] = 0
                df.loc[i,f'dist_{tag}'] = None
    df.to_csv(path + r'/data/housing.csv')   
    
        
    
    

