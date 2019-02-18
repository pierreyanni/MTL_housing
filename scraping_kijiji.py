#!/usr/bin/env python
# coding: utf-8


import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
import os
import OSM_kijiji as osm

# open main dataframe
path = os.getcwd() # only absolute paths on spyder!
df = pd.read_csv(path + r'/data/housing.csv', index_col=0) # 
db_osm = osm.get_OSM()
# all ads collected
dict_ads = {}
last_page = 20 # last page scraped

# list of urls of pages of ads
urls = [f'https://www.kijiji.ca/b-appartement-condo/ville-de-montreal/page-{n}/c37l1700281' 
        for n in range(1,last_page + 1 )]

# loop on ads to extract web addresses for individual ads
for url in urls:
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    ads = soup.find_all('div', {'class':"search-item"})     # find ads
    for ad in ads:
        dict_ads[ad['data-ad-id']] = 'https://www.kijiji.ca' + ad['data-vip-url']

# get rid of ads that are already in database and duplicates    
dict_ads = {key: val for key, val in dict_ads.items() if key not in df.index}
df = df[~df.index.duplicated(keep='last')] # get rid of duplicates 

# scrape every page
for ad, url in dict_ads.items():
    # date and time for file name:
    date = time.asctime().replace('  ', '_').replace(' ', '_').replace(':', '')
    df.loc[ad, 'dateScraped'] = date
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    try:
        df.loc[ad,'datePosted'] = soup.find('div', class_='datePosted-319944123') \
        .find('time')['title']
    except:
            pass
    df.loc[ad,'title'] = soup.find('h1', class_='title-2323565163').get_text()
    try:
        df.loc[ad,'price'] = soup.find('span', class_="currentPrice-441857624") \
        .find('span')['content']
    except:
        pass
    try:
        df.loc[ad,'address'] = soup.find('span', class_="address-3617944557") \
        .get_text()
    except:
        pass
    df.loc[ad,'latitude'] = soup.find("meta",  property="og:latitude")['content']
    df.loc[ad,'longitude'] = soup.find("meta",  property="og:longitude")['content']
    df.loc[ad,'description'] = soup.find("meta",  property="og:description")['content']
    try:
        keys = soup.find_all('dt', class_="attributeLabel-240934283 attributeLabel__isFrench-2403035016")
        values = soup.find_all('dd', class_='attributeValue-2574930263')
        dict_att = {}
        for key, val in zip(keys, values):
            dict_att[key.text] = val.text
        df.loc[ad, 'rooms'] = dict_att['Pièces (nb)']
        df.loc[ad, 'bathrooms'] = dict_att['Salles de bain (nb)']
        df.loc[ad, 'furnished'] = dict_att['Meublé']    
        df.loc[ad, 'pets'] = dict_att['Animaux acceptés']
    except:
        pass
    try:     
        db_ad = osm.select_in_range(float(df.loc[ad,'latitude']),float(df.loc[ad,'longitude']),db_osm)
        df.loc[ad,'num_supermarket'] = db_ad.loc[(db_ad['type'] == 'supermarket')].count()['id']
        df.loc[ad,'dist_smkt'] = db_ad.loc[(db_ad['type'] == 'supermarket')].min()['distance']
    except:
        pass
df.to_csv(path + r'/data/housing.csv')    


