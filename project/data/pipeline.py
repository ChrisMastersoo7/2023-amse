import os
import urllib
import shutil
from pathlib import Path
from gpx_converter import Converter
import pandas as pd
import geopandas as gpd
import unicodedata

# Tirol source data in zip format

tirol_urls = list()

tirol_single_trail_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Einzeletappen.zip'
tirol_day_trips_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Tagestouren.zip'
tirol_multi_day_trips_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Mehrtagestouren.zip'

tirol_urls.append(tirol_single_trail_url)
#tirol_urls.append(tirol_day_trips_url)
tirol_urls.append(tirol_multi_day_trips_url)

Path("./project/data/tmp/").mkdir(parents=True, exist_ok=True)

# Download all zips and extract 
for url in tirol_urls:
    with urllib.request.urlopen(url) as response, open('./project/data/tmp/{}'.format(url.rsplit('/')[-1]), 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
        shutil.unpack_archive('./project/data/tmp/{}'.format(url.rsplit('/')[-1]), './project/data/tmp/{}'.format(url.rsplit('/')[-1].rsplit('.')[0]), 'zip')

# convert data from gpx to df
dfs = list()
for root, dirs, files in os.walk('./project/data/tmp/'):
    for file in files:
        if file.endswith('.gpx'):
            # handle umlauts
            # special_char_map = {ord('ä'):'ae', ord('ü'):'ue', ord('ö'):'oe', ord('ß'):'ss'}
            # umlaut = os.path.join(root, file)
            # umlaut.translate(special_char_map)
            try:
                df = Converter(input_file=os.path.join(root, file)).gpx_to_dataframe()
                df.columns.values[0] = file.rsplit('.')[0]
                dfs.append(df)
            except UnicodeDecodeError:
                # TODO: fix umlaute
                pass
                

df = pd.concat(dfs)

# convert data frame to sqllite

df.to_sql('tirol', 'sqlite:///./project/data/tirol.sqlite', if_exists='replace', index=False)

# Hamburg source data in GeoJSON

hamburg_urls = list()

hamburg_route1_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute1/items?bulk=true&f=json'
hamburg_route2_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute2/items?bulk=true&f=json'
hamburg_route3_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute3/items?bulk=true&f=json'
hamburg_route4_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute4/items?bulk=true&f=json'
hamburg_route5_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute5/items?bulk=true&f=json'
hamburg_route6_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute6/items?bulk=true&f=json'
hamburg_route7_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute7/items?bulk=true&f=json'
hamburg_route8_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute8/items?bulk=true&f=json'
hamburg_route9_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute9/items?bulk=true&f=json'
hamburg_route10_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute10/items?bulk=true&f=json'
hamburg_route11_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute11/items?bulk=true&f=json'
hamburg_route12_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute12/items?bulk=true&f=json'
hamburg_route13_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute13/items?bulk=true&f=json'
hamburg_route14_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute14/items?bulk=true&f=json'
hamburg_trail_gruenerring_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_gruenerring/items?bulk=true&f=json'
hamburg_route_hamburgbremen_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_hamburgbremen/items?bulk=true&f=json'
hamburg_route_elberadweg_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_elberadweg/items?bulk=true&f=json'
hamburg_route_eineheideradweg_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_leineheideradweg/items?bulk=true&f=json'
hamburg_route_nordsee_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_nordsee/items?bulk=true&f=json'
hamburg_route_hamburgruegen_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_hamburgruegen/items?bulk=true&f=json'
hamburg_routes_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitrouten/items?bulk=true&f=json'
hamburg_distance_routes_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernwege/items?bulk=true&f=json'

hamburg_urls.append(hamburg_route1_url)
hamburg_urls.append(hamburg_route2_url)
hamburg_urls.append(hamburg_route3_url)
hamburg_urls.append(hamburg_route4_url)
hamburg_urls.append(hamburg_route5_url)
hamburg_urls.append(hamburg_route6_url)
hamburg_urls.append(hamburg_route7_url)
hamburg_urls.append(hamburg_route8_url)
hamburg_urls.append(hamburg_route9_url)
hamburg_urls.append(hamburg_route10_url)
hamburg_urls.append(hamburg_route11_url)
hamburg_urls.append(hamburg_route12_url)
hamburg_urls.append(hamburg_route13_url)
hamburg_urls.append(hamburg_route14_url)
hamburg_urls.append(hamburg_trail_gruenerring_url)
hamburg_urls.append(hamburg_route_hamburgbremen_url)
hamburg_urls.append(hamburg_route_elberadweg_url)
hamburg_urls.append(hamburg_route_eineheideradweg_url)
hamburg_urls.append(hamburg_route_nordsee_url)
hamburg_urls.append(hamburg_route_hamburgruegen_url)
hamburg_urls.append(hamburg_routes_url)
hamburg_urls.append(hamburg_distance_routes_url)

Path("./project/data/tmp/Hamburg").mkdir(parents=True, exist_ok=True)

dfs_hamburg = list()

# Download all GeoJSON  
for url in hamburg_urls:
    filename = './project/data/tmp/Hamburg/{}.json'.format(url.split('/')[7])
    with urllib.request.urlopen(url) as response, open(filename, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
        try:
            gdf = gpd.read_file(filename)
        except:
            pass
        
        df = pd.DataFrame(gdf.drop(columns='geometry'))
        dfs_hamburg.append(df)

df = pd.concat(dfs_hamburg)

# convert data frame to sqllite

df.to_sql('hamburg', 'sqlite:///./project/data/hamburg.sqlite', if_exists='replace', index=False)

