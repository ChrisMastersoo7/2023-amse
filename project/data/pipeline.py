import urllib
import shutil
from gpx_converter import Converter
import pandas as pd

# Tirol source data in zip format

tirol_single_trail_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Einzeletappen.zip'
tirol_day_trips_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Tagestouren.zip'
tirol_multi_day_trips_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Mehrtagestouren.zip'

# Download all zips and extract 
with urllib.request.urlopen(tirol_single_trail_url) as response, open('TW_BikeTrailTirol_Einzeletappen.zip', 'wb') as out_file:
    shutil.copyfileobj(response, out_file)
    shutil.unpack_archive('TW_BikeTrailTirol_Einzeletappen.zip', './project/data/tmp', 'zip')

# convert data from gpx to df
df = Converter(input_file='./project/data/tmp/gpx/01 Steeg-Weissenbach.gpx').gpx_to_dataframe()

# convert data fram to sqllite

df.to_sql('test', 'sqlite:///test.sqlite', if_exists='replace', index=False)

# Hamburg source data in GeoJSON

hamburg_route14_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute14/items?bulk=true&f=json'
hamburg_trail_gruenerring_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_gruenerring/items?bulk=true&f=json'
hamburg_route12_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute12/items?bulk=true&f=json'
hamburg_route13_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute13/items?bulk=true&f=json'
hamburg_route10_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute10/items?bulk=true&f=json'
hamburg_route11_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute11/items?bulk=true&f=json'
hamburg_route1_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute1/items?bulk=true&f=json'
hamburg_route2_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute2/items?bulk=true&f=json'
hamburg_routes_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitrouten/items?bulk=true&f=json'
hamburg_route7_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute7/items?bulk=true&f=json'
hamburg_route8_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute8/items?bulk=true&f=json'
hamburg_route9_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute9/items?bulk=true&f=json'
hamburg_route_hamburgbremen_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_hamburgbremen/items?bulk=true&f=json'
hamburg_route_elberadweg_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_elberadweg/items?bulk=true&f=json'
hamburg_route3_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute3/items?bulk=true&f=json'
hamburg_route4_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute4/items?bulk=true&f=json'
hamburg_route5_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute5/items?bulk=true&f=json'
hamburg_route6_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute6/items?bulk=true&f=json'
hamburg_route_eineheideradweg_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_leineheideradweg/items?bulk=true&f=json'
hamburg_route_nordsee_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_nordsee/items?bulk=true&f=json'
hamburg_route_hamburgruegen_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernweg_hamburgruegen/items?bulk=true&f=json'
hamburg_distance_routes_url = 'https://api.hamburg.de/datasets/v1/freizeitrouten/collections/radfernwege/items?bulk=true&f=json'


