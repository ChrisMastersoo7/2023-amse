import os
import urllib
import shutil
from pathlib import Path
from gpx_converter import Converter
import pandas as pd
import geopandas as gpd
import unicodedata


class Pipeline:
    def __init__(self, name:str, name_of_root_tmp_dir:str="./project/data/tmp"):
        self.name = name
        self.name_of_root_tmp_dir = name_of_root_tmp_dir
        self.path_pipeline_sub_dir = Path(self.name_of_root_tmp_dir).joinpath(self.name)
        self._create_tmp_dir()
        
    def _create_tmp_dir(self):
        self.path_pipeline_sub_dir.mkdir(parents=True, exist_ok=True)

    def extract_zip(self, url):
        with urllib.request.urlopen(url) as response, open(self.path_pipeline_sub_dir.joinpath(url.rsplit('/')[-1]), 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            shutil.unpack_archive(self.path_pipeline_sub_dir.joinpath(url.rsplit('/')[-1]), self.path_pipeline_sub_dir.joinpath(url.rsplit('/')[-1].rsplit('.')[0]), 'zip')
    
    def convert_from_gpx_to_gdf(self):
        geo_data_frames = list()
        track = gpd.GeoDataFrame(columns=['name', 'geometry'], geometry='geometry')
        for root, dirs, files in os.walk(self.name_of_root_tmp_dir):
            for file in files:
                if file.endswith('.gpx'):
                    # handle umlauts
                    # special_char_map = {ord('ä'):'ae', ord('ü'):'ue', ord('ö'):'oe', ord('ß'):'ss'}
                    # umlaut = os.path.join(root, file)
                    # umlaut.translate(special_char_map)
                    try:
                        gdf = gpd.read_file(os.path.join(root, file), layer='tracks')
                        # df.columns.values[0] = file.rsplit('.')[0]
                        track = pd.concat([track, gdf[['name', 'geometry']]])
                        track.sort_values(by="name", inplace=True)
                        track.reset_index(inplace=True, drop=True)
                        geo_data_frames.append(track)
                    except UnicodeDecodeError:
                        # TODO: fix umlaute
                        pass
        return gpd.GeoDataFrame(pd.concat(geo_data_frames, ignore_index=True))
    
    def convert_from_gpx_to_df(self):
        data_frames = list()
        for root, dirs, files in os.walk(self.name_of_root_tmp_dir):
            for file in files:
                if file.endswith('.gpx'):
                    # handle umlauts
                    # special_char_map = {ord('ä'):'ae', ord('ü'):'ue', ord('ö'):'oe', ord('ß'):'ss'}
                    # umlaut = os.path.join(root, file)
                    # umlaut.translate(special_char_map)
                    try:
                        df = Converter(input_file=os.path.join(root, file)).gpx_to_dataframe()
                        df.columns.values[0] = file.rsplit('.')[0]
                        data_frames.append(df)
                    except UnicodeDecodeError:
                        # TODO: fix umlaute
                        pass
        return pd.concat(data_frames)
    
    def extract_from_GeoJSON_to_gdf(self, url):
        filename = self.path_pipeline_sub_dir.joinpath('{}.json'.format(url.split('/')[7]))
        with urllib.request.urlopen(url) as response, open(filename, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            try:
                gdf = gpd.read_file(filename)
                return gdf
            except:
                pass
            
    def extract_from_GeoJSON_to_df(self, url):
        filename = self.path_pipeline_sub_dir.joinpath('{}.json'.format(url.split('/')[7]))
        with urllib.request.urlopen(url) as response, open(filename, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            try:
                gdf = gpd.read_file(filename)
                df = pd.DataFrame(gdf.drop(columns='geometry'))
                return df
            except:
                pass
            
            #gdf.set_index('id').geometry.explode(ignore_index=False).apply(pd.Series).reset_index().rename(columns={0: 'Langitude', 1: 'Longitude'})
            
            #gdf['lon'] = gdf['geometry'].apply(lambda p: p.x)
            #gdf['lat'] = gdf.point_object.apply(lambda p: p.y)
            
    def create_spatial_database(self, geodataframe:gpd.GeoDataFrame, name_of_root_tmp_dir=None, name_of_database=None):
        if name_of_root_tmp_dir is None:
            name_of_root_tmp_dir = self.name_of_root_tmp_dir
        if name_of_database is None:
            name_of_database = self.name
        parent_path = Path(name_of_root_tmp_dir).parent
        geodataframe.to_file('{}/{}.sqlite'.format(parent_path.absolute(), name_of_database), driver='SQLite', spatialite=True, layer=name_of_database)
    
    def create_database(self, dataframe:pd.DataFrame, name_of_root_tmp_dir=None, name_of_database=None):
        if name_of_root_tmp_dir is None:
            name_of_root_tmp_dir = self.name_of_root_tmp_dir
        if name_of_database is None:
            name_of_database = self.name
        parent_path = Path(name_of_root_tmp_dir).parent
        dataframe.to_sql(name_of_database, 'sqlite:///{}/{}.sqlite'.format(parent_path.absolute(), name_of_database), if_exists='replace', index=False)
        

if __name__ == "__main__":
    
    # Tirol source data in zip format
    tirol_urls = []

    tirol_single_trail_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Einzeletappen.zip'
    tirol_day_trips_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Tagestouren.zip'
    tirol_multi_day_trips_url = 'https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Mehrtagestouren.zip'

    tirol_urls.append(tirol_single_trail_url)
    #tirol_urls.append(tirol_day_trips_url)
    tirol_urls.append(tirol_multi_day_trips_url)
    
    tirol_pipeline = Pipeline('tirol')
    for url in tirol_urls:
        tirol_pipeline.extract_zip(url)
    '''
    df = tirol_pipeline.convert_from_gpx_to_df()
    tirol_pipeline.create_database(df)
    '''
    gdf = tirol_pipeline.convert_from_gpx_to_gdf()
    tirol_pipeline.create_spatial_database(gdf)
    # Hamburg source data in GeoJSON

    hamburg_urls = []

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
    
    hamburg_pipeline = Pipeline('hamburg')
    hamburg_dfs = list()
    '''
    for url in hamburg_urls:
        hamburg_df = hamburg_pipeline.extract_from_GeoJSON_to_df(url)
        hamburg_dfs.append(hamburg_df)
    hamburg_df = pd.concat(hamburg_dfs)
    hamburg_pipeline.create_database(hamburg_df)
    '''
    for url in hamburg_urls:
        hamburg_gdf = hamburg_pipeline.extract_from_GeoJSON_to_gdf(url)
        hamburg_dfs.append(hamburg_gdf)
    hamburg_gdf = gpd.GeoDataFrame( pd.concat( hamburg_dfs, ignore_index=True))
    hamburg_pipeline.create_spatial_database(hamburg_gdf)
    
# use sqlite spatial databse and check to plot
'''
gdf.to_file(
    'dataframe.sqlite', driver='SQLite', spatialite=True, layer='test'
)  
gpd.GeoDataFrame( pd.concat( dataframesList, ignore_index=True) )
'''
    