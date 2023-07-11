import pandas as pd 
import urllib.request
from zipfile import ZipFile
import shutil
import os

def extract_zip(url):
            urllib.request.urlretrieve(url,url.rsplit('/')[-1])
            with ZipFile(url.rsplit('/')[-1], 'r') as zip:
                # printing all the contents of the zip file
                zip.printdir()
            
                # extracting all the files
                print('Extracting all the files now...')
                zip.extractall(url.rsplit('/')[-1].rsplit('.')[0])
                print('Done!')
            
dataset_url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'

extract_zip(dataset_url)

columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']

df = pd.read_csv('GTFS/stops.txt', encoding='latin1', sep=',') 

df = df[columns]

df = df.astype({'stop_id':'int64', 'stop_name':'str', 'stop_lat':'float', 'stop_lon':'float', 'zone_id':'int64'})

df = df[df['zone_id'] == 2001]

df = df[df['stop_name'].str.match(r'/[^a-zA-Z0-9äöüÄÖÜß]/g')]

df = df[df['stop_lat'].between(-90, 90)]

df = df[df['stop_lon'].between(-90, 90)]
        
df = df.dropna()

df.to_sql('stops', 'sqlite:///gtfs.sqlite', if_exists='replace', index=False)

# clean data
shutil.rmtree(dataset_url.rsplit('/')[-1].rsplit('.')[0])
os.remove(dataset_url.rsplit('/')[-1])
