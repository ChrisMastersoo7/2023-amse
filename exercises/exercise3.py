import pandas as pd 

dataset_url = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'

columns = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro','hybrid', 'plugInHybrid', 'others']

df = pd.read_csv(dataset_url, sep=';', encoding="latin1", header=None, skiprows=7, skipfooter=4,
                 usecols=[*range(0, 3), 12, 22, 32, 42, 52, 62, 72], names=columns, engine='python',
                 converters={'CIN': str})

df = df[df["petrol"].str.contains("-")==False]
df = df.astype({'petrol':'int64', 'diesel':'int64', 'gas':'int64', 'electro':'int64', 'hybrid':'int64', 'plugInHybrid':'int64', 'others':'int64'})

df.to_sql('cars', 'sqlite:///cars.sqlite', if_exists='replace', index=False)