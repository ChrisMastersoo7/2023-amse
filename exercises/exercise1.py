import pandas as pd 

dataset_url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'

df = pd.read_csv(dataset_url, sep=';')

df.to_sql('airports', 'sqlite:///airports.sqlite', if_exists='replace', index=False)

