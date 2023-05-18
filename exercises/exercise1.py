from pathlib import Path
import pandas as pd 

dataset_url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'

Path("./exercises/tmp/exercise1").mkdir(parents=True, exist_ok=True)

df = pd.read_csv(dataset_url, sep=';')

df.to_sql('airports', 'sqlite:///./exercises/tmp/exercise1/airports.sqlite', if_exists='replace', index=False)

