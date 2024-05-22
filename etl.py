import requests
import pandas as pd
from sqlalchemy import create_engine

username = 'postgres'
password = 'lalamama'
host = 'localhost'
port = 5432
dbname = 'airfreight_automation'

def extract() -> dict:
    API_URL="http://universities.hipolabs.com/search?country=United+States"
    data=requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Total number of records in the data {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Total number of universities in the california {len(df)}")
    df['domains'] = [','.join(map(str,l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str,l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[['domains','country','web_pages','name']]

def load(df:pd.DataFrame)-> None:
    disk_engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}')
    df.to_sql('cal_uni',disk_engine, if_exists='replace')

data=extract()
df = transform(data)
load(df)