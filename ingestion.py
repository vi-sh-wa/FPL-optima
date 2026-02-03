import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

def ingest_season_data(seasons):
    table_id = "fpl-optima.fpl_bronze.past_season_data_raw"

    job_config = bigquery.LoadJobConfig(
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,  # handling new columns in future seasons
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ],
        write_disposition="WRITE_APPEND",
    )

    for s in seasons:
        url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{s}/gws/merged_gw.csv"
        print(f"--- Processing Season: {s} ---")
        
        try:
            df = pd.read_csv(url, encoding='latin1')
            df['season'] = s

            df.columns = [c.lower( ).replace('.', '_').replace(' ', '_') for c in df.columns] #clean column names
            
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  
            print(f"Successfully uploaded {len(df)} rows to {table_id}")
            
        except Exception as e:
            print(f"Error ingesting {s}: {e}")

if __name__ == "__main__":
    fpl_seasons = ["2016-17", "2017-18", "2018-19", "2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]
    ingest_season_data(fpl_seasons)