import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

def map_id():
    table_id = "fpl-optima.fpl_bronze.id_map"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True, 
    )


    url = "https://raw.githubusercontent.com/ChrisMusson/FPL-ID-Map/main/Master.csv"
    print("Downloading...")
    df = pd.read_csv(url)

    print(f"Uploading {len(df)} player mappings to BigQuery...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    
    job.result() 

    print(f"Mission complete")

if __name__ == "__main__":
    map_id()