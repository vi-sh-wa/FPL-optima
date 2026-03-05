import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

table_id = "fpl-optima.fpl_bronze.fpl_code_understat"

def code_x_understat_map_id():
    url = "https://raw.githubusercontent.com/ChrisMusson/FPL-ID-Map/refs/heads/main/Understat.csv"
    df = pd.read_csv(url)

    print(f"Uploading {len(df)} player mappings to BigQuery...")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    
    job.result() 

    print(f"Mission complete")

if __name__ == "__main__":
    code_x_understat_map_id()