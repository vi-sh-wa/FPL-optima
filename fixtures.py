import requests
import pandas as pd
from google.cloud import bigquery
import json

# Configuration
client = bigquery.Client()
table = "fpl-optima.fpl_bronze.fixtures"
fixture_url = "https://fantasy.premierleague.com/api/fixtures/"

def get_fixtures():
    print("Fetching fixtures from FPL API...")
    response = requests.get(fixture_url)
    fixtures_data = response.json()

    df = pd.DataFrame(fixtures_data)

    if 'stats' in df.columns:
        df['stats'] = df['stats'].apply(json.dumps)

    if 'kickoff_time' in df.columns:
        df['kickoff_time'] = pd.to_datetime(df['kickoff_time'])

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE" )

    print(f"Uploading {len(df)} rows to {table}")
    try:
        job = client.load_table_from_dataframe(df, table, job_config=job_config)
        job.result()  # Wait for the upload to finish
        print("Fixtures table successfully updated.")
    except Exception as e:
        print(f"Error uploading to BigQuery: {e}")

if __name__ == "__main__":
    get_fixtures()