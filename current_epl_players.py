import pandas as pd
import requests
from google.cloud import bigquery
from datetime import datetime

# Initialize
client = bigquery.Client()
table_id = "fpl-optima.fpl_bronze.current_epl_players"
url = "https://fantasy.premierleague.com/api/bootstrap-static/"

def ingest_current_season():
    response = requests.get(url)
    data = response.json()
    
    players_raw = data['elements']
    df = pd.DataFrame(players_raw)
    
    df['scraped_at'] = datetime.now()
    
    current_gw = next((event['id'] for event in data['events'] if event['is_current']), None)
    df['gameweek'] = current_gw

    # 5. Load to BigQuery
    # We use WRITE_APPEND because we want a historical log of every week
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    
    print(f"Uploading {len(df)} players for Gameweek {current_gw}...")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print("Success!")

if __name__ == "__main__":
    ingest_current_season()