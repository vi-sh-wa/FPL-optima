import random
import time
import requests
from tqdm import tqdm
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

TABLE_ID = "fpl-optima.fpl_bronze.fpl"

def main():
    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()

    last_gw_query = "SELECT MAX(gw) as last_gw FROM `fpl-optima.fpl_bronze.fpl`"
    last_gw = client.query(last_gw_query).to_dataframe()['last_gw'].iloc[0]

    # If empty (first time running), start from 0
    if pd.isna(last_gw):
        last_gw = 0

    all_new_rows = []

    for p_id in tqdm(active_ids):
        url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
        try:
                    r = requests.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        if 'history' in data and data['history']:
                            df = pd.DataFrame(data['history'])
                            
                            if 'round' in df.columns:
                                df['gw'] = df['round'] 

                            df['player_id'] = p_id 
                            
                            df = df[df['gw'] > last_gw]
                            
                            if not df.empty:
                                all_new_rows.append(df)
                    
                    time.sleep(random.uniform(0.5, 1.0)) # Polite scraping speed
                    
        except Exception as e:
                    print(f"Skipping player {p_id}: {e}")

    if all_new_rows:
        final_df = pd.concat(all_new_rows, ignore_index=True)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(final_df, "fpl-optima.fpl_bronze.fpl_season_match_history", job_config=job_config).result()
        print(f"Successfully added {len(final_df)} new match rows.")
    else:
        print("No new Gameweeks to ingest.")