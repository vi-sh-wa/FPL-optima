
import time
import random

import tqdm
from understatapi import UnderstatClient
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()
TABLE_ID = "fpl-optima.fpl_bronze.understat_past_match_level"

def main():
    # 1. Get the list of IDs currently in your FPL table
    # We only care about players active in the league right now
    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()

    batch_dfs = []
    
    for p_id in tqdm(active_ids, desc="Weekly Understat Update"):
        try:
            with UnderstatClient() as understat:
                # get_match_data() gets the granular history we need for ML
                data = understat.player(player=p_id).get_match_data()
                df = pd.DataFrame(data)
                
                # FILTER: Only keep matches from the current season
                df = df[df['season'] == '2025']
                df['player_id'] = p_id
                
                batch_dfs.append(df)
                
            # Rest to avoid 429 Errors
            time.sleep(random.uniform(1.0, 2.0))
            
            # Upload in batches of 50 to keep BigQuery happy
            if len(batch_dfs) >= 50:
                upload_to_bq(batch_dfs)
                batch_dfs = []
                
        except Exception as e:
            print(f"Error on {p_id}: {e}")

def upload_to_bq(dfs):
    final_df = pd.concat(dfs, ignore_index=True)
    # Ensure numeric types
    cols = ['xG', 'xA', 'goals', 'assists', 'shots', 'key_passes']
    for c in cols:
        final_df[c] = pd.to_numeric(final_df[c], errors='coerce').fillna(0)
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(final_df, TABLE_ID, job_config=job_config).result()