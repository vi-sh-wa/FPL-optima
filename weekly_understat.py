
import time
import random

import tqdm
from understatapi import UnderstatClient
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()
TABLE_ID = "fpl-optima.fpl_bronze.understat"

def main():
    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()

    last_date_query = "SELECT MAX(date) as last_date FROM `fpl-optima.fpl_bronze.understat`"
    last_date = client.query(last_date_query).to_dataframe()['last_date'].iloc[0]

    # If the table is empty, set a default early date
    if pd.isna(last_date):
        last_date = '2025-08-01' 

    batch_dfs = []
    with UnderstatClient() as understat:
        for p_id in tqdm(active_ids, desc="Weekly Understat Update"):
            try:
                
                data = understat.player(player=p_id).get_match_data()
                df = pd.DataFrame(data)
                    
                df['date'] = pd.to_datetime(df['date'])
                df = df[df['date'] > pd.to_datetime(last_date)]
                    
                if not df.empty:
                    df['player_id'] = p_id
                    batch_dfs.append(df)
                    
                time.sleep(random.uniform(1.0, 2.0))
                

                if len(batch_dfs) >= 50:
                    upload_to_bq(batch_dfs)
                    batch_dfs = []
                    
            except Exception as e:
                print(f"Error on {p_id}: {e}")

def upload_to_bq(dfs):
    final_df = pd.concat(dfs, ignore_index=True)
    cols = ['xG', 'xA', 'goals', 'assists', 'shots', 'key_passes']
    for c in cols:
        final_df[c] = pd.to_numeric(final_df[c], errors='coerce').fillna(0)
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(final_df, TABLE_ID, job_config=job_config).result()