import time, random
import pandas as pd
from tqdm import tqdm
from understatapi import UnderstatClient
from google.cloud import bigquery

client = bigquery.Client()

MASTER_ID_TABLE = "fpl-optima.fpl_silver.master_id"
BRONZE_UNDERSTAT_TABLE = "fpl-optima.fpl_bronze.understat_match_data"

def get_mapped_understat_ids():
    query = f"""
        SELECT DISTINCT CAST(understat AS STRING) as id 
        FROM `{MASTER_ID_TABLE}` 
        WHERE understat IS NOT NULL
    """
    return [row.id for row in client.query(query)]

def main():
    understat_ids = get_mapped_understat_ids()
    print(f"Found {len(understat_ids)} mapped players. Starting understat ingest...")

    batch_dfs = []
    
    with UnderstatClient() as understat:
        for i, u_id in enumerate(tqdm(understat_ids)):
            try:
                data = understat.player(player=u_id).get_match_data()
                
                if data:
                    df = pd.DataFrame(data)
                    
                    df['understat_id'] = u_id
                    
                    df = df[df['date'] >= '2016-08-01']
                    
                    if not df.empty:
                        batch_dfs.append(df)

                # BigQuery Batch Uploading (every 50 players to save memory/API quota)
                if len(batch_dfs) >= 100 or (i == len(understat_ids) - 1 and batch_dfs):
                    final_df = pd.concat(batch_dfs, ignore_index=True)
                    
                    # Numeric cleanup
                    cols = ['xG', 'xA', 'xGChain', 'xGBuildup', 'shots', 'key_passes', 'time']
                    for col in cols:
                        if col in final_df.columns:
                            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0.0)

                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    client.load_table_from_dataframe(final_df, BRONZE_UNDERSTAT_TABLE, job_config=job_config).result()
                    
                    batch_dfs = [] # Reset batch
                
                # Polite scraping delay
                time.sleep(random.uniform(0.6, 1.2))
                
            except Exception as e:
                print(f"Skipping {u_id}: {e}")

if __name__ == "__main__":
    main()