import time, random
import pandas as pd
from tqdm import tqdm
from understatapi import UnderstatClient
from google.cloud import bigquery


client = bigquery.Client()
DEST_TABLE = "fpl-optima.fpl_bronze.understat_match_data"
url = "https://raw.githubusercontent.com/ChrisMusson/FPL-ID-Map/refs/heads/main/Understat.csv"

BATCH_SIZE = 50  # Number of players to collect before uploading to BigQuery

def get_already_scraped_ids():
    try:
        query = f"SELECT DISTINCT CAST(understat AS STRING) as id FROM `{DEST_TABLE}`"
        return set(row.id for row in client.query(query))
    except Exception:
        return set()

def main():
    id_df = pd.read_csv(url)

    all_understat_ids = id_df[understat].dropna().unique().astype(int).astype(str)
    
    scraped_ids = get_already_scraped_ids()

    to_scrape = [i for i in all_understat_ids if i not in scraped_ids]
    
    print(f"Resuming: {len(scraped_ids)} players already in BigQuery.")
    print(f"To Scrape: {len(to_scrape)} players remaining.")

    batch_dfs = []

    with tqdm(to_scrape, desc="Overall Progress", unit="player") as pbar:
        for i, understat_id in enumerate(pbar):
            try:
                pbar.set_description(f"Scraping Player {understat_id}")
                
                with UnderstatClient() as understat:
                    data = understat.player(player=understat_id).get_match_data()
                    if data:
                        df = pd.DataFrame(data)
                        df['understat'] = understat_id 
                        batch_dfs.append(df)
                
                # BATCH UPLOAD LOGIC
                if len(batch_dfs) >= BATCH_SIZE or (i == len(to_scrape) - 1 and batch_dfs):
                    final_df = pd.concat(batch_dfs, ignore_index=True)

                    for col in final_df.columns:
                        final_df[col] = final_df[col].apply(lambda x: x[0] if isinstance(x, list) else x)

                    cols_to_fix = ['xG', 'xA', 'npg', 'npxG', 'xGChain', 'xGBuildup', 
                                   'goals', 'assists', 'key_passes', 'shots', 'time']
                    for col in cols_to_fix:
                        if col in final_df.columns:
                            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype('float64')

                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    client.load_table_from_dataframe(final_df, DEST_TABLE, job_config=job_config).result()
                    
                    batch_dfs = [] 
                    pbar.write(f"Batch of {BATCH_SIZE} saved.")

                time.sleep(random.uniform(1.2, 2.5))
                
            except Exception as e:
                pbar.write(f"Error on {understat_id}: {e}")
                time.sleep(5)

if __name__ == "__main__":
    main()