import time, random
import pandas as pd
from tqdm import tqdm
from understatapi import UnderstatClient
from google.cloud import bigquery

# CONFIG
client = bigquery.Client()
DEST_TABLE = "fpl-optima.fpl_bronze.understat_past_match_level"
ID_MAP_URL = "https://raw.githubusercontent.com/ChrisMusson/FPL-ID-Map/main/Master.csv"


def get_already_scraped_ids():
    try:
        query = f"SELECT DISTINCT CAST(player_id AS STRING) as id FROM `{DEST_TABLE}`" #to avoid repetitions in case of multiple matches per player
        return set(row.id for row in client.query(query))
    except:
        return set()

def main():
    id_df = pd.read_csv(ID_MAP_URL)
    understat_col = [c for c in id_df.columns if 'understat' in c.lower()][0]
    all_understat_ids = id_df[understat_col].dropna().unique().astype(int).astype(str)
    
    scraped_ids = get_already_scraped_ids()
    to_scrape = [i for i in all_understat_ids if i not in scraped_ids]
    
    print(f"Resuming: {len(scraped_ids)} players already in BigQuery.")
    print(f"To Scrape: {len(to_scrape)} players remaining.")

    with tqdm(to_scrape, desc="Overall Progress", unit="player") as pbar:
        for p_id in pbar:
            try:
                pbar.set_description(f"Scraping Player {p_id}") # Update the status bar with the current player ID
                
                with UnderstatClient() as understat:
                    data = understat.player(player_id=p_id).get_match_data()
                    
                    if data:
                        df = pd.DataFrame(data)
                        df['player_id'] = p_id 
                        
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        client.load_table_from_dataframe(df, DEST_TABLE, job_config=job_config).result()
                
                time.sleep(random.uniform(1.2, 2.8))
                
            except Exception as e:
                pbar.write(f"Error on {p_id}: {e}")
                time.sleep(10)

if __name__ == "__main__":
    main()