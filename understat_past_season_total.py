import time
import random
from understatapi import UnderstatClient
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()
TABLE_ID = "fpl-optima.fpl_bronze.understat_historical_all"

def ingest_with_retry(season, retries=3):
    for attempt in range(retries):
        try:
            with UnderstatClient() as understat:
                print(f"Fetching {season} (Attempt {attempt + 1})...")
                data = understat.league(league="EPL").get_player_data(season=season)
                return pd.DataFrame(data)
        except Exception as e:
            wait = (attempt + 1) * 30  # Wait 30s, 60s, 90s...
            print(f"Error for {season}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    return None

def main():
    seasons = [str(year) for year in range(2016, 2025)] 
    
    for season in seasons:
        df = ingest_with_retry(season)
        
        if df is not None:
            df['season_start_year'] = season
            cols_to_fix = ['xG', 'xA', 'npg', 'npxG', 'xGChain', 'xGBuildup', 'goals', 'assists']
            df[cols_to_fix] = df[cols_to_fix].apply(pd.to_numeric)
            
            # Write to BigQuery
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
            
            pause = random.randint(15, 45)
            print(f"Success! Resting for {pause} seconds...")
            time.sleep(pause)
        else:
            print(f"Critical failure: Could not get data for {season} after 3 retries.")

if __name__ == "__main__":
    main()
