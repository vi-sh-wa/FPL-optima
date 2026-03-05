import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

table_id = "fpl-optima.fpl_bronze.fpl_code_element_season"
def map_id():
    import pandas as pd
    base_url = "https://raw.githubusercontent.com/ChrisMusson/FPL-ID-Map/main/FPL/"

    seasons = [
        "16-17", "17-18", "18-19", "19-20", 
        "20-21", "21-22", "22-23", "23-24", "24-25", "25-26"
    ]

    all_dfs = []

    for season in seasons:
        try:
            url = f"{base_url}{season}.csv"
            df = pd.read_csv(url)
            
            if season in df.columns:
                df = df.rename(columns={season: 'element'})
            
            df['season'] = season
            
            all_dfs.append(df)

            print(f"Successfully processed season: {season}")
            
        except Exception as e:
            print(f"Could not retrieve {season}: {e}")

    fpl_code_element_season_map = pd.concat(all_dfs, ignore_index=True)

    print(f"Uploading {len(fpl_code_element_season_map)} player mappings to BigQuery...")
    job = client.load_table_from_dataframe(fpl_code_element_season_map, table_id, job_config=job_config)
    
    job.result() 

    print(f"Mission complete")

if __name__ == "__main__":
    map_id()