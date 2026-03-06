import random
import time
import requests
from tqdm import tqdm
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

def main():
    # 1. Get Global Metadata and find the CURRENT Gameweek
    boot_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    boot_data = requests.get(boot_url).json()

    # Find the current gameweek (the one that is 'is_current' or the last 'finished' one)
    current_gw = None
    for event in boot_data['events']:
        if event['is_current']:
            current_gw = event['id']
            break
    
    # If no GW is marked 'current' (e.g., between seasons), get the most recent finished one
    if not current_gw:
        finished_gws = [e['id'] for e in boot_data['events'] if e['finished']]
        current_gw = max(finished_gws) if finished_gws else 1

    print(f"Targeting Gameweek: {current_gw}")

    # Map Teams and Positions
    teams_df = pd.DataFrame(boot_data['teams'])
    team_map = dict(zip(teams_df['id'], teams_df['name']))
    pos_map = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}

    # Map Player Info
    players_df = pd.DataFrame(boot_data['elements'])
    player_info = {
        row['id']: {
            'code': row['code'],
            'name': f"{row['first_name']}_{row['second_name']}",
            'team': team_map.get(row['team']),
            'position': pos_map.get(row['element_type']),
            'xP': row['ep_next'] 
        } for _, row in players_df.iterrows()
    }

    # 2. Get list of players to check
    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()

    all_new_rows = []

    # 3. Request history for each player
    for p_id in tqdm(active_ids):
        url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if 'history' in data and data['history']:
                    df = pd.DataFrame(data['history'])
                    
                    # FILTER HERE: Only keep the row for the target Gameweek
                    # Note: API uses 'round' for Gameweek number
                    df = df[df['round'] == current_gw]
                    
                    if not df.empty:
                        df['code'] = player_info[p_id]['code']
                        df['name'] = player_info[p_id]['name']
                        df['team'] = player_info[p_id]['team']
                        df['position'] = player_info[p_id]['position']
                        df['xP'] = player_info[p_id]['xP']
                        df['season'] = '2025-26'
                        df['element'] = p_id 
                        
                        all_new_rows.append(df)
            
            time.sleep(random.uniform(0.4, 0.8)) # Polite scraping
                    
        except Exception as e:
            print(f"Skipping player {p_id}: {e}")

    # 4. Upload to BigQuery
    if all_new_rows:
        final_df = pd.concat(all_new_rows, ignore_index=True)
        
        # Decide: Do you want to overwrite the "Weekly" table (WRITE_TRUNCATE)
        # or add to a Master History (WRITE_APPEND)?
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        dest_table = "fpl-optima.fpl_bronze.weekly_fpl_latest"
        client.load_table_from_dataframe(final_df, dest_table, job_config=job_config).result()
        print(f"Successfully uploaded {len(final_df)} rows for GW {current_gw}.")
    else:
        print(f"No data found for Gameweek {current_gw}.")

if __name__ == "__main__":
    main()