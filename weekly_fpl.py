import random
import time
import requests
from tqdm import tqdm
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

TABLE_ID = "fpl-optima.fpl_bronze.fpl"

def main():

    boot_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    boot_data = requests.get(boot_url).json()

    teams_df = pd.DataFrame(boot_data['teams'])
    team_map = dict(zip(teams_df['id'], teams_df['name']))

    pos_map = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}

    players_df = pd.DataFrame(boot_data['elements'])
    player_info = {}
    for _, row in players_df.iterrows():
        player_info[row['id']] = {
             'code': row['code'],
            'name': f"{row['first_name']}_{row['second_name']}",
            'team': team_map.get(row['team']),
            'position': pos_map.get(row['element_type']),
            'xP': row['ep_next'] 
        }

    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()


    last_gw_query = "SELECT MAX(gw) as last_gw FROM `fpl-optima.fpl_bronze.fpl` where season = '2025-26'"
    last_gw = client.query(last_gw_query).to_dataframe()['last_gw'].iloc[0]

    # If empty (first time running), start from 0
    if pd.isna(last_gw):
        last_gw = 0

    numeric_cols = ['expected_goals', 'expected_assists', 'expected_goal_involvements', 
                    'expected_goals_conceded', 'value', 'selected', 'transfers_in','transfers_out', 
                    'influence', 'creativity', 'threat', 'ict_index', 'xP']
    
    all_new_rows = []

    for p_id in tqdm(active_ids):
        url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
        try:
                    r = requests.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        if 'history' in data and data['history']:
                            df = pd.DataFrame(data['history'])
                            df['name'] = player_info[p_id]['name']
                            df['team'] = player_info[p_id]['team']
                            df['position'] = player_info[p_id]['position']
                            df['xP'] = player_info[p_id]['xP']
                            df['season'] = '2025/26'
                            
                            if 'round' in df.columns:
                                df['gw'] = df['round'] 

                            df['player_id'] = p_id 
                            
                            df = df.astype({col: 'float64' for col in numeric_cols if col in df.columns})
                            
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