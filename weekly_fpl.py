import random
import time
import requests
from tqdm import tqdm
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# Configuration
MASTER_TABLE = "fpl-optima.fpl_bronze.fpl"
DESTINATION_TABLE = "fpl-optima.fpl_bronze.fpl_season_match_history"

def main():
    # 1. Get Global Metadata
    boot_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    try:
        boot_data = requests.get(boot_url).json()
    except Exception as e:
        print(f"Failed to reach FPL API: {e}")
        return

    # 2. Determine the TARGET ROUND from Master Data
    last_round_query = f"SELECT MAX(round) as last_round FROM `{MASTER_TABLE}` WHERE season = '2025-26'"
    last_round_res = client.query(last_round_query).to_dataframe()['last_round'].iloc[0]
    
    # If table is empty, start at 1. Otherwise, target the next round.
    target_round = int(last_round_res + 1) if pd.notnull(last_round_res) else 1

    # 3. --- THE "METICULOUS" GATEKEEPER CHECK ---
    target_event = next((e for e in boot_data['events'] if e['id'] == target_round), None)

    if not target_event:
        print(f"Round {target_round} is not in the current season schedule.")
        return

    # We check both 'finished' and 'data_checked' for 100% finalized data
    if not (target_event['finished'] and target_event['data_checked']):
        print(f"🚨 Stand down! Round {target_round} is not finalized yet.")
        print(f"Status - Finished: {target_event['finished']}, Data Checked: {target_event['data_checked']}")
        return

    print(f"✅ Round {target_round} is officially finalized. Starting ingest...")

    # 4. Map Metadata for the Loop
    teams_df = pd.DataFrame(boot_data['teams'])
    team_map = dict(zip(teams_df['id'], teams_df['name']))
    pos_map = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}

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

    # 5. Get active player IDs
    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()

    all_new_rows = []
    numeric_cols = ['expected_goals', 'expected_assists', 'expected_goal_involvements', 
                    'expected_goals_conceded', 'value', 'selected', 'transfers_in','transfers_out', 
                    'influence', 'creativity', 'threat', 'ict_index', 'xP']

    # 6. Fetching Loop
    for p_id in tqdm(active_ids):
        url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if 'history' in data and data['history']:
                    df = pd.DataFrame(data['history'])
                    
                    # Filter for only the targeted finalized round
                    df = df[df['round'] == target_round]
                    
                    if not df.empty:
                        df['code'] = player_info[p_id]['code']
                        df['name'] = player_info[p_id]['name']
                        df['team'] = player_info[p_id]['team']
                        df['position'] = player_info[p_id]['position']
                        df['xP'] = player_info[p_id]['xP']
                        df['season'] = '2025-26'
                        df['element'] = p_id 
                        
                        # Data Cleaning: Fix types
                        for col in numeric_cols:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                        
                        all_new_rows.append(df)
            
            time.sleep(random.uniform(0.4, 0.7)) # Be kind to the API
                    
        except Exception as e:
            print(f"Skipping player {p_id}: {e}")

    # 7. Final Load
    if all_new_rows:
        final_df = pd.concat(all_new_rows, ignore_index=True)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        print(f"Uploading {len(final_df)} rows for Round {target_round} to BigQuery...")
        client.load_table_from_dataframe(final_df, DESTINATION_TABLE, job_config=job_config).result()
        print(f"Success! Weekly Bronze table updated with Round {target_round}.")
    else:
        print(f"No match history found for Round {target_round} despite API status.")

if __name__ == "__main__":
    main()