import random
import time
import requests
from tqdm import tqdm
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# This is the table we check to see where we left off
MASTER_TABLE = "fpl-optima.fpl_bronze.fpl"
# This is the table where we store the fresh weekly pull
DESTINATION_TABLE = "fpl-optima.fpl_bronze.fpl_season_match_history"

def main():
    # 1. Get Metadata & Team/Pos Maps
    boot_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    boot_data = requests.get(boot_url).json()

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

    # 2. Get active players to iterate through
    query = "SELECT DISTINCT id FROM `fpl-optima.fpl_bronze.current_epl_players`"
    active_ids = client.query(query).to_dataframe()['id'].tolist()

    # 3. Determine the TARGET ROUND
    # We find the max round currently in your master table for this season
    last_round_query = f"SELECT MAX(round) as last_round FROM `{MASTER_TABLE}` WHERE season = '2025-26'"
    last_round_res = client.query(last_round_query).to_dataframe()['last_round'].iloc[0]
    
    # If the table is empty, we start at 1. Otherwise, we want the NEXT round.
    target_round = int(last_round_res + 1) if pd.notnull(last_round_res) else 1
    print(f"Latest round in database: {last_round_res}. Target Ingest Round: {target_round}")

    numeric_cols = ['expected_goals', 'expected_assists', 'expected_goal_involvements', 
                    'expected_goals_conceded', 'value', 'selected', 'transfers_in','transfers_out', 
                    'influence', 'creativity', 'threat', 'ict_index', 'xP']
    
    all_new_rows = []

    # 4. Fetching Data
    for p_id in tqdm(active_ids):
        url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if 'history' in data and data['history']:
                    df = pd.DataFrame(data['history'])
                    
                    # --- THE CHANGE: Filter for exactly the target round ---
                    df = df[df['round'] == target_round]
                    
                    if not df.empty:
                        df['code'] = player_info[p_id]['code']
                        df['name'] = player_info[p_id]['name']
                        df['team'] = player_info[p_id]['team']
                        df['position'] = player_info[p_id]['position']
                        df['xP'] = player_info[p_id]['xP']
                        df['season'] = '2025-26'
                        df['element'] = p_id 
                        
                        # Convert numeric columns to avoid schema issues
                        for col in numeric_cols:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        
                        all_new_rows.append(df)
            
            time.sleep(random.uniform(0.4, 0.8)) # Rate limiting
                    
        except Exception as e:
            print(f"Skipping player {p_id}: {e}")

    # 5. Load to BigQuery
    if all_new_rows:
        final_df = pd.concat(all_new_rows, ignore_index=True)
        
        # Using WRITE_TRUNCATE so this table always represents the "Fresh Weekly Pull"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        print(f"Uploading {len(final_df)} rows for Round {target_round} to {DESTINATION_TABLE}...")
        client.load_table_from_dataframe(final_df, DESTINATION_TABLE, job_config=job_config).result()
        print("Weekly Ingest Complete!")
    else:
        print(f"No rows found for Round {target_round}. Is the Gameweek finished yet?")

if __name__ == "__main__":
    main()