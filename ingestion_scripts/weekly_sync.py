import yaml
import os
import pandas as pd
from understatapi import UnderstatClient

from get_match import get_match
from understat_scrapper import get_roster_data
from fpl_scrapper import get_fpl_metadata, fetch_player_histories

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

FPL_SEASON = config['current_season_fpl']
USTAT_SEASONS = config['current_season_understat'] 
FPL_PATH = config['paths']['fpl_historical']
USTAT_SUMMARY_PATH = config['paths']['understat_summary']
USTAT_ROSTER_PATH = config['paths']['understat_roster']

def run_fpl_pipeline():
    print("\n Starting FPL Update")
    if os.path.exists(FPL_PATH):
        existing_df = pd.read_parquet(FPL_PATH)
        season_data = existing_df[existing_df['season'] == FPL_SEASON]
        last_round = season_data['round'].max() if not season_data.empty else None
    else:
        existing_df = pd.DataFrame()
        last_round = None

    boot_data = get_fpl_metadata()
    target_round = int(last_round + 1) if pd.notnull(last_round) else 1
    target_event = next((e for e in boot_data['events'] if e['id'] == target_round), None)

    if not target_event or not (target_event['finished'] and target_event['data_checked']):
        print(f"Round {target_round} is not ready yet.")
        return

    print(f"Round {target_round} is ready. Fetching...")
    
    team_map = {t['id']: t['name'] for t in boot_data['teams']}
    pos_map = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}
    player_map = {
        p['id']: {
            'player_name': f"{p['first_name']}_{p['second_name']}",
            'team': team_map.get(p['team']),
            'position': pos_map.get(p['element_type']),
            'xP': float(p['ep_next'] or 0) 
        } for p in boot_data['elements']
    }

    new_data_df = fetch_player_histories(player_map, FPL_SEASON)

    if not new_data_df.empty:
        combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
        
    numeric_cols = [
        'expected_goals', 'expected_assists', 'expected_goal_involvements', 
        'expected_goals_conceded', 'value', 'selected', 'transfers_in',
        'transfers_out', 'influence', 'creativity', 'threat', 'ict_index', 'xp'
    ]
    for col in numeric_cols:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0.0).astype('float64')

        combined_df = combined_df.drop_duplicates(subset=['player_name', 'round', 'fixture','season'], keep='last')
        combined_df.to_parquet(FPL_PATH, index=False)
        print(f"Success: GW {target_round} added.")

def run_understat_pipeline(understat_client):
    print("\nStarting Understat Update")

    #Match summary : Match ids in a specifi season

    if os.path.exists(USTAT_SUMMARY_PATH):
        existing_summ = pd.read_parquet(USTAT_SUMMARY_PATH, columns=['match_id'])
        completed_matches = set(existing_summ['match_id'].astype(str).tolist())
    else:
        completed_matches = set()

    new_matches = get_match(completed_matches, understat_client)
    if new_matches:
        new_summ_df = pd.DataFrame(new_matches)
        if not completed_matches:
            final_summ = new_summ_df
        else:
            final_summ = pd.concat([pd.read_parquet(USTAT_SUMMARY_PATH), new_summ_df], ignore_index=True)
        final_summ.to_parquet(USTAT_SUMMARY_PATH, index=False)
        print(f"Summary: Added {len(new_matches)} matches.")

    # Player data for all the match ids in a specific season

    if os.path.exists(USTAT_ROSTER_PATH):
        existing_rost = pd.read_parquet(USTAT_ROSTER_PATH)
        skip_ids = set(existing_rost['match_id'].astype(str).unique())
    else:
        existing_rost = pd.DataFrame()
        skip_ids = set()

    new_rost_list = get_roster_data(understat_client, USTAT_SEASONS, skip_ids)
    
    if new_rost_list:
        new_rost_df = pd.DataFrame(new_rost_list)
        
    if not new_rost_df.empty:
        for col in ['xG', 'xA', 'npxG', 'xGChain', 'xGBuildup']:
            if col in new_rost_df.columns:
                # Convert to numeric, turn errors to NaN, then fill NaN with 0.0
                new_rost_df[col] = pd.to_numeric(new_rost_df[col], errors='coerce').fillna(0.0)

        final_rost = pd.concat([existing_rost, new_rost_df], ignore_index=True)
        final_rost.to_parquet(USTAT_ROSTER_PATH, index=False)
        print(f"Roster: Added {len(new_rost_df)} rows.")

if __name__ == "__main__":
    with UnderstatClient() as understat:
        run_fpl_pipeline()
        run_understat_pipeline(understat)
    
    print("\nAll tables updated!")