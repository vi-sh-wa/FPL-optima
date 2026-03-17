import yaml
import os
import pandas as pd
from understatapi import UnderstatClient

from ingestion_scripts.get_match import get_match
from ingestion_scripts.understat_scrapper import get_roster_data
from ingestion_scripts.fpl_scrapper import *

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

FPL_SEASON = config['current_season_fpl']
USTAT_SEASONS = config['current_season_understat'] 
FPL_PATH = config['paths']['fpl_current']
USTAT_SUMMARY_PATH = config['paths']['understat_summary']
USTAT_ROSTER_PATH = config['paths']['understat_roster']



def run_fpl_pipeline():
    print(f"\n--- Starting FPL Update for {FPL_SEASON} ---")
    
    #Checking local file
    if os.path.exists(FPL_PATH):
        existing_df = pd.read_parquet(FPL_PATH)
        season_data = existing_df[existing_df['season'] == FPL_SEASON]
        last_saved_round = season_data['round'].max() if not season_data.empty else 0
    else:
        existing_df = pd.DataFrame()
        last_saved_round = 0

    # Checking if/which GW is ready from the api
    boot_data = get_fpl_metadata()
    # Problem Fix: Correctly identifying finished/checked rounds
    ready_rounds = [e['id'] for e in boot_data['events'] if e['finished'] and e['data_checked']]
    max_ready_round = max(ready_rounds) if ready_rounds else 0

    rounds_to_fetch = list(range(int(last_saved_round + 1), int(max_ready_round + 1)))
    if not rounds_to_fetch:
        print(f"Data is already up to date (GW {last_saved_round}).")
        return
    print(f"Missing gameweek(s): {rounds_to_fetch}")


    player_map = get_player_mappings(boot_data)
    new_data_df = get_player_history(player_map, FPL_SEASON, rounds_to_fetch)

    if not new_data_df.empty:
        new_data_df['kickoff_time'] = pd.to_datetime(new_data_df['kickoff_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)

        numeric_cols = ['expected_goals', 'expected_assists', 'expected_goal_involvements', 
                        'expected_goals_conceded', 'value', 'selected', 'transfers_in','transfers_out', 
                        'influence', 'creativity', 'threat', 'ict_index', 'xP']
        for col in numeric_cols:
            if col in combined_df.columns:
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0.0)

        combined_df = combined_df.drop_duplicates(subset=['name', 'round', 'fixture', 'season'], keep='last')
        
        combined_df.to_parquet(FPL_PATH, index=False)
        print(f"Success: Gameweek {rounds_to_fetch} added to current seasons data.")





def run_understat_pipeline(understat_client):
    print("\nStarting Understat Update")

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