import pandas as pd
import os
import time
import random
from tqdm import tqdm
from understatapi import UnderstatClient
understat = UnderstatClient()

seasons = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
match_map = {}

def get_roster():
    for season in seasons:
        print(f"Mapping season: {season}")
        league_matches = understat.league(league="EPL").get_match_data(season=season)
        
        for m in league_matches:
            m_id = m['id']
            match_map[m_id] = {
                'h': m['h']['title'],
                'a': m['a']['title'],
                'season': season,
                'datetime': m['datetime'] # Helpful for time-series analysis
            }

    match_ids = list(match_map.keys())
    print(f"Total matches to scrape: {len(match_ids)}")

    player_match_data = []

    for m_id in tqdm(match_ids, desc="Scraping Roster Details"):
        try:
            roster = understat.match(match=m_id).get_roster_data()
            
            for side in ['h', 'a']:
                team_data = roster.get(side, {})
                opp_side = 'a' if side == 'h' else 'h'
                
                for p_id, stats in team_data.items():
                    stats['match_id'] = m_id
                    stats['team_name'] = match_map[m_id][side]
                    stats['opponent_name'] = match_map[m_id][opp_side]
                    player_match_data.append(stats)
            
            time.sleep(random.uniform(0.6, 1.2))
            
        except Exception as e:
            print(f"Error on match {m_id}: {e}")

    if player_match_data:
        df = pd.DataFrame(player_match_data)

        df['match_id'] = df['match_id'].astype(str)
        
        # Quick numeric cleanup
        for col in ['xG', 'xA', 'npxG', 'xGChain', 'xGBuildup']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # Save to the "data" folder
        file_path = os.path.join(os.getcwd(), "data", "EPL_player_match_stats.parquet")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_parquet(file_path, index=False)
        print(f"Done! Saved to {file_path}")


if __name__ == "__main__":
    get_roster()