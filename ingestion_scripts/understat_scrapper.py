import time
import random
import pandas as pd
from tqdm import tqdm

def get_roster_data(understat, seasons, skip_ids):
    match_map = {}
    player_match_data = []

    # 1. Map the matches for all seasons
    for season in seasons:
        print(f"Mapping season: {season}")
        league_matches = understat.league(league="EPL").get_match_data(season=season)
        
        for m in league_matches:
            m_id = str(m['id']) # Ensure string for comparison
            if str(m['isResult']).lower() == 'true':
                match_map[m_id] = {
                    'h': m['h']['title'],
                    'a': m['a']['title'],
                    'season': season,
                    'datetime': m['datetime'] 
                }

    # 2. Filter out matches we already have
    all_match_ids = list(match_map.keys())
    matches_to_scrape = [m for m in all_match_ids if m not in skip_ids]
    
    print(f"Total matches found: {len(all_match_ids)}")
    print(f"Already indexed: {len(skip_ids)}")
    print(f"New matches to scrape: {len(matches_to_scrape)}")

    # 3. Scrape only the new ones
    for m_id in tqdm(matches_to_scrape, desc="Scraping Roster Details"):
        try:
            roster = understat.match(match=m_id).get_roster_data()
            
            for side in ['h', 'a']:
                team_data = roster.get(side, {})
                opp_side = 'a' if side == 'h' else 'h'
                
                for p_id, stats in team_data.items():
                    stats['roster_id'] = stats.pop('id')
                    stats['match_id'] = m_id
                    stats['team_name'] = match_map[m_id][side]
                    stats['opponent_name'] = match_map[m_id][opp_side]
                    stats['datetime'] = match_map[m_id]['datetime']
                    stats['season'] = match_map[m_id]['season']
                    player_match_data.append(stats)
            
            time.sleep(random.uniform(0.6, 1.2))
        except Exception as e:
            print(f"Error on match {m_id}: {e}")

    return player_match_data