import requests
import time
from tqdm import tqdm
import pandas as pd

def get_fpl_metadata():
    boot_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    r = requests.get(boot_url)
    if r.status_code != 200:
        raise Exception("Failed to reach FPL API")
    return r.json()

def get_player_history(player_map, season_label):
    curr_gameweek_data = []
    
    for p_id in tqdm(player_map.keys(), desc=f"Fetching FPL Data ({season_label})"):
        try:
            url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if 'history' in data:
                    for entry in data['history']:
                        entry.update(player_map[p_id])
                        entry['season'] = season_label # Uses the "2025-26" format
                        curr_gameweek_data.append(entry)
            time.sleep(0.4) 
        except Exception as e:
            print(f"Error on player {p_id}: {e}")
            
    return pd.DataFrame(curr_gameweek_data)