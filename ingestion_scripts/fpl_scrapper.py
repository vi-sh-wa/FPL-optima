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

def get_player_history(player_map, season_label, rounds_to_fetch):
    all_new_data = []
    
    # tqdm will now show progress for the whole backfill process
    for p_id, p_info in tqdm(player_map.items(), desc="Backfilling FPL Data"):
        try:
            url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if 'history' in data:
                    for entry in data['history']:
                        # Check if this entry's round is in our "to-fetch" list
                        if entry['round'] in rounds_to_fetch:
                            entry.update(p_info)
                            entry['season'] = season_label
                            all_new_data.append(entry)
            time.sleep(0.05) 
        except Exception as e:
            print(f"Error on player {p_id}: {e}")
            
    return pd.DataFrame(all_new_data)


def get_player_mappings(boot_data):
    # This is exactly your "Part D" logic moved into its own house
    team_map = {t['id']: t['name'] for t in boot_data['teams']}
    pos_map = {1: 'GKP', 2: 'DEF', 3: 'MID', 4: 'FWD'}
    
    player_map = {
        p['id']: {
            'name': f"{p['first_name']}_{p['second_name']}",
            'team': team_map.get(p['team']),
            'position': pos_map.get(p['element_type']),
            'xP': float(p['ep_next'] or 0),
            'code': str(p['code']) 
        } for p in boot_data['elements']
    }
    return player_map