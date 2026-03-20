import requests
import time
from tqdm import tqdm
import pandas as pd
import os

def get_fpl_metadata():
    boot_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    r = requests.get(boot_url)
    if r.status_code != 200:
        raise Exception("Failed to reach FPL API")
    return r.json()


def get_player_history(player_map, season_label, rounds_to_fetch):
    all_new_data = []

    for p_id, p_info in tqdm(player_map.items(), desc="Backfilling FPL Data"):
        try:
            url = f"https://fantasy.premierleague.com/api/element-summary/{p_id}/"
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if 'history' in data:
                    for entry in data['history']:
                        if entry['round'] in rounds_to_fetch:
                            entry.update(p_info)
                            entry['season'] = season_label
                            all_new_data.append(entry)
            time.sleep(0.05) 
        except Exception as e:
            print(f"Error on player {p_id}: {e}")
            
    return pd.DataFrame(all_new_data)


def get_player_mappings(boot_data):
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
    return player_map, team_map


def sync_fpl_fixtures(team_map, season_label):
    fixture_url = "https://fantasy.premierleague.com/api/fixtures/"
    print("Fetching fixtures from FPL API...")
    response = requests.get(fixture_url)
    fixtures_data = response.json()

    clean_fixtures = []
    fix_id_map = {}

    for fixture in fixtures_data:
        h_id = fixture['team_h']
        a_id = fixture['team_a']
        fix_id_map[fixture['id']] = (h_id, a_id)

        if not fixture.get('stats'):
            continue
            
        h_name = team_map.get(h_id, "Unknown")
        a_name = team_map.get(a_id, "Unknown")

        for stat_item in fixture['stats']:
            if stat_item['identifier'] == 'bps':
                away_bps_total = sum(player['value'] for player in stat_item["a"])
                home_bps_total = sum(player['value'] for player in stat_item["h"])

                clean_fixtures.append({
                    'fixture_id': fixture['id'],
                    'round': fixture['event'],
                    'home_team': h_name,
                    'away_team': a_name,
                    'h_bps': home_bps_total,
                    'a_bps': away_bps_total,
                    'h_score': fixture['team_h_score'],
                    'a_score': fixture['team_a_score'],
                    'kickoff_time': fixture['kickoff_time'],
                    'season' : season_label,
                })
                break
    
    df = pd.DataFrame(clean_fixtures)
    if not df.empty and 'kickoff_time' in df.columns:
        df['kickoff_time'] = pd.to_datetime(df['kickoff_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return df, fix_id_map

def update_fixture_table(df_new, file_path):
    if os.path.exists(file_path):
        df_old = pd.read_parquet(file_path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['fixture_id'], keep='last')
    else:
        df_combined = df_new

    df_combined.to_parquet(file_path, index=False)
    print(f"Fixture table updated at {file_path}")


def fix_player_teams(df, fix_id_map, team_map):
    def resolve_team(row):
        h_id, a_id = fix_id_map.get(row['fixture'], (None, None))
        p_team_id = h_id if row['was_home'] else a_id
        opp_team_id = a_id if row['was_home'] else h_id
        
        return pd.Series([
            team_map.get(p_team_id, "Unknown"), 
            team_map.get(opp_team_id, "Unknown")
        ])

    df[['team', 'opponent_team']] = df.apply(resolve_team, axis=1)
    return df


