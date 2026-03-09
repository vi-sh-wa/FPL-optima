import pandas as pd
import os
from understatapi import UnderstatClient
understat = UnderstatClient()

file_path = "data/EPL_match_summary.parquet"

def get_match():
    if os.path.exists(file_path):
        existing_data = pd.read_parquet(file_path, columns=['match_id'])
        completed_macthes = set(existing_data['match_id'].astype(str).tolist())
        print('Ingesting this weeks data...')
    else:
        completed_macthes = set()
        print('Starting fresh...')

    seasons = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    macth_list = []

    for season in seasons:
        data = understat.league(league="EPL").get_match_data(season=season)
        for matches in data:
            m_id = str(matches['id'])

            if str(matches['isResult']).lower() == 'true' and m_id not in completed_macthes:
                macth_list.append({
                    'match_id': m_id,
                    'home_team': matches['h']['title'],
                    'away_team': matches['a']['title'],
                    'h_goals': int(matches['goals']['h']),
                    'a_goals': int(matches['goals']['a']),
                    'h_xG': float(matches['xG']['h']),
                    'a_xG': float(matches['xG']['a']),
                    'datetime': matches['datetime'],
                    'h_win_forecast': float(matches['forecast']['w']),
                    'h_draw_forecast': float(matches['forecast']['d']),
                    'h_loss_forecast': float(matches['forecast']['l']),
                })

    if macth_list:
        epl_matches = pd.DataFrame(macth_list)
        
        if not completed_macthes:
            updated_match_info = epl_matches
        else:
            existing_match_info = pd.read_parquet(file_path)
            updated_match_info = pd.concat([existing_match_info, epl_matches], ignore_index=True)
        
        updated_match_info.to_parquet(file_path, index=False)
        print(f"Added {len(macth_list)} new matches. Total matches: {len(updated_match_info)}")
    else:
        print("No new matches found since last update.")

if __name__ == "__main__":
    get_match()