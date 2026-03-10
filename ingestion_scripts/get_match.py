import os
import pandas as pd

def get_match(completed_matches_set, understat_client):
    match_list = [] 
    seasons = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

    for season in seasons:
        data = understat_client.league(league="EPL").get_match_data(season=season)
        for matches in data:
            m_id = str(matches['id'])
            
            # Check against the set passed into the function
            if str(matches['isResult']).lower() == 'true' and m_id not in completed_matches_set:
                match_list.append({
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
    return match_list 