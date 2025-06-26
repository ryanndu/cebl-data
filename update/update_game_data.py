import pandas as pd
from datetime import datetime
import re
import requests

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))

from extract_schedule_data import extract_cebl_schedule
from extract_game_data import *
from helpers import *

def update_pbp_data():
    current_year = datetime.now().year
    current_schedule = pd.read_csv('cebl_schedule.csv').query("season == @current_year")
    try:
        pbp = pd.read_csv('cebl_pbp_' + str(current_year) + '.csv')
        current_schedule = current_schedule[~current_schedule['fiba_id'].isin(pbp['game_id'])]
    except Exception as e:
        pbp = pd.DataFrame()

    new_pbp = pd.DataFrame()
    for _, row in current_schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            new_pbp = pd.concat([new_pbp, extract_pbp_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f"Error for game_id {game_id}: {e}")
            continue
    if not new_pbp.empty:
        new_pbp = clean_pbp_data(new_pbp)
    all_pbp = pd.concat([pbp, new_pbp], ignore_index=True)
    all_pbp.to_csv('cebl_pbp_' + str(current_year) + '.csv', index=False)

update_pbp_data()

def update_officials_data():
    officials = pd.read_csv('cebl_all_officials.csv')
    current_year = datetime.now().year
    current_schedule = pd.read_csv('cebl_schedule.csv').query("season == @current_year")
    current_schedule = current_schedule[~current_schedule['fiba_id'].isin(officials['game_id'])]

    new_officials = pd.DataFrame()
    for _, row in current_schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            new_officials = pd.concat([new_officials, extract_officials_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f"Error for game_id {game_id}: {e}")
            continue
    if not new_officials.empty:
        new_officials = clean_officials_data(new_officials)
    all_officials = pd.concat([officials, new_officials], ignore_index=True)
    all_officials.to_csv('cebl_all_officials.csv', index=False) # I THINK I NEED TO FIGURE OUT THE NAMING FOR OFFICIALS AND PBP WITH THE CSV FILES

# update_officials_data()


def update_coaches_data():
    coaches = pd.read_csv('cebl_coaches.csv')
    current_year = datetime.now().year
    current_schedule = pd.read_csv('cebl_schedule.csv').query("season == @current_year")
    current_schedule = current_schedule[~current_schedule['fiba_id'].isin(coaches['game_id'])]

    new_coaches = pd.DataFrame()
    for _, row in current_schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            new_coaches = pd.concat([new_coaches, extract_coach_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f"Error for game_id {game_id}: {e}")
            continue
    
    if not new_coaches.empty:
        new_coaches = clean_coach_data(new_coaches)
    
    all_coaches = pd.concat([coaches, new_coaches], ignore_index=True)
    all_coaches.to_csv('cebl_coaches.csv', index=False)

# update_coaches_data()

def update_players_data():
    players = pd.read_csv('cebl_players.csv')
    current_year = datetime.now().year
    current_schedule = pd.read_csv('cebl_schedule.csv').query("season == @current_year")
    current_schedule = current_schedule[~current_schedule['fiba_id'].isin(players['game_id'])]

    new_players = pd.DataFrame()
    for _, row in current_schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            new_players = pd.concat([new_players, extract_player_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f"Error for game_id {game_id}: {e}")
            continue

    if not new_players.empty:
        new_players = clean_player_data(new_players)
    
    all_players = pd.concat([players, new_players], ignore_index=True)
    all_players.to_csv('cebl_players.csv', index=False)

# update_players_data()

def update_team_data():
    teams = pd.read_csv('cebl_teams.csv')
    current_year = datetime.now().year
    current_schedule = pd.read_csv('cebl_schedule.csv').query("season == @current_year")
    current_schedule = current_schedule[~current_schedule['fiba_id'].isin(teams['game_id'])]

    new_teams = pd.DataFrame()
    for _, row in current_schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            new_teams = pd.concat([new_teams, extract_team_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f"Error for game_id {game_id}: {e}")
            continue
    if not new_teams.empty:
        new_teams = clean_team_data(new_teams)
    all_teams = pd.concat([teams, new_teams], ignore_index=True)
    all_teams.to_csv('cebl_teams.csv', index=False)

# update_team_data()