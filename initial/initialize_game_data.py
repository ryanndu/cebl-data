import pandas as pd
import requests
import re
import janitor

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_game_data import *
from extract_schedule_data import extract_cebl_schedule
from helpers import *
from upload_to_releases import upload_to_releases

def initialize_player_data():
    """
    Initializes and stores player boxscore data from all games in the schedule.

    Returns
    -------
    None
    """
    players = pd.DataFrame()
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            players = pd.concat([players, extract_player_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    players = clean_player_data(players)
    players.to_csv('cebl_players.csv', index=False)
    upload_to_releases('cebl_players.csv', 'player-boxscore')

def initialize_team_data():
    """
    Initializes and stores team boxscore data from all games in the schedule.

    Returns
    -------
    None
    """
    teams = pd.DataFrame()
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            teams = pd.concat([teams, extract_team_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    teams = clean_team_data(teams)
    teams.to_csv('cebl_teams.csv', index=False)
    upload_to_releases('cebl_teams.csv', 'team-boxscore')

def initialize_coach_data():
    """
    Initializes and stores coaches data from all games in the schedule.

    Returns
    -------
    None
    """
    coaches = pd.DataFrame()
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            coaches = pd.concat([coaches, extract_coach_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    coaches = clean_coach_data(coaches)
    coaches.to_csv('cebl_coaches.csv', index=False)
    upload_to_releases('cebl_coaches.csv', 'coaches')

def initialize_officials_data():
    """
    Initializes and stores officials data from all games in the schedule.

    Returns
    -------
    None
    """
    officials = pd.DataFrame()
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            officials = pd.concat([officials, extract_officials_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    officials = clean_officials_data(officials)
    officials.to_csv('cebl_officials.csv', index=False)
    upload_to_releases('cebl_officials.csv', 'officials')

def initialize_officials_data_2019():
    """
    Initializes and stores officials data from all games in the schedule during 2019.

    Returns
    -------
    None
    """
    officials = pd.DataFrame()
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    schedule = schedule.query('season == 2019')
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            officials = pd.concat([officials, extract_officials_data_2019(json_data)])
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    officials.to_csv('cebl_officials_2019.csv', index=False)
    upload_to_releases('cebl_officials_2019.csv', 'officials') # The 2019 version got deleted after since data got combined into one csv


def initialize_officials_data_all():
    """
    Initializes and stores officials data by combining the 2019 officials data with the rest of the officials data.

    Returns
    -------
    None
    """
    all_officials = pd.concat([pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/officials/cebl_officials.csv'), 
                               pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/officials/cebl_officials_2019.csv')], ignore_index=True)
    all_officials.to_csv('cebl_officials.csv', index=False)
    upload_to_releases('cebl_officials.csv', 'officials')


def initialize_pbp_data():
    """
    Initializes and stores play by play data from all games in the schedule seperated by year.

    Returns
    -------
    None
    """
    pbp = pd.DataFrame()
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            pbp = pd.concat([pbp, extract_pbp_data(json_data)], ignore_index=True)
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    pbp = clean_pbp_data(pbp)
    for season, group_df in pbp.groupby('season'):
        file_name = 'cebl_pbp_' + str(season) + '.csv'
        group_df.to_csv(file_name, index=False)
        upload_to_releases(file_name, 'pbp')


def initialize_pbp_data_2019():
    """
    Initializes and stores play by play data from all games in the schedule during 2019.

    Returns
    -------
    None
    """
    pbp = pd.read_csv('cebl_pbp_2019.csv')
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    schedule = schedule.query('season == 2019')
    new_pbp = pd.DataFrame()
    for _, row in schedule.iterrows():
        json_url = row['fiba_json_url']
        game_id = re.search(r'/data/(\d+)/data\.json', json_url).group(1)
        season = row['season']
        try:
            json_data = requests.get(json_url).json()
            json_data['game_id'] = game_id
            json_data['season'] = season
            new_pbp = pd.concat([new_pbp, extract_pbp_data_2019(json_data)], ignore_index=True)
        except Exception as e:
            print(f'Error for game_id {game_id}: {e}')
            continue
    new_pbp = clean_pbp_data(new_pbp)
    pbp = pd.concat([pbp, new_pbp], ignore_index=True)
    pbp.to_csv('cebl_pbp_2019.csv', index=False)
    upload_to_releases('cebl_pbp_2019.csv', 'pbp')
