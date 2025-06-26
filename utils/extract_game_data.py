import pandas as pd
import requests
import re
import janitor
import helpers as h


def extract_player_data(json):
    """
    Extract player data from game JSON.

    Parameters
    ----------
    json : dict
        The JSON response containing the player data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the player data for a specific game
    """
    players = pd.DataFrame()
    for team_num in ['1', '2']:
        new_players = pd.json_normalize(json['tm'][team_num]['pl'].values()).clean_names(case_type='snake')
        new_players['team_name'] = json['tm'][team_num]['name']
        players = pd.concat([players, new_players], ignore_index=True)

    players['game_id'] = json['game_id']
    players['season'] = json['season']

    return players


def extract_officials_data(json):
    """
    Extract officials data from game JSON.

    Parameters
    ----------
    json : dict
        The JSON response containing the officials data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the officials data for a specific game
    """
    officials_list = []
    for officials_type, officials_data in json['officials'].items():
        officials_data['officials_type'] = officials_type
        officials_list.append(officials_data)

    officials = pd.json_normalize(officials_list).clean_names(case_type='snake')
    officials['game_id'] = json['game_id']
    officials['season'] = json['season']

    return officials


def extract_officials_data_2019(json):
    """
    Extract officials data from a 2019 game where the JSON 
    structure differs from the standard format used in later seasons.

    Parameters
    ----------
    json : dict
        The JSON response containing the officials data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the officials data for a specific game
    """
    officials = pd.DataFrame()
    officials_types = ['officials_commisioner', 'officials_referee1',
                       'officials_referee2', 'officials_referee3']
    for types in officials_types:
        if types not in json:
            continue
        game_id = json['game_id']
        season = json['season']
        officials_type = types.split("_")[1]
        officials_name = json[types]
        name_info = officials_name.strip().split()
        first_name = name_info[0]
        last_name_index = len(name_info) - 1
        last_name = name_info[last_name_index]

        new_officials = pd.DataFrame([{
            'game_id': game_id, 'season': season, 'officials_type': officials_type, 
            'officials_name': officials_name, 'first_name': first_name, 'last_name': last_name,
            'scoreboard_name': first_name[0] + '. ' + last_name, 'first_name_initial': first_name[0],
            'last_name_initial': last_name[0], 'international_first_name': first_name, 
            'international_first_name_initial': first_name[0], 'international_last_name': last_name,
            'international_last_name_initial': last_name[0]
        }])
        
        officials = pd.concat([officials, new_officials], ignore_index=True)

    return officials


def extract_team_data(json):
    """
    Extract team data from game JSON.

    Parameters
    ----------
    json : dict
        The JSON response containing the team data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the team data for a specific game
    """
    team_one_data = json['tm']['1']
    team_two_data = json['tm']['2']

    team_one = pd.json_normalize(team_one_data)
    team_two = pd.json_normalize(team_two_data)
    teams = pd.concat([team_one, team_two], ignore_index=True)

    teams['game_id'] = json['game_id']
    teams['season'] = json['season']

    return teams


def extract_coach_data(json):
    """
    Extract coach data from game JSON.

    Parameters
    ----------
    json : dict
        The JSON response containing the coach data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the coach data for a specific game
    """
    coaches = pd.DataFrame()

    for team_num in ['1', '2']:
        team_data = json['tm'][team_num]
        team_name = team_data['name']
        
        coaches_data = [
            ('coachDetails', 'Head Coach'),
            ('assistcoach1Details', 'Assistant Coach'),
            ('assistcoach2Details', 'Assistant Coach')
        ]

        for key, coach_type in coaches_data:
            if key in team_data and team_data[key] is not None:
                coach_record = pd.json_normalize(team_data[key])
                coach_record['team_name'] = team_name
                coach_record['coach_type'] = coach_type
                coaches = pd.concat([coaches, coach_record], ignore_index=True)

    if coaches.empty:
        return coaches
    
    coaches['game_id'] = json['game_id']
    coaches['season'] = json['season']
    
    return coaches


def extract_pbp_data(json):
    """
    Extract play-by-play and shot data from game JSON.

    Parameters
    ----------
    json : dict
        The JSON response containing the play-by-play and shot data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the play-by-play data with shot data added,
        when applicable, for a specific game
    """
    pbp = pd.json_normalize(json['pbp']).clean_names(case_type='snake')
    pbp = pbp.drop(columns= ['scoreboard_name']).rename(columns={'player' : 'scoreboard_name'})
    pbp['player_name'] = pbp['first_name'] + ' ' + pbp['family_name']
    pbp['game_id'] = json['game_id']
    pbp['season'] = json['season']

    qualifiers = pd.DataFrame(pbp['qualifier'].tolist()).add_prefix('qualifier_')
    pbp = pd.concat([pbp.drop(columns=['qualifier']), qualifiers], axis=1)

    shot_team1 = pd.json_normalize(json['tm']['1']['shot']).clean_names(case_type='snake')
    shot_team2 = pd.json_normalize(json['tm']['2']['shot']).clean_names(case_type='snake')
    shots = pd.concat([shot_team1, shot_team2], ignore_index=True)

    pbp = pbp.merge(
        shots[['action_number', 'x', 'y']],
        on='action_number',
        how='left'
    )

    return pbp


def extract_pbp_data_2019(json):
    """
    Extract pbp data from a 2019 game where the JSON 
    structure differs from the standard format used in later seasons.

    Parameters
    ----------
    json : dict
        The JSON response containing the play-by-play and shot data.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the play-by-play data with shot data added,
        when applicable, for a specific game
    """
    pbp = pd.json_normalize(json['pbp']).clean_names(case_type='snake')
    pbp['shot_number_1'] = pd.NA
    pbp['shot_number_2'] = pd.NA
    pbp = pbp.drop(columns= ['scoreboard_name']).rename(columns={'player' : 'scoreboard_name'})
    pbp['player_name'] = pbp['first_name'] + ' ' + pbp['family_name']
    pbp['game_id'] = json['game_id']
    pbp['season'] = json['season']

    team1_shots = (pbp['tno'] == 1) & pbp['action_type'].str.contains('pt', na=False)
    team2_shots = (pbp['tno'] == 2) & pbp['action_type'].str.contains('pt', na=False)

    pbp.loc[team1_shots, 'shot_number_1'] = range(team1_shots.sum())
    pbp.loc[team2_shots, 'shot_number_2'] = range(team2_shots.sum())

    shot_team1 = pd.json_normalize(json['tm']['1']['shot']).clean_names(case_type='snake')
    shot_team2 = pd.json_normalize(json['tm']['2']['shot']).clean_names(case_type='snake')

    shot_team1_reversed = shot_team1.iloc[::-1].reset_index(drop=True)
    shot_team2_reversed = shot_team2.iloc[::-1].reset_index(drop=True)
    shot_team1_reversed['shot_number'] = range(len(shot_team1_reversed))
    shot_team2_reversed['shot_number'] = range(len(shot_team2_reversed))

    pbp = pbp.merge(
        shot_team1_reversed[['shot_number', 'x', 'y']], 
        left_on='shot_number_1', 
        right_on='shot_number', 
        how='left'
    ).drop('shot_number', axis=1)

    pbp = pbp.merge(
        shot_team2_reversed[['shot_number', 'x', 'y']], 
        left_on='shot_number_2', 
        right_on='shot_number', 
        how='left', 
        suffixes=('_team1', '_team2')
    ).drop('shot_number', axis=1)

    pbp['x'] = pbp['x_team1'].fillna(pbp['x_team2'])
    pbp['y'] = pbp['y_team1'].fillna(pbp['y_team2'])

    pbp = pbp.drop(['shot_number_1', 'shot_number_2', 'x_team1', 'y_team1', 'x_team2', 'y_team2'], axis=1)

    return pbp