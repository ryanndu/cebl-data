import pandas as pd


def normalize_time(time_str):
    """
    Normalizes a time string by converting any excess minutes into hours.

    Parameters
    ----------
    time_str : str
        A time string in the format "H:MM" or "HH:MM".

    Returns
    -------
    str
        A normalized time string in the format "H:MM", where minutes is less than 60.
    """
    hours, minutes = map(int, time_str.split(":"))
    extra_hours, minutes = divmod(minutes, 60)
    hours += extra_hours
    return f"{hours}:{minutes:02d}"


def clean_schedule_data(schedule):
    """
    Clean a schedule DataFrame by rearranging the column order.

    Parameters
    ----------
    schedule : pd.DataFrame
        The DataFrame containing raw schedule data.
    
    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame of schedule with reordered columns.
    """
    columns = [
        'fiba_id', 'season', 'start_time_utc', 'status', 'competition', 'venue_name', 'period', 'home_team_id',
        'home_team_name', 'home_team_score', 'home_team_logo_url', 'home_team_stats_url_en', 
        'home_team_stats_url_fr', 'away_team_id', 'away_team_name', 'away_team_score', 'away_team_logo_url',
        'away_team_stats_url_en', 'away_team_stats_url_fr', 'stats_url_en', 'stats_url_fr', 
        'cebl_stats_url_en', 'cebl_stats_url_fr', 'tickets_url_en', 'tickets_url_fr', 'id', 'fiba_json_url'
    ]
    return schedule[columns]


def clean_player_data(players):
    """
    Clean a player DataFrame by renaming, rearranging, and creating new columns.

    Parameters
    ----------
    players : pd.DataFrame
        The DataFrame containing raw player data.
    
    Returns
    -------
    pd.DataFrame
        A cleaned player dataframe with standardized column names and structure.
        The player's name was added and column data was changed from na and 1.0
        to true and false.
    """
    column_mapping = {
        's_minutes': 'minutes',
        's_field_goals_made': 'field_goals_made',
        's_field_goals_attempted': 'field_goals_attempted',
        's_field_goals_percentage': 'field_goal_percentage',
        's_three_pointers_made': 'three_point_field_goals_made',
        's_three_pointers_attempted': 'three_point_field_goals_attempted',
        's_three_pointers_percentage': 'three_point_percentage',
        's_two_pointers_made': 'two_point_field_goals_made',
        's_two_pointers_attempted': 'two_point_field_goals_attempted',
        's_two_pointers_percentage': 'two_point_percentage',
        's_free_throws_made': 'free_throws_made',
        's_free_throws_attempted': 'free_throws_attempted',
        's_free_throws_percentage': 'free_throw_percentage',
        's_rebounds_defensive': 'defensive_rebounds',
        's_rebounds_offensive': 'offensive_rebounds',
        's_rebounds_total': 'rebounds',
        's_assists': 'assists',
        's_turnovers': 'turnovers',
        's_steals': 'steals',
        's_blocks': 'blocks',
        's_blocks_received': 'blocks_received',
        's_fouls_personal': 'personal_fouls',
        's_fouls_on': 'fouls_drawn',
        's_points': 'points',
        's_points_second_chance': 'second_chance_points',
        's_points_fast_break': 'fast_break_points',
        's_plus_minus_points': 'plus_minus',
        's_points_in_the_paint': 'points_in_the_paint',
        'playing_position': 'position',
        'shirt_number': 'player_number',
        'family_name': 'last_name',
        'family_name_initial': 'last_name_initial',
        'international_family_name': 'international_last_name',
        'international_family_name_initial': 'international_last_name_initial',
        'eff_1': 'index_rating',
        'eff_2': 'index_rating_2',
        'eff_3': 'index_rating_3',
        'eff_4': 'index_rating_4',
        'eff_5': 'index_rating_5',
        'eff_6': 'index_rating_6',
        'eff_7': 'index_rating_7',
    }
    players = players.rename(columns=column_mapping)

    players['player_name'] = players['first_name'] + ' ' + players['last_name']
    players['captain'] = players['captain'].fillna(False)
    players['captain'] = players['captain'].replace({'False': False, '1.0': True})
    players['captain'] = players['captain'].astype(bool)
    players['active'] = players['active'].astype(bool)
    players['starter'] = players['starter'].astype(bool)  
    
    columns = [
        'game_id', 'season', 'team_name', 'player_number', 'player_name', 'position', 'minutes', 'points', 'field_goals_made', 
        'field_goals_attempted', 'field_goal_percentage', 'two_point_field_goals_made', 'two_point_field_goals_attempted', 
        'two_point_percentage', 'three_point_field_goals_made', 'three_point_field_goals_attempted', 'three_point_percentage', 
        'free_throws_made', 'free_throws_attempted', 'free_throw_percentage', 'offensive_rebounds', 'defensive_rebounds', 
        'rebounds', 'assists', 'turnovers', 'steals', 'blocks', 'blocks_received', 'personal_fouls', 'fouls_drawn', 'plus_minus',
        'index_rating', 'index_rating_2', 'index_rating_3', 'index_rating_4', 'index_rating_5', 'index_rating_6', 'index_rating_7',
        'second_chance_points', 'fast_break_points', 'points_in_the_paint', 'first_name', 'first_name_initial', 'last_name', 
        'last_name_initial', 'international_first_name', 'international_first_name_initial', 'international_last_name',
        'international_last_name_initial', 'scoreboard_name', 'active', 'starter', 'captain', 'photo_t', 'photo_s'
    ]
    return players[columns]


def clean_officials_data(officials):
    """
    Clean a officials DataFrame by renaming specific columns, stripping whitespace,
    filtering out empty names, and rearranging the column order.

    Parameters
    ----------
    officials : pd.DataFrame
        The DataFrame containing raw officials data.
    
    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame of officials with standardized column names and structure.
        Rows with empty 'officials_name' are excluded, and all string columns are stripped
        of leading/trailing whitespace.
    """
    column_mapping = {
        'family_name': 'last_name',
        'name': 'officials_name',
        'family_name_initial': 'last_name_initial',
        'international_family_name': 'international_last_name',
        'international_family_name_initial': 'international_last_name_initial',
    }
    officials = officials.rename(columns=column_mapping)

    columns = [
        'game_id', 'season', 'officials_type', 'officials_name', 'first_name', 
        'last_name', 'scoreboard_name', 'first_name_initial', 'last_name_initial',
        'international_first_name', 'international_first_name_initial',
        'international_last_name', 'international_last_name_initial'
    ]
    officials = officials.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    officials = officials.query("officials_name != ''")
    return officials[columns]


def clean_team_data(teams):
    """
    Cleans and standardizes a basketball team DataFrame by ensuring required columns exist,
    renaming fields, normalizing time values, and fomatting structure.

    Parameters
    ----------
    teams : pd.DataFrame
        The DataFrame containing raw teams data

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with standardized column names, complete structure, and
        normalized time and lead values.
    """
    teams = teams.clean_names(case_type="snake")

    columns = [
        'game_id', 'season', 'name', 'short_name', 'code', 'score', 'tot_s_minutes', 'tot_s_field_goals_made', 'tot_s_field_goals_attempted',
        'tot_s_field_goals_percentage', 'tot_s_two_pointers_made', 'tot_s_two_pointers_attempted', 'tot_s_two_pointers_percentage',
        'tot_s_three_pointers_made', 'tot_s_three_pointers_attempted', 'tot_s_three_pointers_percentage', 'tot_s_free_throws_made',
        'tot_s_free_throws_attempted', 'tot_s_free_throws_percentage', 'tot_s_rebounds_offensive', 'tot_s_rebounds_defensive',
        'tot_s_rebounds_total', 'tot_s_assists', 'tot_s_steals', 'tot_s_turnovers', 'tot_s_blocks', 'tot_s_blocks_received',
        'tot_s_fouls_personal', 'tot_s_fouls_on', 'tot_s_fouls_total', 'fouls', 'tot_s_points_in_the_paint', 'tot_s_points_second_chance',
        'tot_s_points_from_turnovers', 'tot_s_bench_points', 'tot_s_points_fast_break', 'tot_eff_1', 'tot_eff_2', 'tot_eff_3',
        'tot_eff_4', 'tot_eff_5', 'tot_eff_6', 'tot_eff_7', 'tot_s_fouls_team', 'tot_s_turnovers_team', 'tot_s_rebounds_team',
        'tot_s_rebounds_team_defensive', 'tot_s_rebounds_team_offensive', 'p1_score', 'p2_score', 'p3_score', 'p4_score', 'tot_s_biggest_lead',
        'tot_s_biggest_scoring_run', 'tot_s_time_leading', 'tot_s_lead_changes', 'tot_s_times_scores_level', 'timeouts', 'coach',
        'assistcoach1', 'assistcoach2', 'name_international', 'short_name_international', 'code_international', 'logo', 'logo_t_url',
        'logo_t_size', 'logo_t_height', 'logo_t_width', 'logo_t_bytes', 'logo_s_url', 'logo_s_size', 'logo_s_height', 'logo_s_width', 
        'logo_s_bytes',
    ]

    for column in columns:
        if column not in teams:
            teams[column] = pd.NA

    teams = teams[columns]

    column_mapping = {
        'tot_s_field_goals_made': 'field_goals_made',
        'tot_s_field_goals_attempted': 'field_goals_attempted',
        'tot_s_field_goals_percentage': 'field_goal_percentage',
        'tot_s_three_pointers_made': 'three_point_field_goals_made',
        'tot_s_three_pointers_attempted': 'three_point_field_goals_attempted',
        'tot_s_three_pointers_percentage': 'three_point_percentage',
        'tot_s_two_pointers_made': 'two_point_field_goals_made',
        'tot_s_two_pointers_attempted': 'two_point_field_goals_attempted',
        'tot_s_two_pointers_percentage': 'two_point_percentage',
        'tot_s_free_throws_made': 'free_throws_made',
        'tot_s_free_throws_attempted': 'free_throws_attempted',
        'tot_s_free_throws_percentage': 'free_throw_percentage',
        'tot_s_rebounds_defensive': 'defensive_rebounds',
        'tot_s_rebounds_offensive': 'offensive_rebounds',
        'tot_s_rebounds_total': 'rebounds',
        'tot_s_assists': 'assists',
        'tot_s_turnovers': 'turnovers',
        'tot_s_steals': 'steals',
        'tot_s_blocks': 'blocks',
        'tot_s_blocks_received': 'blocks_received',
        'tot_s_fouls_personal': 'personal_fouls',
        'tot_s_fouls_on': 'fouls_drawn',
        'tot_s_fouls_total': 'total_fouls',
        'tot_s_points_from_turnovers': 'points_from_turnovers',
        'tot_s_points_second_chance': 'second_chance_points',
        'tot_s_points_fast_break': 'fast_break_points',
        'tot_s_bench_points': 'bench_points',
        'tot_s_points_in_the_paint': 'points_in_the_paint',
        'tot_s_time_leading': 'time_leading',
        'tot_s_biggest_lead': 'biggest_lead',
        'tot_s_biggest_scoring_run': 'biggest_scoring_run',
        'tot_s_lead_changes': 'lead_changes',
        'tot_s_times_scores_level': 'times_scores_level',
        'tot_s_fouls_team': 'team_fouls',
        'tot_s_rebounds_team': 'team_rebounds',
        'tot_s_rebounds_team_defensive': 'team_defensive_rebounds',
        'tot_s_rebounds_team_offensive': 'team_offensive_rebounds',
        'tot_s_turnovers_team': 'team_turnovers',
        'tot_eff_1': 'team_index_rating',
        'tot_eff_2': 'team_index_rating_2',
        'tot_eff_3': 'team_index_rating_3',
        'tot_eff_4': 'team_index_rating_4',
        'tot_eff_5': 'team_index_rating_5',
        'tot_eff_6': 'team_index_rating_6',
        'tot_eff_7': 'team_index_rating_7',
        'name': 'team_name',
        'tot_s_minutes': 'minutes',
        'score': 'team_score',
        'assistcoach1': 'assistant_coach_1',
        'assistcoach2': 'assistant_coach_2',
        'coach': 'head_coach',
        'name_international': 'international_team_name',
        'short_name_international': 'international_short_name',
        'code_international': 'international_code',
        'p1_score': 'period_1_score',
        'p2_score': 'period_2_score',
        'p3_score': 'period_3_score',
        'p4_score': 'period_4_score',
        'fouls': 'bonus_fouls',
        'timeouts': 'timeouts_left',
    }
    teams = teams.rename(columns=column_mapping)
    teams['minutes'] = teams['minutes'].apply(normalize_time)
    teams['biggest_lead'] = teams['biggest_lead'].fillna(0)

    return teams


def clean_coach_data(coaches):
    """
    Cleans a basketball coach DataFrame by standardizing column names, creating
    full coach names, and selecting a consistent column order.

    Parameters
    ----------
    coaches : pd.DataFrame
        The DataFrame containing raw coaches data.

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame including full coach names, renamed fields, and ordered columns.
    """
    coaches = coaches.clean_names(case_type='snake')

    column_mapping = {
        'family_name': 'last_name',
        'family_name_initial': 'last_name_initial',
        'international_family_name': 'international_last_name',
        'international_family_name_initial': 'international_last_name_initial',
    }
    coaches = coaches.rename(columns=column_mapping)
    coaches['coach_name'] = coaches['first_name'] + ' ' + coaches['last_name']
    
    columns = [
        'game_id', 'season', 'team_name', 'coach_name', 'coach_type',
        'first_name', 'first_name_initial', 'last_name', 'last_name_initial',
        'international_first_name', 'international_first_name_initial', 
        'international_last_name', 'international_last_name_initial',
        'scoreboard_name',
    ]
    return coaches[columns]


def clean_pbp_data(pbp):
    """
    Cleans a basketball play-by-play DataFrame by replacing empty values,
    renaming columns, and reordering with all known qualifiers.

    Parameters
    ----------
    pbp : pd.DataFrame
        The DataFrame containing raw pbp data

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with renamed fields, structured column order, and NA-consistent entries.
    """
    pbp = pbp.fillna(pd.NA).replace({None: pd.NA, "": pd.NA})
    
    column_mapping = {
        'gt': 'game_time',
        's1': 'home_score',
        's2': 'away_score',
        'lead': 'home_lead',
        'tno': 'team_id',
        'pno': 'player_id',
        'family_name': 'last_name',
        'international_family_name': 'international_last_name',
        'family_name_initial': 'last_name_initial',
        'international_family_name_initial': 'international_last_name_initial'
    }
    pbp = pbp.rename(columns=column_mapping)

    # pbp['success'] = pbp['success'].replace({1 : True, 0: False})
    # pbp['scoring'] = pbp['scoring'].replace({1 : True, 0: False})

    column_start = [
        'game_id', 'season', 'game_time', 'home_score', 'away_score', 'home_lead', 'team_id',
        'period', 'period_type', 'player_id', 'scoreboard_name', 'success', 
        'action_type', 'action_number', 'previous_action', 'sub_type', 'scoring',
        'shirt_number', 'player_name', 'first_name', 'last_name', 'x', 'y',
    ]
    qualifier_columns = [col for col in pbp.columns if col.startswith('qualifier_')]

    column_end = [
        'international_first_name', 'international_last_name', 'first_name_initial', 'last_name_initial', 
        'international_first_name_initial', 'international_last_name_initial'
    ]
    columns = column_start + qualifier_columns + column_end
    
    return pbp[columns]