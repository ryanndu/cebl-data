import pandas as pd
import requests
import re
from datetime import datetime

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_schedule_data import extract_cebl_schedule
from extract_game_data import *
from helpers import *
from upload_to_releases import upload_to_releases

from update_game_data import update_pbp_data, update_officials_data, update_coaches_data, update_players_data, update_team_data
from update_schedule_data import update_schedule_data


def update_data():
    """
    Runs all the update functions sequentially.

    Returns
    -------
    None
    """
    update_schedule_data()
    update_pbp_data()
    update_officials_data()
    update_coaches_data()
    update_players_data()
    update_team_data()

update_data()