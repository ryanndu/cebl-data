import pandas as pd
import requests
import re
import janitor
from datetime import datetime

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_game_data import *
from extract_schedule_data import extract_cebl_schedule
from helpers import *
from upload_to_releases import upload_to_releases

from initialize_game_data import *
from intialize_schedule_data import initialize_schedule_data


def initialize_data():
    initialize_schedule_data()
    initialize_team_data()
    initialize_coach_data()
    initialize_officials_data()
    initialize_officials_data_2019()
    initialize_officials_data_all()
    initialize_pbp_data()
    initialize_pbp_data_2019()
    initialize_player_data()

initialize_data()