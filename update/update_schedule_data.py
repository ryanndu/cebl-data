import pandas as pd
import requests
import re
from datetime import datetime

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_schedule_data import extract_cebl_schedule
from helpers import *
from upload_to_releases import upload_to_releases

def update_schedule_data():
    """
    Updates the schedule data with new completed games for the current season.
    """
    year = datetime.now().year
    schedule = pd.read_csv('https://github.com/ryanndu/cebl-data/releases/download/schedule/cebl_schedule.csv')
    current_schedule = extract_cebl_schedule(year)
    current_schedule = current_schedule[~current_schedule['fiba_id'].isin(schedule['fiba_id'])]
    current_schedule = current_schedule.query("status == 'COMPLETE'")
    if not current_schedule.empty:
        current_schedule = clean_schedule_data(current_schedule)

    full_schedule = pd.concat([schedule, current_schedule], ignore_index=True)
    full_schedule = clean_schedule_data(full_schedule)
    full_schedule.to_csv('cebl_schedule.csv', index=False)
    upload_to_releases('cebl_schedule.csv', 'schedule')