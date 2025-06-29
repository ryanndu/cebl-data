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


def initialize_schedule_data():
    """
    Initialize and stores the schedule data with games from 2019 to the current year - 1.

    Returns 
    -------
    None
    """
    schedule = pd.DataFrame()
    year = datetime.now().year
    for year in range(2019, year):
        schedule = pd.concat([schedule, extract_cebl_schedule(year)], ignore_index=True)
    schedule = clean_schedule_data(schedule)
    schedule.to_csv('cebl_schedule.csv', index=False)
    upload_to_releases('cebl_schedule.csv', 'schedule')