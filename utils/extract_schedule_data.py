import pandas as pd
import requests
import re
import helpers as h


def extract_cebl_schedule(year):
    """
    Extracts the CEBL game schedule for a given year from JSON.

    Parameters
    ----------
    year : int 
        The season year (e.g., 2023) to retrieve the schedule for.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the schedule and metadata for that season.
    """ 
    headers = {
        "x-api-key": "800chyzv2hvur3z0ogh39cve2zok0c",
        "accept": "application/json"
    }
    schedule_url = "https://api.data.cebl.ca/games/" + str(year) + "/"
    schedule_data = requests.get(schedule_url, headers=headers).json()
    schedule_df = pd.json_normalize(schedule_data)

    schedule_df['season'] = year
    schedule_df['fiba_id'] = schedule_df['cebl_stats_url_en'].str.extract(r"id=(\d+)")
    schedule_df['fiba_json_url'] = "https://fibalivestats.dcd.shared.geniussports.com/data/" + schedule_df['fiba_id'] + "/data.json"
    schedule_df['fiba_id'] = schedule_df['fiba_id'].astype(int)
    return schedule_df