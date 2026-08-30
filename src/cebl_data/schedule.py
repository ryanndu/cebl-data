import re

import pandas as pd
import requests

from cebl_data.utils import load_packaged_config


def fetch_schedule(season: int) -> list[dict]:
    """Returns the raw schedule payload for one season.

    Args:
        season (int): The season to fetch, as a four-digit year.

    Returns:
        list[dict]: One dict per game.

    Raises:
        requests.HTTPError: If the API responds with an error status.
    """
    response = requests.get(
        f"https://api.data.cebl.ca/games/{season}/",
        headers={
            "x-api-key": "800chyzv2hvur3z0ogh39cve2zok0c",
            "accept": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def extract_fiba_id(stats_url: str) -> str:
    """Pull the fiba id out of a livestats url.

    Args:
        stats_url (str): A fibalivestats url.

    Returns:
        The fiba id as a string.

    Raises:
        ValueError: If no id can be found in the URL.
    """
    match = re.search(r"/CEBL/(\d+)", stats_url or "")
    if match is None:
        raise ValueError(f"No FIBA id found in stats URL: {stats_url!r}")
    return match.group(1)


def build_schedule(payload: list[dict], season: int) -> pd.DataFrame:
    """Turns a raw schedule payload into a dataframe.

    Adds a ``season`` column, since the payload doesn't carry one, and a
    ``fiba_game_id`` column identifying the game in Genius Sports' livestats.
    Columns are then renamed, selected and cast per ``configs/schedule.json``.
    Scores and ``period`` use nullable integers because cancelled games leave
    them empty.


    Args:
        payload (list[dict]): A raw schedule payload.
        season (int): The season the payload was fetched for.

    Returns:
        pd.DataFrame: One row per game.

    Raises:
        ValueError: If the payload contains no games.
        KeyError: If the payload is missing a column the config expects.
    """
    if not payload:
        raise ValueError(f"No games returned for season {season}")

    config = load_packaged_config("schedule.json")

    schedule = pd.DataFrame(payload)
    schedule["season"] = season
    schedule["fiba_game_id"] = schedule["stats_url_en"].map(extract_fiba_id)
    schedule = schedule.rename(columns=config["rename"])
    return schedule[list(config["dtypes"])].astype(config["dtypes"])


def get_schedule(seasons: list[int]) -> pd.DataFrame:
    """Fetches and builds the schedule for one or more seasons.

    Args:
        seasons (list[int]): The seasons to fetch, as four-digit years.

    Returns:
        pd.DataFrame: One row per game across all requested seasons.
    """
    schedules = [build_schedule(fetch_schedule(season), season) for season in seasons]
    return pd.concat(schedules, ignore_index=True)



if __name__ == "__main__":
    schedule = get_schedule(list(range(2019, 2027)))
    schedule.to_parquet("schedule.parquet", index=False)