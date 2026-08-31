import datetime
import requests

import pandas as pd
from dotenv import load_dotenv

from cebl_data.games import build_team_boxscore, fetch_game
from cebl_data.schedule import get_schedule
from cebl_data.storage import read_published, write_published


def update_schedule(seasons: list[int]) -> pd.DataFrame:
    """Refreshes the published schedule for the given seasons.

    Rows for those seasons are replaced rather than appended to, so scores and
    statuses that changed after publication are corrected.

    Args:
        seasons (list[int]): The seasons to refresh, as four-digit years.

    Returns:
        pd.DataFrame: The full published schedule, all seasons.
    """
    published = read_published("schedule")
    schedule = get_schedule(seasons)

    kept = published[~published["season"].isin(seasons)]
    schedule = pd.concat([kept, schedule], ignore_index=True)
    schedule = schedule.sort_values(["season", "start_time_utc"], ignore_index=True)

    write_published(schedule, "schedule")
    return schedule


def update_team_boxscore(schedule: pd.DataFrame) -> None:
    """Adds any games missing from the published team box score.

    Only completed games are fetched, since Genius returns 403 for cancelled
    ones. Games already published are skipped, so a run that fails partway
    picks up where it left off.

    Args:
        schedule (pd.DataFrame): The full schedule.
    """
    published = read_published("team-boxscore")

    playable = schedule[schedule["status"] == "COMPLETE"]
    missing = playable[~playable["fiba_game_id"].isin(published["fiba_game_id"])]

    boxscores = []
    for game in missing.itertuples():
        try:
            payload = fetch_game(game.fiba_game_id)
        except requests.RequestException as error:
            print(f"{game.fiba_game_id}: {error}")
            continue
        boxscores.append(build_team_boxscore(payload, game))

    if not boxscores:
        return

    team_boxscore = pd.concat([published, *boxscores], ignore_index=True)
    write_published(team_boxscore, "team-boxscore")
