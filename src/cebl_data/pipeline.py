import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

from cebl_data.games import (
    build_officials,
    build_pbp,
    build_player_boxscore,
    build_team_boxscore,
    fetch_game,
)
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

    Only completed and cancelled games are fetched, since Genius
    returns 403 for cancelledones. Games already published are
    skipped, so a run that fails partway picks up where it left off.

    Args:
        schedule (pd.DataFrame): The full schedule.
    """
    published = read_published("team-boxscore")

    playable = schedule[schedule["status"].isin(["COMPLETE", "CANCELLED"])]
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


def update_player_boxscore(schedule: pd.DataFrame) -> None:
    """Adds any games missing from the published player box score.

    Only completed and cancelled games are fetched, since Genius
    returns 403 for cancelledones. Games already published are
    skipped, so a run that fails partway picks up where it left off.

    Args:
        schedule (pd.DataFrame): The full schedule.
    """
    published = read_published("player-boxscore")

    playable = schedule[schedule["status"].isin(["COMPLETE", "CANCELLED"])]
    missing = playable[~playable["fiba_game_id"].isin(published["fiba_game_id"])]

    boxscores = []
    for game in missing.itertuples():
        try:
            payload = fetch_game(game.fiba_game_id)
        except requests.RequestException as error:
            print(f"{game.fiba_game_id}: {error}")
            continue
        boxscores.append(build_player_boxscore(payload, game))

    if not boxscores:
        return

    player_boxscore = pd.concat([published, *boxscores], ignore_index=True)
    write_published(player_boxscore, "player-boxscore")


def update_pbp(schedule: pd.DataFrame) -> None:
    """Adds any games missing from the published play-by-play.

    Only completed and cancelled games are fetched, since Genius
    returns 403 for cancelledones. Games already published are
    skipped, so a run that fails partway picks up where it left off.

    Args:
        schedule (pd.DataFrame): The full schedule.
    """
    published = read_published("pbp")

    playable = schedule[schedule["status"].isin(["COMPLETE", "CANCELLED"])]
    missing = playable[~playable["fiba_game_id"].isin(published["fiba_game_id"])]

    events = []
    for game in missing.itertuples():
        try:
            payload = fetch_game(game.fiba_game_id)
        except requests.RequestException as error:
            print(f"{game.fiba_game_id}: {error}")
            continue
        events.append(build_pbp(payload, game))

    if not events:
        return

    pbp = pd.concat([published, *events], ignore_index=True)
    write_published(pbp, "pbp")


def update_officials(schedule: pd.DataFrame) -> None:
    """Adds any games missing from the published officials.

    Only completed and cancelled games are fetched, since Genius
    returns 403 for cancelledones. Games already published are
    skipped, so a run that fails partway picks up where it left off.

    Args:
        schedule (pd.DataFrame): The full schedule.
    """
    published = read_published("officials")

    playable = schedule[schedule["status"].isin(["COMPLETE", "CANCELLED"])]
    missing = playable[~playable["fiba_game_id"].isin(published["fiba_game_id"])]

    crews = []
    for game in missing.itertuples():
        try:
            payload = fetch_game(game.fiba_game_id)
        except requests.RequestException as error:
            print(f"{game.fiba_game_id}: {error}")
            continue
        crews.append(build_officials(payload, game))

    if not crews:
        return

    officials = pd.concat([published, *crews], ignore_index=True)
    write_published(officials, "officials")


def update_all(seasons: list[int]) -> None:
    """Refreshes every published dataset.

    The schedule is refreshed only for the given seasons, but the game
    datasets reconcile against the whole schedule, so a game missed in an
    earlier season is still picked up.

    Args:
        seasons (list[int]): The seasons to refresh, as four-digit years.
    """
    schedule = update_schedule(seasons)
    update_team_boxscore(schedule)
    update_player_boxscore(schedule)
    update_pbp(schedule)
    update_officials(schedule)


if __name__ == "__main__":
    load_dotenv()
    update_all([datetime.datetime.now(datetime.UTC).year])
