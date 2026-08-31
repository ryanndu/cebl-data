import pandas as pd
import requests

from cebl_data.utils import clean_strings, load_packaged_config


def fetch_game(fiba_game_id: str) -> dict:
    """Returns the raw livestats payload for one game.

    Args:
        fiba_game_id (str): The Genius Sports game id.

    Returns:
        dict: The game payload.

    Raises:
        requests.HTTPError: If the request fails.
    """
    response = requests.get(
        f"https://fibalivestats.dcd.shared.geniussports.com/data/{fiba_game_id}/data.json",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def parse_minutes(value: str) -> float | None:
    """Converts a "MM:SS" duration into minutes.

    Genius reports team minutes as strings like "199:60", which is both a
    string and arithmetically odd, so seconds are simply divided by 60 rather
    than validated.

    Args:
        value (str): A duration of the form "MM:SS".

    Returns:
        float | None: The duration in minutes, or None if value is empty.
    """
    if not value:
        return None
    minutes, seconds = value.split(":")
    return int(minutes) + int(seconds) / 60


def build_team_boxscore(payload: dict, game) -> pd.DataFrame:
    """Builds the team box score for one game.

    One row per team. Team totals lose their ``tot_s`` prefix so the columns
        match the player boxscore. Team ids come from the schedule, since the game
        payload identifies teams only by name and code. Fields the config doesn't
        account for are reported to stdout so changes to the source are visible.

    Args:
        payload (dict): A raw game payload.
        game: A row from the schedule DataFrame.

    Returns:
        pd.DataFrame: Two rows, home team first.
    """
    config = load_packaged_config("team_boxscore.json")
    known = set(config["rename"]) | set(config["dropped"]) | set(config["dtypes"])

    teams = []
    for team_number, team in payload["tm"].items():
        unexpected = set(team) - known
        if unexpected:
            print(f"{game.fiba_game_id}: unexpected fields {sorted(unexpected)}")

        is_home = team_number == "1"
        row = {
            field: value
            for field, value in team.items()
            if not isinstance(value, (dict, list))
        }
        row["fiba_game_id"] = game.fiba_game_id
        row["season"] = game.season
        row["is_home"] = is_home
        row["team_id"] = game.home_team_id if is_home else game.away_team_id
        teams.append(row)

    box_score = pd.DataFrame(teams).rename(columns=config["rename"])
    box_score["minutes"] = box_score["minutes"].map(parse_minutes)
    box_score = clean_strings(box_score)
    return box_score.reindex(columns=list(config["dtypes"])).astype(config["dtypes"])
