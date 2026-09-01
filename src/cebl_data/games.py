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


def build_player_boxscore(payload: dict, game) -> pd.DataFrame:
    """Builds the player box score for one game.

    Args:
        payload (dict): A raw game payload.
        game: A row from the schedule DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with one row per player.
    """
    config = load_packaged_config("player_boxscore.json")
    known = set(config["rename"]) | set(config["dropped"]) | set(config["dtypes"])

    players = []
    for team_number, team in payload["tm"].items():
        is_home = team_number == "1"
        for player_number, player in team["pl"].items():
            unexpected = set(player) - known
            if unexpected:
                print(f"{game.fiba_game_id}: unexpected fields {sorted(unexpected)}")

            row = {
                field: value
                for field, value in player.items()
                if not isinstance(value, (dict, list))
            }
            row["fiba_game_id"] = game.fiba_game_id
            row["season"] = game.season
            row["is_home"] = is_home
            row["team_id"] = game.home_team_id if is_home else game.away_team_id
            row["game_player_number"] = str(player_number)
            row["captain"] = bool(player.get("captain", 0))
            players.append(row)

    box_score = pd.DataFrame(players).rename(columns=config["rename"])
    box_score["minutes"] = box_score["minutes"].map(parse_minutes)
    box_score["playing_position"] = box_score["playing_position"].str.upper()
    box_score = clean_strings(box_score)
    return box_score.reindex(columns=list(config["dtypes"])).astype(config["dtypes"])


def build_pbp(payload: dict, game) -> pd.DataFrame:
    """Builds the play-by-play for one game.

    Args:
        payload (dict): A raw game payload.
        game: A row from the schedule DataFrame.

    Returns:
        pd.DataFrame: One row per event, oldest first.
    """
    config = load_packaged_config("pbp.json")
    known = set(config["rename"]) | set(config["dropped"]) | set(config["dtypes"])
    events = list(reversed(payload["pbp"]))

    coordinates = {}
    for team_number, team in payload["tm"].items():
        shooting = [
            index
            for index, event in enumerate(events)
            if event["actionType"] in ("2pt", "3pt") and event["tno"] == int(team_number)
        ]
        for index, shot in zip(shooting, team["shot"]):
            coordinates[index] = (shot["x"], shot["y"])

    rows = []
    for index, event in enumerate(events):
        unexpected = set(event) - known
        if unexpected:
            print(f"{game.fiba_game_id}: unexpected fields {sorted(unexpected)}")

        is_home = None if event["tno"] == 0 else event["tno"] == 1
        row = {
            field: value
            for field, value in event.items()
            if not isinstance(value, (dict, list))
        }
        row["fiba_game_id"] = game.fiba_game_id
        row["season"] = game.season
        row["is_home"] = is_home
        row["team_id"] = (
            None if is_home is None
            else game.home_team_id if is_home
            else game.away_team_id
        )
        row["pno"] = None if event["pno"] == 0 else str(event["pno"])
        row["qualifier"] = ", ".join(event.get("qualifier") or [])
        row["x"], row["y"] = coordinates.get(index, (None, None))
        rows.append(row)

    pbp = pd.DataFrame(rows).rename(columns=config["rename"])
    for column in ["home_score", "away_score", "action_number", "previous_action_number"]:
        if column in pbp:
            pbp[column] = pd.to_numeric(pbp[column], errors="coerce")
    pbp = clean_strings(pbp)
    return pbp.reindex(columns=list(config["dtypes"])).astype(config["dtypes"])
