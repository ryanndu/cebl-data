import html
import json
from importlib.resources import files

import pandas as pd


def load_packaged_config(relative_path: str) -> dict:
    """Load a JSON config bundled in the cebl_data package.

    Args:
        relative_path (str): The filename or path of the JSON file relative
            to the "cebl_data/configs/" directory.

    Returns:
        dict: A Python dictionary containing the parsed JSON data.
    """
    with files("cebl_data").joinpath("configs", relative_path).open(encoding="utf-8") as f:
        return json.load(f)


def clean_strings(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalises string columns in place of the source's quirks.

    Genius emits empty strings and single spaces where a value is missing, and
    a handful of names arrive HTML-escaped.

    Args:
        frame (pd.DataFrame): A frame built from a raw payload.

    Returns:
        pd.DataFrame: The same frame with string columns normalised.
    """
    for column in frame.columns:
        if frame[column].dtype == "object" or pd.api.types.is_string_dtype(frame[column]):
            frame[column] = frame[column].map(unescape)
            frame[column] = frame[column].map(
                lambda value: pd.NA if isinstance(value, str) and not value.strip() else value
            )
    return frame


def unescape(value):
    """Decodes HTML entities repeatedly until the value stops changing.

    Args:
        value: A value from a raw payload; non-strings pass through.

    Returns:
        The decoded string, or the value unchanged.
    """
    if not isinstance(value, str):
        return value
    while (decoded := html.unescape(value)) != value:
        value = decoded
    return value
